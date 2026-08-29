/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.bulkload.protocol;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.bulkload.file.BulkLoadFileHandle;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.BulkLoadStatus;
import org.apache.fluss.metadata.BulkLoadTargetInfo;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.rpc.RpcClient;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Drives the internal BulkLoad transaction verbs with the recovery policy of the primary-key table
 * bulk-load protocol. All methods block the calling thread until the underlying request completes
 * or the caller-provided budget expires.
 *
 * <p>{@link #begin} issues one Begin request and returns its frozen target information. If the
 * caller's local wait expires, the same request may still complete remotely; this driver never
 * submits a replacement Begin request.
 *
 * <p>{@link #commitUntilReady} retries the same Commit request on {@link RetriableException} —
 * retrying is safe because a repeated commit joins the in-flight commit and awaits the same result
 * — until the await timeout expires. A successful commit already means the target is {@code ACTIVE}
 * and ready for access, so no status polling follows. Non-retriable failures are propagated without
 * retrying.
 *
 * <p>{@link #abort} issues its request exactly once and propagates the persisted status or failure
 * unchanged.
 */
@Internal
public final class BulkLoadTransactionDriver {

    /** Fixed interval between Commit retries after a retriable failure. */
    private static final long COMMIT_RETRY_INTERVAL_MS = 200L;

    private final BulkLoadRpcClient rpcClient;

    /** Creates a driver sharing the connection's RPC and metadata resources. */
    public BulkLoadTransactionDriver(RpcClient rpcClient, MetadataUpdater metadataUpdater) {
        this(new BulkLoadRpcClient(rpcClient, metadataUpdater));
    }

    BulkLoadTransactionDriver(BulkLoadRpcClient rpcClient) {
        this.rpcClient = checkNotNull(rpcClient, "BulkLoad RPC client must not be null.");
    }

    /**
     * Begins a new BulkLoad transaction for the target.
     *
     * @param target the physical table or partition to load
     * @param buildTimeout the explicit build timeout forwarded to Begin, or {@code null} to use the
     *     Coordinator's configured build timeout
     * @param overallTimeout the positive upper bound of the Begin request
     * @return the frozen target information of the created transaction
     * @throws TimeoutException if the overall timeout expires before a begin outcome is reached
     * @throws InterruptedException if the calling thread is interrupted while waiting
     * @throws Exception any failure of the underlying Begin, unwrapped
     */
    public BulkLoadTargetInfo begin(
            PhysicalTablePath target, @Nullable Duration buildTimeout, Duration overallTimeout)
            throws Exception {
        checkNotNull(target, "BulkLoad target must not be null.");
        checkNotNull(overallTimeout, "BulkLoad overall timeout must not be null.");
        checkArgument(
                !overallTimeout.isNegative() && !overallTimeout.isZero(),
                "BulkLoad overall timeout must be positive.");

        try {
            return await(beginFuture(target, buildTimeout), overallTimeout.toMillis());
        } catch (TimeoutException e) {
            throw beginTimeout(target, overallTimeout);
        }
    }

    /**
     * Returns the in-progress transaction for the target, if the caller may access one.
     *
     * @param target the physical table or partition to inspect
     * @return the in-progress status, or empty when no transaction is in progress
     */
    public Optional<BulkLoadStatus> getInProgressBulkLoad(PhysicalTablePath target)
            throws Exception {
        checkNotNull(target, "BulkLoad target must not be null.");
        return await(rpcClient.getInProgressBulkLoad(target));
    }

    /**
     * Validates the manifest and commits the transaction, retrying the same Commit request on
     * {@link RetriableException} until the await timeout expires. A repeated commit joins the
     * in-flight commit and awaits the same result, and a successful commit already means the target
     * is ready for access, so this method returns as soon as a commit succeeds without polling the
     * transaction status.
     *
     * @param handle the transaction handle returned by Begin
     * @param manifest the manifest file handle returned by the manifest assembler
     * @param awaitTimeout the positive upper bound of the whole commit sequence
     * @throws TimeoutException if the await timeout expires before a commit is confirmed, wrapping
     *     the last failure as its cause
     * @throws InterruptedException if the calling thread is interrupted while waiting
     * @throws Exception any non-retriable failure of the Commit request, unwrapped and without
     *     retrying
     */
    public BulkLoadStatus commitUntilReady(
            BulkLoadHandle handle, BulkLoadFileHandle manifest, Duration awaitTimeout)
            throws Exception {
        checkNotNull(manifest, "BulkLoad manifest must not be null.");
        checkNotNull(handle, "BulkLoad handle must not be null.");
        checkNotNull(awaitTimeout, "BulkLoad commit await timeout must not be null.");
        checkArgument(
                !awaitTimeout.isNegative() && !awaitTimeout.isZero(),
                "BulkLoad commit await timeout must be positive.");

        long deadlineNanos = System.nanoTime() + awaitTimeout.toNanos();
        Throwable lastFailure = null;
        while (true) {
            long remainingMs = remainingMs(deadlineNanos);
            if (remainingMs <= 0L) {
                TimeoutException timeout =
                        new TimeoutException(
                                "BulkLoad commit "
                                        + handle.getBulkLoadId()
                                        + " was not confirmed within "
                                        + awaitTimeout.toMillis()
                                        + " ms.");
                if (lastFailure != null) {
                    timeout.initCause(lastFailure);
                }
                throw timeout;
            }
            try {
                return await(rpcClient.commitBulkLoad(handle, manifest), remainingMs);
            } catch (RetriableException e) {
                // The commit outcome is unknown: retry the same request after a
                // short pause, bounded by the await budget.
                lastFailure = e;
                long sleepMs = Math.min(COMMIT_RETRY_INTERVAL_MS, remainingMs(deadlineNanos));
                if (sleepMs > 0L) {
                    sleepInterruptibly(sleepMs);
                }
            } catch (TimeoutException e) {
                // The local wait consumed the remaining budget; the deadline check above throws.
                lastFailure = e;
            }
        }
    }

    /**
     * Aborts the transaction, issuing the request exactly once. Abort is idempotent.
     *
     * @param handle the transaction handle returned by Begin
     * @return the persisted transaction status
     * @throws InterruptedException if the calling thread is interrupted while waiting
     * @throws Exception any failure of the Abort request, unwrapped
     */
    public BulkLoadStatus abort(BulkLoadHandle handle) throws Exception {
        checkNotNull(handle, "BulkLoad handle must not be null.");
        return await(rpcClient.abortBulkLoad(handle));
    }

    private CompletableFuture<BulkLoadTargetInfo> beginFuture(
            PhysicalTablePath target, @Nullable Duration buildTimeout) {
        return buildTimeout == null
                ? rpcClient.beginBulkLoad(target)
                : rpcClient.beginBulkLoad(target, buildTimeout);
    }

    private static TimeoutException beginTimeout(
            PhysicalTablePath target, Duration overallTimeout) {
        return new TimeoutException(
                "Beginning a BulkLoad transaction for target "
                        + target
                        + " did not succeed within "
                        + overallTimeout.toMillis()
                        + " ms.");
    }

    private static long remainingMs(long deadlineNanos) {
        return TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
    }

    private static void sleepInterruptibly(long millis) throws InterruptedException {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    private static <T> T await(CompletableFuture<T> future) throws Exception {
        try {
            return future.get();
        } catch (ExecutionException e) {
            throw propagate(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    private static <T> T await(CompletableFuture<T> future, long timeoutMs) throws Exception {
        try {
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw propagate(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    private static Exception propagate(Throwable cause) {
        if (cause instanceof Exception) {
            return (Exception) cause;
        }
        if (cause instanceof Error) {
            throw (Error) cause;
        }
        return new IllegalStateException(
                "BulkLoad request failed with an unexpected cause.", cause);
    }
}
