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

import org.apache.fluss.client.bulkload.BulkLoadBuildContext;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.exception.CorruptMessageException;
import org.apache.fluss.exception.InvalidBulkLoadRequestException;
import org.apache.fluss.metadata.BulkLoadAbortReason;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.BulkLoadState;
import org.apache.fluss.metadata.BulkLoadStatus;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.AdminGateway;
import org.apache.fluss.rpc.messages.AbortBulkLoadRequest;
import org.apache.fluss.rpc.messages.BeginBulkLoadRequest;
import org.apache.fluss.rpc.messages.CommitBulkLoadRequest;
import org.apache.fluss.rpc.messages.GetInProgressBulkLoadRequest;
import org.apache.fluss.rpc.messages.GetInProgressBulkLoadResponse;
import org.apache.fluss.rpc.messages.PbBulkLoadStatus;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.apache.fluss.client.bulkload.protocol.BulkLoadMessageUtils.toBulkLoadHandle;
import static org.apache.fluss.client.bulkload.protocol.BulkLoadMessageUtils.toPbBulkLoadHandle;
import static org.apache.fluss.client.bulkload.protocol.BulkLoadMessageUtils.toPbPhysicalTablePath;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** RPC adapter for the BulkLoad transaction protocol. */
final class BulkLoadRpcClient {

    private final AdminGateway gateway;

    /** Creates a BulkLoad RPC adapter sharing the connection's RPC and metadata resources. */
    BulkLoadRpcClient(RpcClient rpcClient, MetadataUpdater metadataUpdater) {
        this(
                GatewayClientProxy.createGatewayProxy(
                        checkNotNull(metadataUpdater, "Metadata updater must not be null.")
                                ::getCoordinatorServer,
                        checkNotNull(rpcClient, "RPC client must not be null."),
                        AdminGateway.class));
    }

    /** Creates a BulkLoad RPC adapter over an existing Coordinator gateway. */
    BulkLoadRpcClient(AdminGateway gateway) {
        this.gateway = checkNotNull(gateway, "Admin gateway must not be null.");
    }

    /** Begins one metadata-only transaction for all supplied physical targets. */
    CompletableFuture<BulkLoadBuildContext> beginBulkLoad(
            Collection<PhysicalTablePath> targets, @Nullable Duration buildTimeout) {
        BeginBulkLoadRequest request = new BeginBulkLoadRequest();
        for (PhysicalTablePath target : targets) {
            request.addAllTargets(
                    java.util.Collections.singletonList(
                            toPbPhysicalTablePath(validateBulkLoadTarget(target))));
        }
        if (buildTimeout != null) {
            request.setTimeoutMs(validateBuildTimeout(buildTimeout));
        }
        return gateway.beginBulkLoad(request).thenApply(BulkLoadBuildContext::new);
    }

    /** Gets the in-progress BulkLoad transaction for the target, if one is visible. */
    CompletableFuture<Optional<BulkLoadStatus>> getInProgressBulkLoad(PhysicalTablePath target) {
        PhysicalTablePath validatedTarget = validateBulkLoadTarget(target);
        GetInProgressBulkLoadRequest request =
                new GetInProgressBulkLoadRequest()
                        .setTarget(toPbPhysicalTablePath(validatedTarget));
        return gateway.getInProgressBulkLoad(request)
                .thenApply(
                        response ->
                                convertBulkLoadResponse(
                                        "GetInProgress",
                                        () -> toMatchingBulkLoadStatus(response, validatedTarget)));
    }

    /** Commits a BulkLoad transaction using the supplied input digest. */
    CompletableFuture<BulkLoadStatus> commitBulkLoad(BulkLoadHandle handle, String inputSha256) {
        CommitBulkLoadRequest request =
                new CommitBulkLoadRequest()
                        .setHandle(toPbBulkLoadHandle(handle))
                        .setInputSha256(inputSha256);
        return gateway.commitBulkLoad(request)
                .thenApply(
                        response ->
                                convertBulkLoadResponse(
                                        "Commit",
                                        () ->
                                                toMatchingBulkLoadStatus(
                                                        response.getStatus(), handle)));
    }

    /** Aborts a BulkLoad transaction. */
    CompletableFuture<BulkLoadStatus> abortBulkLoad(BulkLoadHandle handle) {
        AbortBulkLoadRequest request =
                new AbortBulkLoadRequest().setHandle(toPbBulkLoadHandle(handle));
        return gateway.abortBulkLoad(request)
                .thenApply(
                        response ->
                                convertBulkLoadResponse(
                                        "Abort",
                                        () ->
                                                toMatchingBulkLoadStatus(
                                                        response.getStatus(), handle)));
    }

    private static PhysicalTablePath validateBulkLoadTarget(PhysicalTablePath target) {
        try {
            if (target == null || !target.isValid()) {
                throw new IllegalArgumentException("BulkLoad target must be valid.");
            }
            return target;
        } catch (RuntimeException e) {
            throw new InvalidBulkLoadRequestException("Invalid BulkLoad target.", e);
        }
    }

    private static long validateBuildTimeout(Duration buildTimeout) {
        try {
            if (buildTimeout == null) {
                throw new IllegalArgumentException("BulkLoad build timeout must not be null.");
            }
            long buildTimeoutMs = buildTimeout.toMillis();
            if (buildTimeoutMs <= 0) {
                throw new IllegalArgumentException(
                        "BulkLoad build timeout must be at least one millisecond.");
            }
            return buildTimeoutMs;
        } catch (RuntimeException e) {
            throw new InvalidBulkLoadRequestException("Invalid BulkLoad build timeout.", e);
        }
    }

    private static Optional<BulkLoadStatus> toMatchingBulkLoadStatus(
            GetInProgressBulkLoadResponse response, PhysicalTablePath requestedTarget) {
        if (!response.hasStatus()) {
            return Optional.empty();
        }
        BulkLoadStatus status = toBulkLoadStatus(response.getStatus());
        if (!status.getHandle().getTablePath().equals(requestedTarget.getTablePath())) {
            throw new IllegalArgumentException(
                    "BulkLoad GetInProgress response does not identify the requested target.");
        }
        return Optional.of(status);
    }

    private static BulkLoadStatus toMatchingBulkLoadStatus(
            PbBulkLoadStatus responseStatus, BulkLoadHandle requestedHandle) {
        BulkLoadStatus status = toBulkLoadStatus(responseStatus);
        if (!status.getHandle().equals(requestedHandle)) {
            throw new IllegalArgumentException(
                    "BulkLoad response does not identify the requested handle.");
        }
        return status;
    }

    private static BulkLoadStatus toBulkLoadStatus(PbBulkLoadStatus status) {
        return new BulkLoadStatus(
                toBulkLoadHandle(status.getHandle()),
                BulkLoadState.fromCode(status.getState()),
                status.hasAbortReason()
                        ? BulkLoadAbortReason.fromCode(status.getAbortReason())
                        : null,
                status.hasAbortMessage() ? status.getAbortMessage() : null);
    }

    private static <T> T convertBulkLoadResponse(String operation, Supplier<T> converter) {
        try {
            return converter.get();
        } catch (RuntimeException e) {
            throw new CorruptMessageException("Corrupt BulkLoad " + operation + " response.", e);
        }
    }
}
