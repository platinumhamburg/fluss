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

import org.apache.fluss.client.bulkload.file.BulkLoadFileHandle;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.exception.CorruptMessageException;
import org.apache.fluss.exception.InvalidBulkLoadRequestException;
import org.apache.fluss.metadata.BulkLoadAbortReason;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.BulkLoadState;
import org.apache.fluss.metadata.BulkLoadStatus;
import org.apache.fluss.metadata.BulkLoadTargetInfo;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.AdminGateway;
import org.apache.fluss.rpc.messages.AbortBulkLoadRequest;
import org.apache.fluss.rpc.messages.BeginBulkLoadRequest;
import org.apache.fluss.rpc.messages.BeginBulkLoadResponse;
import org.apache.fluss.rpc.messages.CommitBulkLoadRequest;
import org.apache.fluss.rpc.messages.GetBulkLoadStatusRequest;
import org.apache.fluss.rpc.messages.PbBulkLoadStatus;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static org.apache.fluss.client.bulkload.protocol.BulkLoadTargetInfoSerde.toBulkLoadHandle;
import static org.apache.fluss.client.bulkload.protocol.BulkLoadTargetInfoSerde.toBulkLoadTargetInfo;
import static org.apache.fluss.client.bulkload.protocol.BulkLoadTargetInfoSerde.toPbBulkLoadHandle;
import static org.apache.fluss.client.bulkload.protocol.BulkLoadTargetInfoSerde.toPbPhysicalTablePath;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** RPC adapter for the BulkLoad transaction protocol. */
final class BulkLoadRpcClient {

    private static final Pattern SHA_256_PATTERN = Pattern.compile("[0-9a-f]{64}");

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

    /** Begins a BulkLoad transaction using the Coordinator's configured build timeout. */
    CompletableFuture<BeginBulkLoadResult> beginBulkLoad(
            PhysicalTablePath target, String callerToken) {
        PhysicalTablePath validatedTarget = validateBulkLoadTarget(target);
        BeginBulkLoadRequest request =
                new BeginBulkLoadRequest()
                        .setTarget(toPbPhysicalTablePath(validatedTarget))
                        .setCallerToken(validateCallerToken(callerToken));
        return gateway.beginBulkLoad(request)
                .thenApply(
                        response ->
                                convertBulkLoadResponse(
                                        "Begin",
                                        () -> toBeginBulkLoadResult(response, validatedTarget)));
    }

    /** Begins a BulkLoad transaction using the supplied build timeout. */
    CompletableFuture<BeginBulkLoadResult> beginBulkLoad(
            PhysicalTablePath target, String callerToken, Duration buildTimeout) {
        PhysicalTablePath validatedTarget = validateBulkLoadTarget(target);
        long buildTimeoutMs = validateBuildTimeout(buildTimeout);
        BeginBulkLoadRequest request =
                new BeginBulkLoadRequest()
                        .setTarget(toPbPhysicalTablePath(validatedTarget))
                        .setCallerToken(validateCallerToken(callerToken))
                        .setBuildTimeoutMs(buildTimeoutMs);
        return gateway.beginBulkLoad(request)
                .thenApply(
                        response ->
                                convertBulkLoadResponse(
                                        "Begin",
                                        () -> toBeginBulkLoadResult(response, validatedTarget)));
    }

    /** Commits a BulkLoad transaction using the supplied manifest. */
    CompletableFuture<BulkLoadStatus> commitBulkLoad(
            BulkLoadHandle handle, BulkLoadFileHandle manifest) {
        BulkLoadHandle validatedHandle = validateBulkLoadHandle(handle);
        validateManifest(manifest);
        CommitBulkLoadRequest request =
                new CommitBulkLoadRequest()
                        .setHandle(toPbBulkLoadHandle(validatedHandle))
                        .setManifestLength(manifest.getLength())
                        .setManifestSha256(manifest.getSha256())
                        .setManifestPath(manifest.getPath());
        return gateway.commitBulkLoad(request)
                .thenApply(
                        response ->
                                convertBulkLoadResponse(
                                        "Commit",
                                        () ->
                                                toMatchingBulkLoadStatus(
                                                        response.getStatus(), validatedHandle)));
    }

    /** Resumes a BulkLoad transaction whose manifest identity is already durable. */
    CompletableFuture<BulkLoadStatus> commitBulkLoad(BulkLoadHandle handle) {
        BulkLoadHandle validatedHandle = validateBulkLoadHandle(handle);
        CommitBulkLoadRequest request =
                new CommitBulkLoadRequest().setHandle(toPbBulkLoadHandle(validatedHandle));
        return gateway.commitBulkLoad(request)
                .thenApply(
                        response ->
                                convertBulkLoadResponse(
                                        "Commit",
                                        () ->
                                                toMatchingBulkLoadStatus(
                                                        response.getStatus(), validatedHandle)));
    }

    /** Aborts a BulkLoad transaction. */
    CompletableFuture<BulkLoadStatus> abortBulkLoad(BulkLoadHandle handle) {
        BulkLoadHandle validatedHandle = validateBulkLoadHandle(handle);
        AbortBulkLoadRequest request =
                new AbortBulkLoadRequest().setHandle(toPbBulkLoadHandle(validatedHandle));
        return gateway.abortBulkLoad(request)
                .thenApply(
                        response ->
                                convertBulkLoadResponse(
                                        "Abort",
                                        () ->
                                                toMatchingBulkLoadStatus(
                                                        response.getStatus(), validatedHandle)));
    }

    /** Returns the persisted status of a BulkLoad transaction. */
    CompletableFuture<BulkLoadStatus> getBulkLoadStatus(BulkLoadHandle handle) {
        BulkLoadHandle validatedHandle = validateBulkLoadHandle(handle);
        GetBulkLoadStatusRequest request =
                new GetBulkLoadStatusRequest().setHandle(toPbBulkLoadHandle(validatedHandle));
        return gateway.getBulkLoadStatus(request)
                .thenApply(
                        response ->
                                convertBulkLoadResponse(
                                        "Status",
                                        () ->
                                                toMatchingBulkLoadStatus(
                                                        response.getStatus(), validatedHandle)));
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

    private static String validateCallerToken(String callerToken) {
        if (callerToken == null || callerToken.trim().isEmpty()) {
            throw new InvalidBulkLoadRequestException("BulkLoad caller token must not be empty.");
        }
        return callerToken;
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

    private static BulkLoadHandle validateBulkLoadHandle(BulkLoadHandle handle) {
        try {
            if (handle == null) {
                throw new IllegalArgumentException("BulkLoad handle must not be null.");
            }
            return new BulkLoadHandle(
                    handle.getTarget(),
                    handle.getTableId(),
                    handle.getPartitionId(),
                    handle.getBulkLoadId());
        } catch (RuntimeException e) {
            throw new InvalidBulkLoadRequestException("Invalid BulkLoad handle.", e);
        }
    }

    private static void validateManifest(BulkLoadFileHandle manifest) {
        if (manifest == null) {
            throw new InvalidBulkLoadRequestException("BulkLoad manifest must not be null.");
        }
        if (manifest.getPath() == null || manifest.getPath().isEmpty()) {
            throw new InvalidBulkLoadRequestException("BulkLoad manifest path must not be empty.");
        }
        if (manifest.getLength() <= 0L) {
            throw new InvalidBulkLoadRequestException("BulkLoad manifest length must be positive.");
        }
        if (manifest.getSha256() == null
                || !SHA_256_PATTERN.matcher(manifest.getSha256()).matches()) {
            throw new InvalidBulkLoadRequestException(
                    "BulkLoad manifest SHA-256 must contain exactly 64 lowercase hexadecimal "
                            + "characters.");
        }
    }

    private static BeginBulkLoadResult toBeginBulkLoadResult(
            BeginBulkLoadResponse response, PhysicalTablePath requestedTarget) {
        BulkLoadStatus status = toBulkLoadStatus(response.getStatus());
        if (!status.getHandle().getTarget().equals(requestedTarget)) {
            throw new IllegalArgumentException(
                    "BulkLoad Begin response does not identify the requested target.");
        }
        BulkLoadTargetInfo targetInfo =
                response.hasTargetInfo() ? toBulkLoadTargetInfo(response.getTargetInfo()) : null;
        return new BeginBulkLoadResult(response.isCreated(), status, targetInfo);
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
