/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */

package org.apache.fluss.server.coordinator.event;

import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.rpc.messages.CommitBulkLoadResponse;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/** Event-thread request to commit a BulkLoad transaction. */
public final class CommitBulkLoadEvent implements CoordinatorEvent {
    private final BulkLoadHandle handle;
    private final @Nullable String manifestPath;
    private final @Nullable Long manifestLength;
    private final @Nullable String manifestSha256;
    private final CompletableFuture<CommitBulkLoadResponse> resultFuture;

    public CommitBulkLoadEvent(
            BulkLoadHandle handle,
            @Nullable String manifestPath,
            @Nullable Long manifestLength,
            @Nullable String manifestSha256,
            CompletableFuture<CommitBulkLoadResponse> resultFuture) {
        this.handle = handle;
        this.manifestPath = manifestPath;
        this.manifestLength = manifestLength;
        this.manifestSha256 = manifestSha256;
        this.resultFuture = resultFuture;
    }

    public BulkLoadHandle getHandle() {
        return handle;
    }

    public @Nullable String getManifestPath() {
        return manifestPath;
    }

    public @Nullable Long getManifestLength() {
        return manifestLength;
    }

    public @Nullable String getManifestSha256() {
        return manifestSha256;
    }

    public CompletableFuture<CommitBulkLoadResponse> getResultFuture() {
        return resultFuture;
    }
}
