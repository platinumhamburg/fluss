/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */

package org.apache.fluss.server.coordinator.event;

import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.rpc.messages.AbortBulkLoadResponse;

import java.util.concurrent.CompletableFuture;

/** Event-thread request to abort a BulkLoad transaction. */
public final class AbortBulkLoadEvent implements CoordinatorEvent {
    private final BulkLoadHandle handle;
    private final CompletableFuture<AbortBulkLoadResponse> resultFuture;

    public AbortBulkLoadEvent(
            BulkLoadHandle handle, CompletableFuture<AbortBulkLoadResponse> resultFuture) {
        this.handle = handle;
        this.resultFuture = resultFuture;
    }

    public BulkLoadHandle getHandle() {
        return handle;
    }

    public CompletableFuture<AbortBulkLoadResponse> getResultFuture() {
        return resultFuture;
    }
}
