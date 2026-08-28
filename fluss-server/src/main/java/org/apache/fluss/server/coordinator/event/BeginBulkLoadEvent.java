/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */

package org.apache.fluss.server.coordinator.event;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.rpc.messages.BeginBulkLoadResponse;
import org.apache.fluss.security.acl.FlussPrincipal;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/** Event-thread request to begin a BulkLoad transaction. */
public final class BeginBulkLoadEvent implements CoordinatorEvent {
    private final PhysicalTablePath target;
    private final String callerToken;
    private final @Nullable Long buildTimeoutMs;
    private final FlussPrincipal creator;
    private final CompletableFuture<BeginBulkLoadResponse> resultFuture;

    public BeginBulkLoadEvent(
            PhysicalTablePath target,
            String callerToken,
            @Nullable Long buildTimeoutMs,
            FlussPrincipal creator,
            CompletableFuture<BeginBulkLoadResponse> resultFuture) {
        this.target = target;
        this.callerToken = callerToken;
        this.buildTimeoutMs = buildTimeoutMs;
        this.creator = creator;
        this.resultFuture = resultFuture;
    }

    public PhysicalTablePath getTarget() {
        return target;
    }

    public String getCallerToken() {
        return callerToken;
    }

    @Nullable
    public Long getBuildTimeoutMs() {
        return buildTimeoutMs;
    }

    public FlussPrincipal getCreator() {
        return creator;
    }

    public CompletableFuture<BeginBulkLoadResponse> getResultFuture() {
        return resultFuture;
    }
}
