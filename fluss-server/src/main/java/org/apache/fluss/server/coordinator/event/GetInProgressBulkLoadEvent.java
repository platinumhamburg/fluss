/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */

package org.apache.fluss.server.coordinator.event;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.rpc.messages.GetInProgressBulkLoadResponse;
import org.apache.fluss.security.acl.FlussPrincipal;

import java.util.concurrent.CompletableFuture;

/** Event-thread request to get an accessible in-progress BulkLoad transaction. */
public final class GetInProgressBulkLoadEvent implements CoordinatorEvent {
    private final PhysicalTablePath target;
    private final FlussPrincipal creator;
    private final boolean canAlter;
    private final CompletableFuture<GetInProgressBulkLoadResponse> resultFuture;

    public GetInProgressBulkLoadEvent(
            PhysicalTablePath target,
            FlussPrincipal creator,
            boolean canAlter,
            CompletableFuture<GetInProgressBulkLoadResponse> resultFuture) {
        this.target = target;
        this.creator = creator;
        this.canAlter = canAlter;
        this.resultFuture = resultFuture;
    }

    public PhysicalTablePath getTarget() {
        return target;
    }

    public FlussPrincipal getCreator() {
        return creator;
    }

    public boolean canAlter() {
        return canAlter;
    }

    public CompletableFuture<GetInProgressBulkLoadResponse> getResultFuture() {
        return resultFuture;
    }
}
