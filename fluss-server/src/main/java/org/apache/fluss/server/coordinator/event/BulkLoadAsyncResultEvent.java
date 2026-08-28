/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */

package org.apache.fluss.server.coordinator.event;

/** Immutable completion from off-event-thread BulkLoad work. */
public final class BulkLoadAsyncResultEvent implements CoordinatorEvent {
    private final Object result;

    public BulkLoadAsyncResultEvent(Object result) {
        this.result = result;
    }

    public Object getResult() {
        return result;
    }
}
