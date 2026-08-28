/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */

package org.apache.fluss.server.coordinator.event;

import org.apache.fluss.config.Configuration;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Immutable event-thread signal for BulkLoad recovery, deadlines, and configuration changes. */
public final class BulkLoadMaintenanceEvent implements CoordinatorEvent {
    /** Reason for scheduling the signal. */
    public enum Reason {
        STARTUP,
        PERIODIC,
        CONFIG_CHANGE
    }

    private final Reason reason;
    private final @Nullable Map<String, String> configuration;

    /** Creates a startup or periodic maintenance signal. */
    public BulkLoadMaintenanceEvent(Reason reason) {
        this(reason, null);
    }

    /** Creates a signal with an immutable copy of a proposed configuration. */
    public BulkLoadMaintenanceEvent(Reason reason, @Nullable Configuration configuration) {
        if ((reason == Reason.CONFIG_CHANGE) != (configuration != null)) {
            throw new IllegalArgumentException(
                    "BulkLoad CONFIG_CHANGE must carry exactly one configuration snapshot.");
        }
        this.reason = reason;
        this.configuration =
                configuration == null
                        ? null
                        : Collections.unmodifiableMap(new HashMap<>(configuration.toMap()));
    }

    /** Returns why this maintenance event was enqueued. */
    public Reason getReason() {
        return reason;
    }

    /** Returns a fresh copy of the proposed configuration. */
    public Configuration getConfiguration() {
        if (configuration == null) {
            throw new IllegalStateException("BulkLoad maintenance event has no configuration.");
        }
        return Configuration.fromMap(configuration);
    }
}
