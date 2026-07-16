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

package org.apache.fluss.flink.action.orphan.audit;

import org.apache.fluss.annotation.PublicUnstable;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

/** Immutable provider-neutral envelope for an orphan cleanup audit event. */
@PublicUnstable
public final class AuditEvent {

    public static final int SCHEMA_VERSION = 1;

    private static final Pattern ACTION_PATTERN = Pattern.compile("[a-z][a-z0-9_]*");

    private final int schemaVersion;
    private final String eventId;
    private final String runId;
    private final long eventTimeMillis;
    private final AuditSeverity severity;
    private final AuditStage stage;
    private final String action;

    private final @Nullable String operatorName;
    private final @Nullable Integer subtaskIndex;
    private final @Nullable Integer attemptNumber;

    private final @Nullable String database;
    private final @Nullable String table;
    private final @Nullable Long tableId;
    private final @Nullable Long partitionId;
    private final @Nullable Integer bucketId;
    private final @Nullable String scopeKind;

    private final @Nullable String objectType;
    private final @Nullable String path;
    private final @Nullable Long sizeBytes;
    private final @Nullable Long mtimeMs;
    private final @Nullable String rule;
    private final @Nullable String reasonCode;
    private final @Nullable String result;

    private final Map<String, String> dimensions;
    private final Map<String, Long> metrics;
    private final Map<String, Boolean> flags;

    private AuditEvent(Builder builder) {
        this.schemaVersion = SCHEMA_VERSION;
        this.eventId = validateUuid(builder.eventId, "eventId");
        this.runId = validateUuid(builder.runId, "runId");
        if (builder.severity == null) {
            throw new IllegalArgumentException("severity");
        }
        if (builder.stage == null) {
            throw new IllegalArgumentException("stage");
        }
        if (builder.action == null || !ACTION_PATTERN.matcher(builder.action).matches()) {
            throw new IllegalArgumentException("action");
        }
        if (builder.eventTimeMillis < 0) {
            throw new IllegalArgumentException("eventTimeMillis");
        }
        this.eventTimeMillis = builder.eventTimeMillis;
        this.severity = builder.severity;
        this.stage = builder.stage;
        this.action = builder.action;
        this.operatorName = builder.operatorName;
        this.subtaskIndex = builder.subtaskIndex;
        this.attemptNumber = builder.attemptNumber;
        this.database = builder.database;
        this.table = builder.table;
        this.tableId = builder.tableId;
        this.partitionId = builder.partitionId;
        this.bucketId = builder.bucketId;
        this.scopeKind = builder.scopeKind;
        this.objectType = builder.objectType;
        this.path = builder.path;
        this.sizeBytes = builder.sizeBytes;
        this.mtimeMs = builder.mtimeMs;
        this.rule = builder.rule;
        this.reasonCode = builder.reasonCode;
        this.result = builder.result;
        this.dimensions = copyStringMap(builder.dimensions, "dimensions");
        this.metrics = copyScalarMap(builder.metrics, "metrics");
        this.flags = copyScalarMap(builder.flags, "flags");
    }

    public static Builder builder() {
        return new Builder();
    }

    public int getSchemaVersion() {
        return schemaVersion;
    }

    public String getEventId() {
        return eventId;
    }

    public String getRunId() {
        return runId;
    }

    public long getEventTimeMillis() {
        return eventTimeMillis;
    }

    public AuditSeverity getSeverity() {
        return severity;
    }

    public AuditStage getStage() {
        return stage;
    }

    public String getAction() {
        return action;
    }

    @Nullable
    public String getOperatorName() {
        return operatorName;
    }

    @Nullable
    public Integer getSubtaskIndex() {
        return subtaskIndex;
    }

    @Nullable
    public Integer getAttemptNumber() {
        return attemptNumber;
    }

    @Nullable
    public String getDatabase() {
        return database;
    }

    @Nullable
    public String getTable() {
        return table;
    }

    @Nullable
    public Long getTableId() {
        return tableId;
    }

    @Nullable
    public Long getPartitionId() {
        return partitionId;
    }

    @Nullable
    public Integer getBucketId() {
        return bucketId;
    }

    @Nullable
    public String getScopeKind() {
        return scopeKind;
    }

    @Nullable
    public String getObjectType() {
        return objectType;
    }

    @Nullable
    public String getPath() {
        return path;
    }

    @Nullable
    public Long getSizeBytes() {
        return sizeBytes;
    }

    @Nullable
    public Long getMtimeMs() {
        return mtimeMs;
    }

    @Nullable
    public String getRule() {
        return rule;
    }

    @Nullable
    public String getReasonCode() {
        return reasonCode;
    }

    @Nullable
    public String getResult() {
        return result;
    }

    public Map<String, String> getDimensions() {
        return dimensions;
    }

    public Map<String, Long> getMetrics() {
        return metrics;
    }

    public Map<String, Boolean> getFlags() {
        return flags;
    }

    private static String validateUuid(@Nullable String value, String field) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(field);
        }
        try {
            UUID.fromString(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(field);
        }
        return value;
    }

    private static Map<String, String> copyStringMap(
            @Nullable Map<String, String> source, String field) {
        if (source == null) {
            throw new IllegalArgumentException(field);
        }
        LinkedHashMap<String, String> copy = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : source.entrySet()) {
            if (entry == null
                    || entry.getKey() == null
                    || entry.getKey().isEmpty()
                    || entry.getValue() == null
                    || entry.getValue().isEmpty()) {
                throw new IllegalArgumentException(field);
            }
            copy.put(entry.getKey(), entry.getValue());
        }
        return Collections.unmodifiableMap(copy);
    }

    private static <T> Map<String, T> copyScalarMap(@Nullable Map<String, T> source, String field) {
        if (source == null) {
            throw new IllegalArgumentException(field);
        }
        LinkedHashMap<String, T> copy = new LinkedHashMap<>();
        for (Map.Entry<String, T> entry : source.entrySet()) {
            if (entry == null
                    || entry.getKey() == null
                    || entry.getKey().isEmpty()
                    || entry.getValue() == null) {
                throw new IllegalArgumentException(field);
            }
            copy.put(entry.getKey(), entry.getValue());
        }
        return Collections.unmodifiableMap(copy);
    }

    /** Builder for {@link AuditEvent}. */
    @PublicUnstable
    public static final class Builder {

        private @Nullable String eventId;
        private @Nullable String runId;
        private long eventTimeMillis;
        private @Nullable AuditSeverity severity;
        private @Nullable AuditStage stage;
        private @Nullable String action;

        private @Nullable String operatorName;
        private @Nullable Integer subtaskIndex;
        private @Nullable Integer attemptNumber;

        private @Nullable String database;
        private @Nullable String table;
        private @Nullable Long tableId;
        private @Nullable Long partitionId;
        private @Nullable Integer bucketId;
        private @Nullable String scopeKind;

        private @Nullable String objectType;
        private @Nullable String path;
        private @Nullable Long sizeBytes;
        private @Nullable Long mtimeMs;
        private @Nullable String rule;
        private @Nullable String reasonCode;
        private @Nullable String result;

        private @Nullable Map<String, String> dimensions = Collections.emptyMap();
        private @Nullable Map<String, Long> metrics = Collections.emptyMap();
        private @Nullable Map<String, Boolean> flags = Collections.emptyMap();

        private Builder() {}

        public Builder eventId(String eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder runId(String runId) {
            this.runId = runId;
            return this;
        }

        public Builder eventTimeMillis(long eventTimeMillis) {
            this.eventTimeMillis = eventTimeMillis;
            return this;
        }

        public Builder severity(AuditSeverity severity) {
            this.severity = severity;
            return this;
        }

        public Builder stage(AuditStage stage) {
            this.stage = stage;
            return this;
        }

        public Builder action(String action) {
            this.action = action;
            return this;
        }

        public Builder operatorName(@Nullable String operatorName) {
            this.operatorName = operatorName;
            return this;
        }

        public Builder subtaskIndex(@Nullable Integer subtaskIndex) {
            this.subtaskIndex = subtaskIndex;
            return this;
        }

        public Builder attemptNumber(@Nullable Integer attemptNumber) {
            this.attemptNumber = attemptNumber;
            return this;
        }

        public Builder database(@Nullable String database) {
            this.database = database;
            return this;
        }

        public Builder table(@Nullable String table) {
            this.table = table;
            return this;
        }

        public Builder tableId(@Nullable Long tableId) {
            this.tableId = tableId;
            return this;
        }

        public Builder partitionId(@Nullable Long partitionId) {
            this.partitionId = partitionId;
            return this;
        }

        public Builder bucketId(@Nullable Integer bucketId) {
            this.bucketId = bucketId;
            return this;
        }

        public Builder scopeKind(@Nullable String scopeKind) {
            this.scopeKind = scopeKind;
            return this;
        }

        public Builder objectType(@Nullable String objectType) {
            this.objectType = objectType;
            return this;
        }

        public Builder path(@Nullable String path) {
            this.path = path;
            return this;
        }

        public Builder sizeBytes(@Nullable Long sizeBytes) {
            this.sizeBytes = sizeBytes;
            return this;
        }

        public Builder mtimeMs(@Nullable Long mtimeMs) {
            this.mtimeMs = mtimeMs;
            return this;
        }

        public Builder rule(@Nullable String rule) {
            this.rule = rule;
            return this;
        }

        public Builder reasonCode(@Nullable String reasonCode) {
            this.reasonCode = reasonCode;
            return this;
        }

        public Builder result(@Nullable String result) {
            this.result = result;
            return this;
        }

        public Builder dimensions(Map<String, String> dimensions) {
            this.dimensions = dimensions;
            return this;
        }

        public Builder metrics(Map<String, Long> metrics) {
            this.metrics = metrics;
            return this;
        }

        public Builder flags(Map<String, Boolean> flags) {
            this.flags = flags;
            return this;
        }

        public AuditEvent build() {
            return new AuditEvent(this);
        }
    }
}
