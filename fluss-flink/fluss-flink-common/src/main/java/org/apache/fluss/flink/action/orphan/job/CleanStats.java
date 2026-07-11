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

package org.apache.fluss.flink.action.orphan.job;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.flink.action.orphan.audit.CleanupObjectType;
import org.apache.fluss.flink.action.orphan.audit.ScopeIdentity;
import org.apache.fluss.flink.action.orphan.audit.SkipReasonCode;

import java.io.Serializable;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

/**
 * Per-task cleanup statistics emitted by each {@link ScanAndCleanFunction} subtask. The scalar
 * counters are accumulated by {@link StatsAggregateOperator} via simple addition.
 */
@Internal
public final class CleanStats implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long scannedFiles;
    private final long plannedFiles;
    private final long plannedDirs;
    private final long plannedBytes;
    private final long deletedFiles;
    private final long emptyDirsRemoved;
    private final long deleteFailures;
    private final long bytesReclaimed;
    private final ScopeIdentity scope;
    private final Map<CleanupObjectType, CleanupCounters> byObjectType;
    private final Map<SkipReasonCode, Long> bySkipReason;

    public CleanStats(
            long scannedFiles,
            long plannedFiles,
            long plannedDirs,
            long plannedBytes,
            long deletedFiles,
            long emptyDirsRemoved,
            long deleteFailures,
            long bytesReclaimed) {
        this(
                ScopeIdentity.global(),
                new CleanupCounters(
                        scannedFiles,
                        plannedFiles,
                        plannedDirs,
                        plannedBytes,
                        deletedFiles,
                        emptyDirsRemoved,
                        deleteFailures,
                        bytesReclaimed),
                Collections.emptyMap(),
                Collections.emptyMap());
    }

    public CleanStats(
            ScopeIdentity scope,
            CleanupCounters counters,
            Map<CleanupObjectType, CleanupCounters> byObjectType,
            Map<SkipReasonCode, Long> bySkipReason) {
        this.scope = scope;
        EnumMap<CleanupObjectType, CleanupCounters> objectCopy =
                new EnumMap<>(CleanupObjectType.class);
        objectCopy.putAll(byObjectType);
        this.byObjectType = Collections.unmodifiableMap(objectCopy);
        EnumMap<SkipReasonCode, Long> reasonCopy = new EnumMap<>(SkipReasonCode.class);
        reasonCopy.putAll(bySkipReason);
        this.bySkipReason = Collections.unmodifiableMap(reasonCopy);
        this.scannedFiles = counters.scannedFiles();
        this.plannedFiles = counters.plannedFiles();
        this.plannedDirs = counters.plannedDirs();
        this.plannedBytes = counters.plannedBytes();
        this.deletedFiles = counters.deletedFiles();
        this.emptyDirsRemoved = counters.emptyDirsRemoved();
        this.deleteFailures = counters.deleteFailures();
        this.bytesReclaimed = counters.bytesReclaimed();
    }

    public static CleanStats empty() {
        return new CleanStats(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
    }

    public static Builder builder(ScopeIdentity scope) {
        return new Builder(scope);
    }

    public ScopeIdentity scope() {
        return scope;
    }

    public CleanupCounters counters() {
        return new CleanupCounters(
                scannedFiles,
                plannedFiles,
                plannedDirs,
                plannedBytes,
                deletedFiles,
                emptyDirsRemoved,
                deleteFailures,
                bytesReclaimed);
    }

    public Map<CleanupObjectType, CleanupCounters> byObjectType() {
        return byObjectType;
    }

    public Map<SkipReasonCode, Long> bySkipReason() {
        return bySkipReason;
    }

    public long scannedFiles() {
        return scannedFiles;
    }

    public long plannedFiles() {
        return plannedFiles;
    }

    public long plannedDirs() {
        return plannedDirs;
    }

    public long plannedBytes() {
        return plannedBytes;
    }

    public long deletedFiles() {
        return deletedFiles;
    }

    public long emptyDirsRemoved() {
        return emptyDirsRemoved;
    }

    public long deleteFailures() {
        return deleteFailures;
    }

    public long bytesReclaimed() {
        return bytesReclaimed;
    }

    /** Builder used by cleaners to retain low-cardinality audit dimensions. */
    public static final class Builder {

        private final ScopeIdentity scope;
        private CleanupCounters counters = CleanupCounters.empty();
        private final EnumMap<CleanupObjectType, CleanupCounters> byObjectType =
                new EnumMap<>(CleanupObjectType.class);
        private final EnumMap<SkipReasonCode, Long> bySkipReason =
                new EnumMap<>(SkipReasonCode.class);

        private Builder(ScopeIdentity scope) {
            this.scope = scope;
        }

        public Builder planned(CleanupObjectType type, long files, long bytes) {
            CleanupCounters delta = new CleanupCounters(0L, files, 0L, bytes, 0L, 0L, 0L, 0L);
            counters = counters.add(delta);
            byObjectType.put(
                    type, byObjectType.getOrDefault(type, CleanupCounters.empty()).add(delta));
            return this;
        }

        public Builder deleted(CleanupObjectType type, long files, long bytes) {
            CleanupCounters delta = new CleanupCounters(0L, 0L, 0L, 0L, files, 0L, 0L, bytes);
            counters = counters.add(delta);
            byObjectType.put(
                    type, byObjectType.getOrDefault(type, CleanupCounters.empty()).add(delta));
            return this;
        }

        public Builder skipped(SkipReasonCode reason, long count) {
            bySkipReason.put(reason, bySkipReason.getOrDefault(reason, 0L) + count);
            return this;
        }

        public CleanStats build() {
            return new CleanStats(scope, counters, byObjectType, bySkipReason);
        }
    }
}
