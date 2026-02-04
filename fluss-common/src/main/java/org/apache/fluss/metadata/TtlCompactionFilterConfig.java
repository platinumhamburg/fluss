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

package org.apache.fluss.metadata;

import org.apache.fluss.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

/**
 * TTL compaction filter configuration.
 *
 * <p>TTL compaction filter removes entries older than a configured retention time based on a
 * timestamp column.
 *
 * @since 0.9
 */
@PublicEvolving
public class TtlCompactionFilterConfig implements CompactionFilterConfig {

    private static final long serialVersionUID = 1L;

    /** TTL prefix length is 8 bytes (Long timestamp). */
    public static final int TTL_PREFIX_LENGTH = 8;

    private final @Nullable String ttlColumn;
    private final long retentionMs;

    public TtlCompactionFilterConfig(@Nullable String ttlColumn, long retentionMs) {
        // When TTL is enabled (retentionMs > 0), ttlColumn must be specified
        if (retentionMs > 0 && (ttlColumn == null || ttlColumn.trim().isEmpty())) {
            throw new IllegalArgumentException(
                    "TTL column must be specified when TTL retention is positive (retentionMs: "
                            + retentionMs
                            + "). Please configure 'table.kv.compaction-filter.ttl.column'.");
        }
        this.ttlColumn = ttlColumn;
        this.retentionMs = retentionMs;
    }

    @Override
    public CompactionFilterType getType() {
        return CompactionFilterType.TTL;
    }

    @Override
    public int getPrefixLength() {
        return TTL_PREFIX_LENGTH;
    }

    /**
     * Returns the BIGINT column name that provides TTL timestamp (epoch millis).
     *
     * @return the column name, or empty if not configured
     */
    public Optional<String> getTtlColumn() {
        return Optional.ofNullable(ttlColumn);
    }

    /**
     * Returns TTL retention in milliseconds for KV values.
     *
     * <p>A non-positive value disables TTL.
     */
    public long getRetentionMs() {
        return retentionMs;
    }

    /** Returns whether TTL-based compaction is enabled. */
    public boolean isEnabled() {
        return retentionMs > 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TtlCompactionFilterConfig that = (TtlCompactionFilterConfig) o;
        return retentionMs == that.retentionMs && Objects.equals(ttlColumn, that.ttlColumn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ttlColumn, retentionMs);
    }

    @Override
    public String toString() {
        return "TtlCompactionFilterConfig{"
                + "ttlColumn='"
                + ttlColumn
                + '\''
                + ", retentionMs="
                + retentionMs
                + '}';
    }
}
