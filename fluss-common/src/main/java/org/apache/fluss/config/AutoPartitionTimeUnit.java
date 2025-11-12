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

package org.apache.fluss.config;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.shaded.guava32.com.google.common.cache.Cache;
import org.apache.fluss.shaded.guava32.com.google.common.cache.CacheBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Enum for time unit of auto partition.
 *
 * @since 0.2
 */
@PublicEvolving
public enum AutoPartitionTimeUnit {
    YEAR(5), // Typically only recent 5 years are used
    QUARTER(20), // 4 quarters per year, 5 years = 20 quarters
    MONTH(60), // 12 months per year, 5 years = 60 months
    DAY(366), // Maximum days in a year (leap year)
    HOUR(24 * 7); // One week of hours (168 hours)

    private static final Logger LOG = LoggerFactory.getLogger(AutoPartitionTimeUnit.class);

    // Each time unit has its own cache to avoid creating cache key objects
    private final Cache<String, Long> partitionTimestampCache;

    AutoPartitionTimeUnit(int cacheSize) {
        this.partitionTimestampCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build();
    }

    /**
     * Convert retention count to milliseconds.
     *
     * @param numToRetain number of units to retain
     * @return TTL in milliseconds
     */
    public long toApproximateTtlMillis(int numToRetain) {
        switch (this) {
            case YEAR:
                // Approximate using 366 days (leap year)
                return numToRetain * 366L * 24 * 3600_000L;
            case QUARTER:
                // Approximate using 92 days (3 months)
                return numToRetain * 92L * 24 * 3600_000L;
            case MONTH:
                // Approximate using 31 days
                return numToRetain * 31L * 24 * 3600_000L;
            case DAY:
                return numToRetain * 24 * 3600_000L;
            case HOUR:
                return numToRetain * 3600_000L;
            default:
                throw new IllegalStateException("Unknown time unit: " + this);
        }
    }

    /**
     * Parse partition string value to milliseconds timestamp with caching. Each time unit maintains
     * its own cache, using the partition string directly as the cache key to avoid creating
     * additional cache key objects.
     *
     * <p>The string format depends on the time unit:
     *
     * <ul>
     *   <li>HOUR: "yyyyMMddHH" (e.g., "2025050808")
     *   <li>DAY: "yyyyMMdd" (e.g., "20250508")
     *   <li>MONTH: "yyyyMM" (e.g., "202505")
     *   <li>YEAR: "yyyy" (e.g., "2025")
     *   <li>QUARTER: "yyyyQ#" (e.g., "2025Q2")
     * </ul>
     *
     * @param partitionString the partition string value
     * @return the timestamp in milliseconds, or -1 if parsing fails
     */
    public long parsePartitionStringToTimestamp(String partitionString) {
        if (partitionString == null || partitionString.isEmpty()) {
            return -1;
        }

        try {
            // Use cache to avoid repeated parsing, cache key is just the partition string
            return partitionTimestampCache.get(
                    partitionString, () -> doParsePartitionStringToTimestamp(partitionString));
        } catch (Exception e) {
            LOG.warn(
                    "Failed to parse partition string '{}' with time unit {}: {}",
                    partitionString,
                    this,
                    e.getMessage());
            return -1;
        }
    }

    /**
     * Actual implementation of partition string to timestamp conversion without caching.
     *
     * @param partitionString the partition string value
     * @return the timestamp in milliseconds
     */
    private long doParsePartitionStringToTimestamp(String partitionString) {
        switch (this) {
            case HOUR:
                // Format: yyyyMMddHH
                DateTimeFormatter hourFormatter = DateTimeFormatter.ofPattern("yyyyMMddHH");
                LocalDateTime hourDateTime = LocalDateTime.parse(partitionString, hourFormatter);
                return hourDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();

            case DAY:
                // Format: yyyyMMdd
                DateTimeFormatter dayFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
                LocalDate dayDate = LocalDate.parse(partitionString, dayFormatter);
                return dayDate.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();

            case MONTH:
                // Format: yyyyMM
                LocalDate monthDate =
                        LocalDate.parse(
                                partitionString + "01", DateTimeFormatter.ofPattern("yyyyMMdd"));
                return monthDate.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();

            case YEAR:
                // Format: yyyy
                LocalDate yearDate =
                        LocalDate.parse(
                                partitionString + "0101", DateTimeFormatter.ofPattern("yyyyMMdd"));
                return yearDate.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();

            case QUARTER:
                // Format: yyyyQ (e.g., "2025Q2")
                // Extract year and quarter
                int qIndex = partitionString.indexOf('Q');
                if (qIndex > 0) {
                    int year = Integer.parseInt(partitionString.substring(0, qIndex));
                    int quarter = Integer.parseInt(partitionString.substring(qIndex + 1));
                    int month = (quarter - 1) * 3 + 1; // Q1->1, Q2->4, Q3->7, Q4->10
                    LocalDate quarterDate = LocalDate.of(year, month, 1);
                    return quarterDate.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
                }
                return -1;

            default:
                LOG.warn("Unsupported AutoPartitionTimeUnit for string parsing: {}", this);
                return -1;
        }
    }
}
