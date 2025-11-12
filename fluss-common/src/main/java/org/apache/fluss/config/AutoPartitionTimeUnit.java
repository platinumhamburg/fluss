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

/**
 * Enum for time unit of auto partition.
 *
 * @since 0.2
 */
@PublicEvolving
public enum AutoPartitionTimeUnit {
    YEAR,
    QUARTER,
    MONTH,
    DAY,
    HOUR;

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
}
