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

import java.io.Serializable;

/**
 * Base configuration for compaction filters.
 *
 * <p>Different compaction filter types may have different encoding prefix lengths and
 * configurations.
 *
 * @since 0.9
 */
@PublicEvolving
public interface CompactionFilterConfig extends Serializable {

    /** Returns the type of compaction filter. */
    CompactionFilterType getType();

    /**
     * Returns the encoding prefix length in bytes.
     *
     * <p>For example, TTL compaction filter uses 8 bytes (Long) to encode the timestamp prefix.
     */
    int getPrefixLength();

    /** Returns a NONE compaction filter config (no filtering). */
    static CompactionFilterConfig none() {
        return NoneCompactionFilterConfig.INSTANCE;
    }

    /** NONE compaction filter configuration. */
    final class NoneCompactionFilterConfig implements CompactionFilterConfig {
        private static final long serialVersionUID = 1L;
        private static final NoneCompactionFilterConfig INSTANCE = new NoneCompactionFilterConfig();

        private NoneCompactionFilterConfig() {}

        @Override
        public CompactionFilterType getType() {
            return CompactionFilterType.NONE;
        }

        @Override
        public int getPrefixLength() {
            return 0;
        }

        @Override
        public String toString() {
            return "NoneCompactionFilterConfig{}";
        }
    }
}
