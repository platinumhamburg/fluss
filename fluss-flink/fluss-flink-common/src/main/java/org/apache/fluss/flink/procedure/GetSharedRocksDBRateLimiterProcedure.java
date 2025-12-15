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

package org.apache.fluss.flink.procedure;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.config.cluster.ConfigEntry;

import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import java.util.Collection;

/**
 * Procedure to get current RocksDB rate limiter configuration from cluster config.
 *
 * <p>Usage:
 *
 * <pre>
 * CALL sys.get_shared_rocksdb_rate_limiter();
 * </pre>
 */
public class GetSharedRocksDBRateLimiterProcedure extends ProcedureBase {

    @ProcedureHint(argument = {})
    public String[] call(ProcedureContext context) throws Exception {

        try {
            // Get cluster configuration
            Collection<ConfigEntry> configs = admin.describeClusterConfigs().get();

            // Find shared rate limiter configuration
            String rateLimiterKey = ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key();

            for (ConfigEntry entry : configs) {
                if (entry.key().equals(rateLimiterKey)) {
                    String value = entry.value();
                    long bytesPerSec = MemorySize.parseBytes(value);

                    if (bytesPerSec == 0) {
                        return new String[] {
                            "Shared RocksDB rate limiter is disabled (0 bytes/sec)"
                        };
                    }

                    String source =
                            entry.source() != null ? " [source: " + entry.source() + "]" : "";
                    return new String[] {
                        String.format(
                                "Shared RocksDB rate limiter: %s%s",
                                new MemorySize(bytesPerSec).toHumanReadableString(), source)
                    };
                }
            }

            // Not found, return default value
            MemorySize defaultValue =
                    ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.defaultValue();

            return new String[] {
                String.format(
                        "Shared RocksDB rate limiter: %s (default)",
                        defaultValue.toHumanReadableString())
            };

        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to get shared RocksDB rate limiter config: " + e.getMessage(), e);
        }
    }
}
