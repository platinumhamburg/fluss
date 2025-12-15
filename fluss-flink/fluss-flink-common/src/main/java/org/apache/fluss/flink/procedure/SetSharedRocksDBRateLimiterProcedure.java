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
import org.apache.fluss.config.cluster.AlterConfig;
import org.apache.fluss.config.cluster.AlterConfigOpType;
import org.apache.fluss.config.cluster.ConfigEntry;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import java.util.Collection;
import java.util.Collections;

/**
 * Procedure to set RocksDB rate limiter dynamically via cluster configuration. The configuration
 * will be persisted in ZooKeeper and applied to all TabletServers.
 *
 * <p>Usage examples:
 *
 * <pre>
 * -- Set rate limiter to 200MB/s
 * CALL sys.set_shared_rocksdb_rate_limiter('200MB');
 *
 * -- Set rate limiter to 1GB/s
 * CALL sys.set_shared_rocksdb_rate_limiter('1GB');
 * </pre>
 */
public class SetSharedRocksDBRateLimiterProcedure extends ProcedureBase {

    @ProcedureHint(
            argument = {@ArgumentHint(name = "bytes_per_second", type = @DataTypeHint("STRING"))})
    public String[] call(ProcedureContext context, String bytesPerSecondStr) throws Exception {

        try {
            // Parse and validate input format first
            long bytesPerSecond = MemorySize.parse(bytesPerSecondStr).getBytes();

            // Get cluster configuration
            Collection<ConfigEntry> configs = admin.describeClusterConfigs().get();

            ConfigEntry configEntry = null;
            for (ConfigEntry entry : configs) {
                if (entry.key().equals(ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key())) {
                    configEntry = entry;
                }
            }

            if (null == configEntry) {
                throw new IllegalStateException(
                        "Failed to set shared RocksDB rate limiter: previous config not found, server does not support rate limiter setting.");
            }

            long oldValue = MemorySize.parseBytes(configEntry.value());

            if (oldValue < 0 && bytesPerSecond >= 0) {
                throw new IllegalArgumentException(
                        "Failed to set shared RocksDB rate limiter: rate limiter is disabled, cannot enable dynamically.");
            }

            if (oldValue >= 0 && bytesPerSecond < 0) {
                throw new IllegalArgumentException(
                        "Failed to set shared RocksDB rate limiter: rate limiter is enabled, cannot disable dynamically.");
            }

            // Construct configuration modification operation
            AlterConfig alterConfig =
                    new AlterConfig(
                            ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key(),
                            bytesPerSecondStr,
                            AlterConfigOpType.SET);

            // Call Admin API to modify cluster configuration
            admin.alterClusterConfigs(Collections.singletonList(alterConfig)).get();

            return new String[] {
                String.format(
                        "Successfully set shared RocksDB rate limiter to %s (%d bytes/sec) for all TabletServers. "
                                + "The configuration is persisted in ZooKeeper and will survive server restarts.",
                        bytesPerSecondStr, bytesPerSecond)
            };

        } catch (Exception e) {
            // Wrap other exceptions with more context
            throw new RuntimeException(
                    "Failed to set shared RocksDB rate limiter: " + e.getMessage(), e);
        }
    }
}
