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

package org.apache.fluss.server.kv;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.config.cluster.ConfigValidator;
import org.apache.fluss.exception.ConfigException;

import javax.annotation.Nullable;

/**
 * Stateless validator for RocksDB shared rate limiter configuration.
 *
 * <p>This validator can be registered on CoordinatorServer without requiring a KvManager instance,
 * enabling early validation of KV configs before they are persisted to ZooKeeper.
 *
 * <p>The validator enforces the following rules for RocksDB shared rate limiter:
 *
 * <ul>
 *   <li>Cannot enable rate limiter dynamically if it was not enabled at server startup
 *   <li>Cannot disable rate limiter dynamically if it was enabled at server startup
 *   <li>Can only adjust the rate limit value when it's already enabled
 *   <li>Rate limit value must be within valid range (0 to 10GB/s)
 * </ul>
 */
public class KvConfigValidator implements ConfigValidator {

    private static final long MAX_RATE_LIMIT_BYTES = 10L * 1024 * 1024 * 1024; // 10GB/s

    private final boolean isRateLimiterEnabledAtStartup;

    /**
     * Creates a KV config validator for rate limiter configuration.
     *
     * @param initialConfig the initial configuration when server starts
     */
    public KvConfigValidator(Configuration initialConfig) {
        this.isRateLimiterEnabledAtStartup =
                initialConfig.get(ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC).getBytes()
                        > 0;
    }

    @Override
    public String configKey() {
        return ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key();
    }

    @Override
    public void validate(@Nullable String oldValue, @Nullable String newValue)
            throws ConfigException {
        // Parse old and new values
        // Note: if oldValue is null, it means the config was not set dynamically before,
        // we should consider the startup value (from static config or default)
        long oldBytes = parseMemorySize(oldValue);
        long newBytes = parseMemorySize(newValue);

        // When oldValue is null, treat it as the startup configuration state
        boolean wasEnabled = oldValue != null ? (oldBytes > 0) : isRateLimiterEnabledAtStartup;
        boolean willBeEnabled = newBytes > 0;

        // Rule 1: Cannot enable rate limiter dynamically if it was not enabled at startup
        if (!isRateLimiterEnabledAtStartup && willBeEnabled) {
            throw new ConfigException(
                    String.format(
                            "Cannot enable shared RocksDB rate limiter dynamically. "
                                    + "RateLimiter was not enabled at server startup. "
                                    + "To enable shared rate limiter, please set '%s' to a positive value "
                                    + "in server configuration and restart. Current attempt: '%s' -> '%s'",
                            configKey(), oldValue, newValue));
        }

        // Rule 2: Cannot disable rate limiter dynamically if it was enabled at startup
        if (isRateLimiterEnabledAtStartup && wasEnabled && !willBeEnabled) {
            throw new ConfigException(
                    String.format(
                            "Cannot disable shared RocksDB rate limiter dynamically. "
                                    + "To disable it, please restart the server with '%s' set to 0. "
                                    + "Dynamic disabling is not supported. Current attempt: '%s' -> '%s'",
                            configKey(), oldValue, newValue));
        }

        // Rule 3: Validate value range when enabled
        if (newBytes > 0 && newBytes > MAX_RATE_LIMIT_BYTES) {
            throw new ConfigException(
                    String.format(
                            "Rate limiter bytes per second (%s, %d bytes) exceeds maximum allowed "
                                    + "value: %d bytes (10GB/s)",
                            newValue, newBytes, MAX_RATE_LIMIT_BYTES));
        }

        // Rule 4: Ensure value is not negative
        if (newBytes < 0) {
            throw new ConfigException(
                    "Rate limiter bytes per second cannot be negative, got: " + newValue);
        }
    }

    /**
     * Parse memory size string to bytes. Returns 0 if value is null or invalid.
     *
     * @param value the memory size string (e.g., "100mb", "1gb")
     * @return bytes value, or 0 if null/invalid
     */
    private long parseMemorySize(@Nullable String value) {
        if (value == null || value.isEmpty()) {
            return 0;
        }
        try {
            return MemorySize.parse(value).getBytes();
        } catch (IllegalArgumentException e) {
            // Return 0 for invalid values, actual validation will catch it
            return 0;
        }
    }
}
