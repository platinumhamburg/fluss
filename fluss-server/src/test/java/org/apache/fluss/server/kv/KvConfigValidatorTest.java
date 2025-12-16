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
import org.apache.fluss.exception.ConfigException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link KvConfigValidator}. */
class KvConfigValidatorTest {

    @Test
    void testConfigKey() {
        Configuration config = new Configuration();
        KvConfigValidator validator = new KvConfigValidator(config);

        assertThat(validator.configKey())
                .isEqualTo(ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key());
    }

    @Test
    void testCannotEnableDynamicallyWhenNotEnabledAtStartup() {
        // Rate limiter disabled at startup (0 or not set)
        Configuration config = new Configuration();
        config.setString(ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key(), "0b");

        KvConfigValidator validator = new KvConfigValidator(config);

        // Try to enable dynamically should fail
        assertThatThrownBy(() -> validator.validate("0b", "100MB"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Cannot enable shared RocksDB rate limiter dynamically")
                .hasMessageContaining("RateLimiter was not enabled at server startup");
    }

    @Test
    void testCannotDisableDynamicallyWhenEnabledAtStartup() {
        // Rate limiter enabled at startup
        Configuration config = new Configuration();
        config.setString(ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key(), "100MB");

        KvConfigValidator validator = new KvConfigValidator(config);

        // Try to disable dynamically should fail
        assertThatThrownBy(() -> validator.validate("100MB", "0b"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Cannot disable shared RocksDB rate limiter dynamically");
    }

    @Test
    void testCanAdjustValueWhenEnabled() {
        // Rate limiter enabled at startup
        Configuration config = new Configuration();
        config.setString(ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key(), "100MB");

        KvConfigValidator validator = new KvConfigValidator(config);

        // Adjust value should succeed
        assertThatCode(() -> validator.validate("100MB", "200MB")).doesNotThrowAnyException();
        assertThatCode(() -> validator.validate("200MB", "1GB")).doesNotThrowAnyException();
        assertThatCode(() -> validator.validate("1GB", "500MB")).doesNotThrowAnyException();
    }

    @Test
    void testHandleNullOldValue() {
        // Rate limiter enabled at startup
        Configuration config = new Configuration();
        config.setString(ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key(), "100MB");

        KvConfigValidator validator = new KvConfigValidator(config);

        // oldValue is null means config was never dynamically changed
        // Should use startup configuration state
        assertThatCode(() -> validator.validate(null, "200MB")).doesNotThrowAnyException();

        // But cannot disable when enabled at startup (even oldValue is null)
        assertThatThrownBy(() -> validator.validate(null, "0b"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Cannot disable shared RocksDB rate limiter dynamically");
    }

    @Test
    void testHandleNullOldValueWhenDisabledAtStartup() {
        // Rate limiter disabled at startup
        Configuration config = new Configuration();
        config.setString(ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key(), "0b");

        KvConfigValidator validator = new KvConfigValidator(config);

        // oldValue is null, startup state is disabled
        // Try to enable should fail
        assertThatThrownBy(() -> validator.validate(null, "100MB"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Cannot enable shared RocksDB rate limiter dynamically");

        // Keep disabled should succeed
        assertThatCode(() -> validator.validate(null, "0b")).doesNotThrowAnyException();
    }

    @Test
    void testValueRangeValidation() {
        Configuration config = new Configuration();
        config.setString(ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key(), "100MB");

        KvConfigValidator validator = new KvConfigValidator(config);

        // Valid range
        assertThatCode(() -> validator.validate("100MB", "1GB")).doesNotThrowAnyException();
        assertThatCode(() -> validator.validate("1GB", "5GB")).doesNotThrowAnyException();

        // Exceeds maximum (10GB)
        assertThatThrownBy(() -> validator.validate("1GB", "20GB"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("exceeds maximum allowed value");
    }

    @Test
    void testNegativeValue() {
        Configuration config = new Configuration();
        config.setString(ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key(), "100MB");

        KvConfigValidator validator = new KvConfigValidator(config);

        // Negative value should fail
        // Note: parseMemorySize returns 0 for invalid "-1", which triggers disable check
        assertThatThrownBy(() -> validator.validate("100MB", "-1"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Cannot disable shared RocksDB rate limiter dynamically");
    }

    @Test
    void testKeepSameValue() {
        Configuration config = new Configuration();
        config.setString(ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key(), "100MB");

        KvConfigValidator validator = new KvConfigValidator(config);

        // Same value should be valid
        assertThatCode(() -> validator.validate("100MB", "100MB")).doesNotThrowAnyException();
    }

    @Test
    void testInvalidMemorySizeFormat() {
        Configuration config = new Configuration();
        config.setString(ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key(), "100MB");

        KvConfigValidator validator = new KvConfigValidator(config);

        // Invalid format - parseMemorySize will return 0, which triggers disable check
        assertThatThrownBy(() -> validator.validate("100MB", "invalid"))
                .isInstanceOf(ConfigException.class);
    }
}
