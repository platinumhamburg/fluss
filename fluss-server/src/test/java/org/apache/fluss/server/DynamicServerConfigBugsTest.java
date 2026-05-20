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

package org.apache.fluss.server;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.ConfigValidator;
import org.apache.fluss.config.cluster.ServerReconfigurable;
import org.apache.fluss.exception.ConfigException;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Targeted tests for logic bugs in {@link DynamicServerConfig#computeEffectiveChanges} and the
 * surrounding apply pipeline.
 *
 * <p>Each test demonstrates one specific defect; they all fail against the unfixed implementation
 * and should pass after the fix.
 */
class DynamicServerConfigBugsTest {

    /**
     * BUG 1: validation bypass for keys that already exist in {@code dynamicConfigs}.
     *
     * <p>When a key was previously dynamic and a new (invalid) value is pushed with {@code
     * skipErrorConfig=true}, validation reports it as skipped — but the unvalidated value still
     * lands in the applied configuration.
     */
    @Test
    void invalidValueForReModifiedKeyMustNotBeApplied() throws Exception {
        Configuration init = new Configuration();
        init.setInt(ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER, 1);
        DynamicServerConfig cfg = new DynamicServerConfig(init);

        RecordingReconfigurable rec = new RecordingReconfigurable();
        cfg.register(rec);

        // First, push a valid dynamic value.
        cfg.updateDynamicConfig(
                Collections.singletonMap(
                        ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER.key(), "2"),
                false);
        assertThat(rec.lastSeenRaw.get()).isEqualTo("2"); // sanity: dynamic config applied

        // Now push an invalid value with skipErrorConfig=true.
        cfg.updateDynamicConfig(
                Collections.singletonMap(
                        ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER.key(), "not-an-int"),
                true);

        // Expectation: invalid value MUST NOT reach the reconfigurable; the value seen by the
        // reconfigurable stays at the last valid one ("2"), never the unparseable string.
        assertThat(rec.lastSeenRaw.get()).isEqualTo("2");
    }

    /**
     * BUG 2: no-op updates trigger {@code reconfigure} unnecessarily.
     *
     * <p>{@code processDeletions} blindly stages every still-present dynamic key into {@code
     * effectiveChanges}, so {@code effectiveChanges.isEmpty()} is false even when nothing changed.
     */
    @Test
    void noOpUpdateMustNotTriggerReconfigure() throws Exception {
        Configuration init = new Configuration();
        init.setInt(ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER, 1);
        DynamicServerConfig cfg = new DynamicServerConfig(init);

        RecordingReconfigurable rec = new RecordingReconfigurable();
        cfg.register(rec);

        Map<String, String> dynamic = new HashMap<>();
        dynamic.put(ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER.key(), "2");
        cfg.updateDynamicConfig(dynamic, false);
        int callsAfterFirstApply = rec.applyCount.get();

        // Push the SAME map again — this is a no-op event (e.g. ZK re-notification).
        cfg.updateDynamicConfig(dynamic, false);

        // Expectation: no extra reconfigure happens.
        assertThat(rec.applyCount.get()).isEqualTo(callsAfterFirstApply);
    }

    /**
     * BUG 3: a {@link ConfigValidator} registered for a prefix-only config (e.g. {@code
     * datalake.*}) is silently skipped because the implementation gates validator invocation on
     * {@code configOption != null}.
     */
    @Test
    void validatorForPrefixOnlyConfigMustBeInvoked() throws Exception {
        Configuration init = new Configuration();
        DynamicServerConfig cfg = new DynamicServerConfig(init);

        RecordingValidator validator = new RecordingValidator("datalake.endpoint");
        cfg.registerValidator(validator);
        // Validator throws when the new value is "bad".
        validator.rejectIfNewValueEquals("bad");

        // skipErrorConfig=false: an invalid value MUST throw.
        assertThatThrownBy(
                        () ->
                                cfg.updateDynamicConfig(
                                        Collections.singletonMap("datalake.endpoint", "bad"),
                                        false))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("rejected");

        // Validator must have been invoked.
        assertThat(validator.invocations.get()).isGreaterThan(0);
    }

    /**
     * BUG 4: when {@code skipErrorConfig=true}, a {@link ServerReconfigurable} whose {@code
     * validate} fails should not have {@code reconfigure} called against it. The original code logs
     * the validation failure but still proceeds to {@code reconfigure}.
     */
    @Test
    void validateFailureMustGateReconfigure() throws Exception {
        Configuration init = new Configuration();
        init.setInt(ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER, 1);
        DynamicServerConfig cfg = new DynamicServerConfig(init);

        RejectingReconfigurable rejecting = new RejectingReconfigurable();
        cfg.register(rejecting);

        cfg.updateDynamicConfig(
                Collections.singletonMap(
                        ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER.key(), "5"),
                true /* skipErrorConfig */);

        assertThat(rejecting.validateCalls.get()).isEqualTo(1);
        // Expectation: reconfigure was not called because validate threw.
        assertThat(rejecting.reconfigureCalls.get()).isEqualTo(0);
    }

    /**
     * BUG 5: rollback iteration is not fault-tolerant — if one rollback throws, the others are
     * skipped and the original failure is masked. The original throwable must still propagate (with
     * rollback failures recorded as suppressed exceptions).
     */
    @Test
    void rollbackFailureMustNotMaskOriginalException() {
        Configuration init = new Configuration();
        init.setInt(ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER, 1);
        DynamicServerConfig cfg = new DynamicServerConfig(init);

        // First reconfigurable: rollback throws.
        RollbackThrowingReconfigurable a = new RollbackThrowingReconfigurable();
        // Second reconfigurable: forward apply throws (causes rollback path).
        FailOnApplyReconfigurable b = new FailOnApplyReconfigurable();

        cfg.register(a);
        cfg.register(b);

        // skipErrorConfig=false: forward apply error should propagate.
        assertThatThrownBy(
                        () ->
                                cfg.updateDynamicConfig(
                                        Collections.singletonMap(
                                                ConfigOptions
                                                        .LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER
                                                        .key(),
                                                "5"),
                                        false))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("apply BOOM");
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Records the last seen raw string value of LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER.
     *
     * <p>Uses raw map access (not {@code getInt}) so that an unparseable value reaching {@code
     * reconfigure} can be observed via {@code lastSeenRaw} rather than triggering an
     * IllegalArgumentException that would mask the assertion.
     */
    private static final class RecordingReconfigurable implements ServerReconfigurable {
        final AtomicReference<String> lastSeenRaw = new AtomicReference<>();
        final AtomicInteger applyCount = new AtomicInteger();

        @Override
        public void validate(Configuration newConfig) {
            // accept everything
        }

        @Override
        public void reconfigure(Configuration newConfig) {
            lastSeenRaw.set(
                    newConfig
                            .toMap()
                            .get(ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER.key()));
            applyCount.incrementAndGet();
        }
    }

    /** Validator that rejects whenever the new (raw String) value equals a forbidden value. */
    private static final class RecordingValidator implements ConfigValidator<String> {
        private final String key;
        private final AtomicReference<String> reject = new AtomicReference<>();
        final AtomicInteger invocations = new AtomicInteger();

        RecordingValidator(String key) {
            this.key = key;
        }

        void rejectIfNewValueEquals(String forbidden) {
            this.reject.set(forbidden);
        }

        @Override
        public String configKey() {
            return key;
        }

        @Override
        public void validate(String oldValue, String newValue) {
            invocations.incrementAndGet();
            String forbidden = reject.get();
            if (forbidden != null && forbidden.equals(newValue)) {
                throw new ConfigException("rejected by validator: " + newValue);
            }
        }
    }

    /** {@link #validate} throws every time, {@link #reconfigure} would record a call. */
    private static final class RejectingReconfigurable implements ServerReconfigurable {
        final AtomicInteger validateCalls = new AtomicInteger();
        final AtomicInteger reconfigureCalls = new AtomicInteger();

        @Override
        public void validate(Configuration newConfig) {
            validateCalls.incrementAndGet();
            throw new ConfigException("validate refuses everything");
        }

        @Override
        public void reconfigure(Configuration newConfig) {
            reconfigureCalls.incrementAndGet();
        }
    }

    /** Forward apply throws on second call — sufficient to drive the rollback path. */
    private static final class FailOnApplyReconfigurable implements ServerReconfigurable {
        final AtomicInteger calls = new AtomicInteger();

        @Override
        public void validate(Configuration newConfig) {
            // accept
        }

        @Override
        public void reconfigure(Configuration newConfig) {
            int n = calls.incrementAndGet();
            if (n == 1) {
                throw new ConfigException("apply BOOM");
            }
        }
    }

    /** Rollback path throws — should not mask the original apply error. */
    private static final class RollbackThrowingReconfigurable implements ServerReconfigurable {
        final List<Configuration> applied = new ArrayList<>();

        @Override
        public void validate(Configuration newConfig) {
            // accept
        }

        @Override
        public void reconfigure(Configuration newConfig) {
            applied.add(newConfig);
            // First call: forward apply succeeds. Second call (rollback): throw.
            if (applied.size() >= 2) {
                throw new ConfigException("rollback failed");
            }
        }
    }
}
