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

package org.apache.fluss.flink.action.orphan.config;

import org.apache.fluss.flink.adapter.MultipleParameterToolAdapter;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link OrphanCleanConfig}. */
class OrphanCleanConfigTest {

    private static final DateTimeFormatter CUTOFF_FORMATTER =
            DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    @Test
    void parsesAllDatabasesWithDefaults() {
        long beforeParse = System.currentTimeMillis();
        OrphanCleanConfig config =
                OrphanCleanConfig.fromParams(
                        MultipleParameterToolAdapter.fromArgs(
                                new String[] {"--bootstrap-server", "h:9123", "--all-databases"}));
        long afterParse = System.currentTimeMillis();

        assertThat(config.allDatabases()).isTrue();
        assertThat(config.database()).isEmpty();
        long olderThanLow = beforeParse - Duration.ofDays(3).toMillis();
        long olderThanHigh = afterParse - Duration.ofDays(3).toMillis();
        assertThat(config.olderThanMillis()).isBetween(olderThanLow, olderThanHigh);
        assertThat(config.dryRun()).isFalse();
        assertThat(config.remoteFsOpRateLimitPerSecond()).isEqualTo(100L);
        assertThat(config.allowDeleteManifest()).isFalse();
        assertThat(config.allowCleanOrphanTables()).isFalse();
        assertThat(config.allowCleanOrphanPartitions()).isFalse();
        assertThat(config.postRunWait()).isEqualTo(Duration.ZERO);
        assertThat(config.progressLogInterval()).isEqualTo(Duration.ZERO);
    }

    @Test
    void parsesPostRunWait() {
        assertThat(config("--all-databases", "--post-run-wait", "10m").postRunWait())
                .isEqualTo(Duration.ofMinutes(10));
    }

    @Test
    void rejectsNegativePostRunWait() {
        assertThatThrownBy(() -> config("--all-databases", "--post-run-wait", "-1s"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("--post-run-wait must be between 0ms and 1h");
    }

    @Test
    void rejectsPostRunWaitAboveOneHour() {
        assertThatThrownBy(() -> config("--all-databases", "--post-run-wait", "61m"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("--post-run-wait must be between 0ms and 1h");
    }

    @Test
    void parsesProgressLogInterval() {
        assertThat(
                        config("--all-databases", "--progress-log-interval", "30s")
                                .progressLogInterval())
                .isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void acceptsExplicitZeroProgressLogInterval() {
        assertThat(
                        config("--all-databases", "--progress-log-interval", "0ms")
                                .progressLogInterval())
                .isEqualTo(Duration.ZERO);
    }

    @Test
    void rejectsProgressLogIntervalBelowOneSecond() {
        assertThatThrownBy(() -> config("--all-databases", "--progress-log-interval", "500ms"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("--progress-log-interval must be 0ms or between 1s and 1h");
    }

    @Test
    void rejectsProgressLogIntervalAboveOneHour() {
        assertThatThrownBy(() -> config("--all-databases", "--progress-log-interval", "61m"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("--progress-log-interval must be 0ms or between 1s and 1h");
    }

    @Test
    void remoteFsOpRateLimitParsed() {
        OrphanCleanConfig cfg =
                OrphanCleanConfig.fromParams(
                        MultipleParameterToolAdapter.fromArgs(
                                new String[] {
                                    "--bootstrap-server",
                                    "h:9123",
                                    "--all-databases",
                                    "--remote-fs-op-rate-limit-per-second",
                                    "42"
                                }));
        assertThat(cfg.remoteFsOpRateLimitPerSecond()).isEqualTo(42L);
    }

    @Test
    void remoteFsOpRateLimitMustBePositive() {
        assertThatThrownBy(
                        () ->
                                OrphanCleanConfig.fromParams(
                                        MultipleParameterToolAdapter.fromArgs(
                                                new String[] {
                                                    "--bootstrap-server",
                                                    "h:9123",
                                                    "--all-databases",
                                                    "--remote-fs-op-rate-limit-per-second",
                                                    "0"
                                                })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("--remote-fs-op-rate-limit-per-second must be positive");
    }

    @Test
    void databaseAndAllDatabasesAreMutuallyExclusive() {
        assertThatThrownBy(
                        () ->
                                OrphanCleanConfig.fromParams(
                                        MultipleParameterToolAdapter.fromArgs(
                                                new String[] {
                                                    "--bootstrap-server",
                                                    "h:9123",
                                                    "--database",
                                                    "x",
                                                    "--all-databases"
                                                })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mutually exclusive");
    }

    @Test
    void cutoffCloserThanOneDayRejected() {
        OffsetDateTime tooClose = OffsetDateTime.now(ZoneOffset.UTC).minusMinutes(30);
        assertThatThrownBy(
                        () ->
                                OrphanCleanConfig.fromParams(
                                        MultipleParameterToolAdapter.fromArgs(
                                                new String[] {
                                                    "--bootstrap-server",
                                                    "h:9123",
                                                    "--all-databases",
                                                    "--older-than",
                                                    tooClose.format(CUTOFF_FORMATTER)
                                                })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least 1d before now");
    }

    @Test
    void cutoffWithoutExplicitOffsetRejected() {
        assertThatThrownBy(
                        () ->
                                OrphanCleanConfig.fromParams(
                                        MultipleParameterToolAdapter.fromArgs(
                                                new String[] {
                                                    "--bootstrap-server",
                                                    "h:9123",
                                                    "--all-databases",
                                                    "--older-than",
                                                    "2024-01-01 00:00:00"
                                                })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ISO-8601");
    }

    @Test
    void cutoffWithExplicitOffsetParsed() {
        OffsetDateTime cutoff = OffsetDateTime.now(ZoneOffset.UTC).minusDays(2).withNano(0);
        OrphanCleanConfig cfg =
                OrphanCleanConfig.fromParams(
                        MultipleParameterToolAdapter.fromArgs(
                                new String[] {
                                    "--bootstrap-server",
                                    "h:9123",
                                    "--all-databases",
                                    "--older-than",
                                    cutoff.format(CUTOFF_FORMATTER)
                                }));
        assertThat(cfg.olderThanMillis()).isEqualTo(cutoff.toInstant().toEpochMilli());
    }

    @Test
    void tableCannotBeUsedWithAllDatabases() {
        assertThatThrownBy(
                        () ->
                                OrphanCleanConfig.fromParams(
                                        MultipleParameterToolAdapter.fromArgs(
                                                new String[] {
                                                    "--bootstrap-server",
                                                    "h:9123",
                                                    "--all-databases",
                                                    "--table",
                                                    "t1"
                                                })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("--table requires --database");
    }

    @Test
    void bootstrapServerRequired() {
        assertThatThrownBy(
                        () ->
                                OrphanCleanConfig.fromParams(
                                        MultipleParameterToolAdapter.fromArgs(
                                                new String[] {"--all-databases"})))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bootstrap-server");
    }

    @Test
    void optInFlagsParsed() {
        OrphanCleanConfig cfg =
                OrphanCleanConfig.fromParams(
                        MultipleParameterToolAdapter.fromArgs(
                                new String[] {
                                    "--bootstrap-server",
                                    "x:1",
                                    "--all-databases",
                                    "--allow-delete-manifest",
                                    "--allow-clean-orphan-tables",
                                    "--allow-clean-orphan-partitions"
                                }));
        assertThat(cfg.allowDeleteManifest()).isTrue();
        assertThat(cfg.allowCleanOrphanTables()).isTrue();
        assertThat(cfg.allowCleanOrphanPartitions()).isTrue();
    }

    @Test
    void extraConfigsParsed() {
        OrphanCleanConfig cfg =
                OrphanCleanConfig.fromParams(
                        MultipleParameterToolAdapter.fromArgs(
                                new String[] {
                                    "--bootstrap-server",
                                    "h:9123",
                                    "--all-databases",
                                    "--conf",
                                    "fs.oss.accessKeyId=myKey",
                                    "--conf",
                                    "fs.oss.accessKeySecret=mySecret",
                                    "--conf",
                                    "fs.oss.endpoint=oss-cn-hangzhou.aliyuncs.com"
                                }));
        assertThat(cfg.extraConfigs()).hasSize(3);
        assertThat(cfg.extraConfigs().get("fs.oss.accessKeyId")).isEqualTo("myKey");
        assertThat(cfg.extraConfigs().get("fs.oss.accessKeySecret")).isEqualTo("mySecret");
        assertThat(cfg.extraConfigs().get("fs.oss.endpoint"))
                .isEqualTo("oss-cn-hangzhou.aliyuncs.com");
    }

    @Test
    void extraConfigsEmptyWhenNotProvided() {
        OrphanCleanConfig cfg =
                OrphanCleanConfig.fromParams(
                        MultipleParameterToolAdapter.fromArgs(
                                new String[] {"--bootstrap-server", "h:9123", "--all-databases"}));
        assertThat(cfg.extraConfigs()).isEmpty();
    }

    @Test
    void extraConfigsRejectsMalformedEntry() {
        assertThatThrownBy(
                        () ->
                                OrphanCleanConfig.fromParams(
                                        MultipleParameterToolAdapter.fromArgs(
                                                new String[] {
                                                    "--bootstrap-server",
                                                    "h:9123",
                                                    "--all-databases",
                                                    "--conf",
                                                    "noEqualsSign"
                                                })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("key=value");
    }

    private static OrphanCleanConfig config(String... args) {
        String[] fullArgs = new String[args.length + 2];
        fullArgs[0] = "--bootstrap-server";
        fullArgs[1] = "h:9123";
        System.arraycopy(args, 0, fullArgs, 2, args.length);
        return OrphanCleanConfig.fromParams(MultipleParameterToolAdapter.fromArgs(fullArgs));
    }
}
