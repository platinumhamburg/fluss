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

import org.apache.fluss.flink.action.orphan.audit.AuditReporterSpec;
import org.apache.fluss.flink.action.orphan.audit.AuditReporterSpec.ReporterSpec;
import org.apache.fluss.flink.adapter.MultipleParameterToolAdapter;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** Tests for {@link OrphanCleanConfig}. */
class OrphanCleanConfigTest {

    private static final String RUN_ID = "3b5939f1-9837-49d8-8a02-945273a0d7e2";
    private static final String UPPERCASE_RUN_ID = "3B5939F1-9837-49D8-8A02-945273A0D7E2";
    private static final String NON_CANONICAL_UUID = "1-1-1-1-1";

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
        assertThat(config.olderThanConfigured()).isFalse();
        assertThat(config.dryRun()).isFalse();
        assertThat(config.remoteFsOpRateLimitPerSecond()).isEqualTo(100L);
        assertThat(config.scopeEnumerationConcurrency()).isEqualTo(1);
        assertThat(config.allowDeleteManifest()).isFalse();
        assertThat(config.allowCleanOrphanTables()).isFalse();
        assertThat(config.allowCleanOrphanPartitions()).isFalse();
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
    void scopeEnumerationConcurrencyParsed() {
        OrphanCleanConfig config =
                OrphanCleanConfig.fromParams(
                        MultipleParameterToolAdapter.fromArgs(
                                new String[] {
                                    "--bootstrap-server",
                                    "h:9123",
                                    "--all-databases",
                                    "--scope-enumeration-concurrency",
                                    "8"
                                }));

        assertThat(config.scopeEnumerationConcurrency()).isEqualTo(8);
    }

    @Test
    void scopeEnumerationConcurrencyMustBePositive() {
        assertThatThrownBy(
                        () ->
                                OrphanCleanConfig.fromParams(
                                        MultipleParameterToolAdapter.fromArgs(
                                                new String[] {
                                                    "--bootstrap-server",
                                                    "h:9123",
                                                    "--all-databases",
                                                    "--scope-enumeration-concurrency",
                                                    "0"
                                                })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("--scope-enumeration-concurrency must be positive");
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
        assertThat(cfg.olderThanConfigured()).isTrue();
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
    void parsesAuditReporterSpecWithOpaqueStringOptions() {
        OrphanCleanConfig cfg =
                OrphanCleanConfig.fromParams(
                        MultipleParameterToolAdapter.fromArgs(
                                new String[] {
                                    "--bootstrap-server",
                                    "h:9123",
                                    "--all-databases",
                                    "--conf",
                                    "audit.run-id=00000000-0000-0000-0000-000000000004",
                                    "--conf",
                                    "audit.reporters=testing",
                                    "--conf",
                                    "audit.reporter.testing.required=true",
                                    "--conf",
                                    "audit.reporter.testing.endpoint=opaque-value"
                                }));

        assertThat(cfg.auditReporterSpec().runId())
                .isEqualTo("00000000-0000-0000-0000-000000000004");
        assertThat(cfg.auditReporterSpec().reporters())
                .singleElement()
                .satisfies(
                        reporter -> {
                            assertThat(reporter.identifier()).isEqualTo("testing");
                            assertThat(reporter.required()).isTrue();
                            assertThat(reporter.options())
                                    .containsEntry("endpoint", "opaque-value");
                        });
    }

    @Test
    void extraConfigsRejectsMalformedEntry() {
        String rawEntry = "raw-entry-that-must-not-be-echoed";
        assertThatThrownBy(
                        () ->
                                OrphanCleanConfig.fromParams(
                                        MultipleParameterToolAdapter.fromArgs(
                                                new String[] {
                                                    "--bootstrap-server",
                                                    "h:9123",
                                                    "--all-databases",
                                                    "--conf",
                                                    rawEntry
                                                })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("--conf must be in key=value format")
                .hasMessageNotContaining(rawEntry);
    }

    @Test
    void missingRunIdGeneratesOneUuidPerConfig() {
        OrphanCleanConfig first = fromConfs();
        OrphanCleanConfig second = fromConfs();

        AuditReporterSpec firstSpec = first.auditReporterSpec();
        assertThat(first.auditReporterSpec()).isSameAs(firstSpec);
        assertThat(firstSpec.runId()).isEqualTo(UUID.fromString(firstSpec.runId()).toString());
        assertThat(firstSpec.reporters()).isEmpty();
        assertThat(second.auditReporterSpec().runId()).isNotEqualTo(firstSpec.runId());
    }

    @Test
    void explicitRunIdIsPreserved() {
        OrphanCleanConfig config = fromConfs("audit.run-id=" + RUN_ID);

        assertThat(config.auditReporterSpec().runId()).isEqualTo(RUN_ID);
        assertThat(config.extraConfigs()).isEmpty();
    }

    @Test
    void explicitRunIdRequiresCanonicalUuidText() {
        assertThatThrownBy(() -> fromConfs("audit.run-id=" + NON_CANONICAL_UUID))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("audit.run-id")
                .hasMessageNotContaining(NON_CANONICAL_UUID);
        OrphanCleanConfig config = fromConfs("audit.run-id=" + UPPERCASE_RUN_ID);

        assertThat(config.auditReporterSpec().runId()).isEqualTo(UPPERCASE_RUN_ID);
    }

    @Test
    void parsesOrderedReportersAndIsolatesTheirOptions() {
        OrphanCleanConfig config =
                fromConfs(
                        "audit.run-id=" + RUN_ID,
                        "audit.reporters=jdbc,sls",
                        "audit.reporter.jdbc.required=true",
                        "audit.reporter.jdbc.url=jdbc:postgresql://audit-db:5432/fluss",
                        "audit.reporter.jdbc.username=orphan_auditor",
                        "audit.reporter.jdbc.password-file=/var/run/secrets/orphan-jdbc/password",
                        "audit.reporter.sls.required=false",
                        "audit.reporter.sls.endpoint=cn-example.log.aliyuncs.com",
                        "audit.reporter.sls.access-key-id-file=/var/run/secrets/orphan-sls/access-key-id",
                        "audit.reporter.sls.access-key-secret-file=/var/run/secrets/orphan-sls/access-key-secret",
                        "fs.oss.accessKeySecret=filesystem-value",
                        "client.fs.protocol= client-value=with-equals ");

        List<ReporterSpec> reporters = config.auditReporterSpec().reporters();
        assertThat(reporters).extracting(ReporterSpec::identifier).containsExactly("jdbc", "sls");
        assertThat(reporters.get(0).required()).isTrue();
        assertThat(reporters.get(0).options())
                .containsExactly(
                        entry("url", "jdbc:postgresql://audit-db:5432/fluss"),
                        entry("username", "orphan_auditor"),
                        entry("password-file", "/var/run/secrets/orphan-jdbc/password"));
        assertThat(reporters.get(1).required()).isFalse();
        assertThat(reporters.get(1).options())
                .containsExactly(
                        entry("endpoint", "cn-example.log.aliyuncs.com"),
                        entry("access-key-id-file", "/var/run/secrets/orphan-sls/access-key-id"),
                        entry(
                                "access-key-secret-file",
                                "/var/run/secrets/orphan-sls/access-key-secret"));
        assertThat(config.extraConfigs())
                .containsOnly(
                        entry("fs.oss.accessKeySecret", "filesystem-value"),
                        entry("client.fs.protocol", " client-value=with-equals "));
        assertThat(config.extraConfigs().keySet()).noneMatch(key -> key.startsWith("audit."));
    }

    @Test
    void emptyReporterListIsAllowedAndRequiredDefaultsToTrue() {
        assertThat(fromConfs("audit.reporters=").auditReporterSpec().reporters()).isEmpty();

        ReporterSpec reporter =
                fromConfs(
                                "audit.reporters=jdbc",
                                "audit.reporter.jdbc.url=jdbc:postgresql://audit-db:5432/fluss")
                        .auditReporterSpec()
                        .reporters()
                        .get(0);
        assertThat(reporter.required()).isTrue();
        assertThat(reporter.options())
                .containsExactly(entry("url", "jdbc:postgresql://audit-db:5432/fluss"));
    }

    @Test
    void rejectsInvalidReporterListsWithoutEchoingValues() {
        for (String invalid :
                Arrays.asList(
                        "jdbc,jdbc",
                        "jdbc,,sls",
                        "log",
                        "JDBC",
                        " ",
                        " jdbc",
                        "jdbc ",
                        "jdbc sls",
                        "1jdbc",
                        "jdbc.sls")) {
            assertThatThrownBy(() -> fromConfs("audit.reporters=" + invalid))
                    .as("invalid reporter list %s", invalid)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("audit.reporters")
                    .hasMessageNotContaining(invalid);
        }
    }

    @Test
    void rejectsInvalidRunIdWithoutEchoingValue() {
        String invalid = "run-id-value-that-must-not-be-echoed";

        assertThatThrownBy(() -> fromConfs("audit.run-id=" + invalid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("audit.run-id")
                .hasMessageNotContaining(invalid);
    }

    @Test
    void requiredAcceptsOnlyLowercaseBooleanLiterals() {
        assertThat(
                        fromConfs("audit.reporters=jdbc", "audit.reporter.jdbc.required=false")
                                .auditReporterSpec()
                                .reporters()
                                .get(0)
                                .required())
                .isFalse();

        for (String invalid : Arrays.asList("TRUE", "False", "", " true", "yes")) {
            assertThatThrownBy(
                            () ->
                                    fromConfs(
                                            "audit.reporters=jdbc",
                                            "audit.reporter.jdbc.required=" + invalid))
                    .as("invalid required value %s", invalid)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("audit.reporter.jdbc.required");
        }
    }

    @Test
    void rejectsUnknownAndMisScopedAuditKeys() {
        assertRejectedAuditKey("audit.enabled", "audit.enabled=true");
        assertRejectedAuditKey(
                "audit.reporter.sls.endpoint",
                "audit.reporters=jdbc",
                "audit.reporter.sls.endpoint=endpoint-value-that-must-not-be-echoed");
        assertRejectedAuditKey(
                "audit.reporter.jdbc",
                "audit.reporters=jdbc",
                "audit.reporter.jdbc=value-that-must-not-be-echoed");
        assertRejectedAuditKey(
                "audit.reporter.jdbc.",
                "audit.reporters=jdbc",
                "audit.reporter.jdbc.=value-that-must-not-be-echoed");
    }

    @Test
    void passesDirectReporterOptionsThroughAsOpaqueStrings() {
        String sensitiveValue = "value-that-must-not-be-echoed";

        ReporterSpec reporter =
                fromConfs(
                                "audit.reporters=sls",
                                "audit.reporter.sls.access-key-id=plain-access-key-id",
                                "audit.reporter.sls.access-key-secret=" + sensitiveValue,
                                "audit.reporter.sls.security-token=plain-security-token",
                                "audit.reporter.sls.db.password=plain-password",
                                "audit.reporter.sls.credential=plain-credential",
                                "audit.reporter.sls.password_file=plain-underscore-option")
                        .auditReporterSpec()
                        .reporters()
                        .get(0);

        assertThat(reporter.options())
                .containsExactly(
                        entry("access-key-id", "plain-access-key-id"),
                        entry("access-key-secret", sensitiveValue),
                        entry("security-token", "plain-security-token"),
                        entry("db.password", "plain-password"),
                        entry("credential", "plain-credential"),
                        entry("password_file", "plain-underscore-option"));
    }

    @Test
    void fileOptionsRequireNonEmptyAbsolutePaths() {
        ReporterSpec reporter =
                fromConfs(
                                "audit.reporters=jdbc",
                                "audit.reporter.jdbc.password-file=/var/run/secrets/password",
                                "audit.reporter.jdbc.config-file=/etc/fluss/audit.properties")
                        .auditReporterSpec()
                        .reporters()
                        .get(0);
        assertThat(reporter.options())
                .containsExactly(
                        entry("password-file", "/var/run/secrets/password"),
                        entry("config-file", "/etc/fluss/audit.properties"));

        String key = "audit.reporter.jdbc.password-file";
        for (String invalid : Arrays.asList("", "relative/path", " /absolute/path")) {
            assertRejectedAuditKey(key, "audit.reporters=jdbc", key + "=" + invalid);
        }
    }

    @Test
    void duplicateConfKeysMustHaveIdenticalValues() {
        OrphanCleanConfig config = fromConfs("client.mode=stable", "client.mode=stable");
        assertThat(config.extraConfigs()).containsExactly(entry("client.mode", "stable"));

        String first = "first-value-that-must-not-be-echoed";
        String second = "second-value-that-must-not-be-echoed";
        assertThatThrownBy(() -> fromConfs("client.mode=" + first, "client.mode=" + second))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Duplicate --conf key: client.mode")
                .hasMessageNotContaining(first)
                .hasMessageNotContaining(second);
    }

    private static void assertRejectedAuditKey(String key, String... configs) {
        assertThatThrownBy(() -> fromConfs(configs))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(key);
    }

    private static OrphanCleanConfig fromConfs(String... configs) {
        List<String> args =
                new ArrayList<>(Arrays.asList("--bootstrap-server", "h:9123", "--all-databases"));
        for (String config : configs) {
            Collections.addAll(args, "--conf", config);
        }
        return OrphanCleanConfig.fromParams(
                MultipleParameterToolAdapter.fromArgs(args.toArray(new String[0])));
    }
}
