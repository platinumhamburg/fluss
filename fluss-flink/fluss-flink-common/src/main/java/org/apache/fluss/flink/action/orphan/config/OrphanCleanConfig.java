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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.flink.action.orphan.audit.AuditReporterSpec;
import org.apache.fluss.flink.action.orphan.audit.AuditReporterSpec.ReporterSpec;
import org.apache.fluss.flink.adapter.MultipleParameterToolAdapter;
import org.apache.fluss.utils.StringUtils;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

/** Parsed command-line options for the orphan files cleanup action. */
@Internal
public final class OrphanCleanConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Minimum gap between any user-supplied cutoff and {@code now}. A cutoff closer to {@code now}
     * would risk classifying files that are mid-write (committed file written, snapshot/manifest
     * not yet visible to {@code ListRemoteLogManifests} / {@code ListKvSnapshots}) as orphan and
     * deleting them.
     */
    private static final Duration HARD_LOWER_BOUND = Duration.ofDays(1);

    /** Default file-level cutoff: files written before {@code now - 3d} are deletion-eligible. */
    private static final Duration DEFAULT_OLDER_THAN = Duration.ofDays(3);

    private static final long DEFAULT_REMOTE_FS_OP_RATE_LIMIT_PER_SECOND = 100L;

    private static final String AUDIT_RUN_ID = "audit.run-id";
    private static final String AUDIT_CLUSTER_ID = "audit.cluster-id";
    private static final String AUDIT_REPORTERS = "audit.reporters";
    private static final String AUDIT_REPORTER_PREFIX = "audit.reporter.";
    private static final Pattern CLUSTER_ID_PATTERN =
            Pattern.compile("[A-Za-z0-9][A-Za-z0-9._:-]{0,127}");
    private static final Pattern REPORTER_IDENTIFIER_PATTERN = Pattern.compile("[a-z][a-z0-9_-]*");

    private final String bootstrapServer;
    private final boolean allDatabases;
    private final @Nullable String database;
    private final @Nullable String table;
    private final long olderThanMillis;
    private final boolean olderThanConfigured;
    private final boolean dryRun;
    private final long remoteFsOpRateLimitPerSecond;
    private final @Nullable Integer parallelism;
    private final boolean allowDeleteManifest;
    private final boolean allowCleanOrphanTables;
    private final boolean allowCleanOrphanPartitions;
    private final AuditReporterSpec auditReporterSpec;
    private final Map<String, String> extraConfigs;

    private OrphanCleanConfig(
            String bootstrapServer,
            boolean allDatabases,
            @Nullable String database,
            @Nullable String table,
            long olderThanMillis,
            boolean olderThanConfigured,
            boolean dryRun,
            long remoteFsOpRateLimitPerSecond,
            @Nullable Integer parallelism,
            boolean allowDeleteManifest,
            boolean allowCleanOrphanTables,
            boolean allowCleanOrphanPartitions,
            AuditReporterSpec auditReporterSpec,
            Map<String, String> extraConfigs) {
        this.bootstrapServer = bootstrapServer;
        this.allDatabases = allDatabases;
        this.database = database;
        this.table = table;
        this.olderThanMillis = olderThanMillis;
        this.olderThanConfigured = olderThanConfigured;
        this.dryRun = dryRun;
        this.remoteFsOpRateLimitPerSecond = remoteFsOpRateLimitPerSecond;
        this.parallelism = parallelism;
        this.allowDeleteManifest = allowDeleteManifest;
        this.allowCleanOrphanTables = allowCleanOrphanTables;
        this.allowCleanOrphanPartitions = allowCleanOrphanPartitions;
        this.auditReporterSpec = auditReporterSpec;
        this.extraConfigs = Collections.unmodifiableMap(new LinkedHashMap<>(extraConfigs));
    }

    /** Parses a cleanup config from CLI parameters. */
    public static OrphanCleanConfig fromParams(MultipleParameterToolAdapter params) {
        String bootstrapServer = params.get("bootstrap-server");
        if (StringUtils.isNullOrWhitespaceOnly(bootstrapServer)) {
            throw new IllegalArgumentException("--bootstrap-server is required");
        }

        boolean allDatabases = params.has("all-databases");
        String database = params.get("database");
        if (allDatabases && !StringUtils.isNullOrWhitespaceOnly(database)) {
            throw new IllegalArgumentException(
                    "--database and --all-databases are mutually exclusive");
        }
        if (!allDatabases && StringUtils.isNullOrWhitespaceOnly(database)) {
            throw new IllegalArgumentException(
                    "Either --database or --all-databases must be provided");
        }
        if (allDatabases && !StringUtils.isNullOrWhitespaceOnly(params.get("table"))) {
            throw new IllegalArgumentException(
                    "--table requires --database and cannot be used with --all-databases");
        }

        long now = System.currentTimeMillis();
        String olderThan = params.get("older-than");
        long olderThanMillis = parseCutoff("--older-than", olderThan, now, DEFAULT_OLDER_THAN);
        long remoteFsOpRateLimitPerSecond =
                parsePositiveRateLimit(
                        "--remote-fs-op-rate-limit-per-second",
                        params.get("remote-fs-op-rate-limit-per-second"),
                        DEFAULT_REMOTE_FS_OP_RATE_LIMIT_PER_SECOND);
        Integer parallelism = parseParallelism(params.get("parallelism"));
        boolean allowDeleteManifest = params.has("allow-delete-manifest");
        boolean allowCleanOrphanTables = params.has("allow-clean-orphan-tables");
        boolean allowCleanOrphanPartitions = params.has("allow-clean-orphan-partitions");
        LinkedHashMap<String, String> allConfigs = parseConfigs(params.getMultiParameter("conf"));
        AuditReporterSpec auditReporterSpec = parseAuditReporterSpec(allConfigs);

        return new OrphanCleanConfig(
                bootstrapServer,
                allDatabases,
                database,
                params.get("table"),
                olderThanMillis,
                !StringUtils.isNullOrWhitespaceOnly(olderThan),
                params.has("dry-run"),
                remoteFsOpRateLimitPerSecond,
                parallelism,
                allowDeleteManifest,
                allowCleanOrphanTables,
                allowCleanOrphanPartitions,
                auditReporterSpec,
                extractExtraConfigs(allConfigs));
    }

    /**
     * Parses a CLI cutoff value into an absolute epoch-ms timestamp. Empty input falls back to
     * {@code now - defaultGap}. Explicit input must be ISO-8601 with an explicit offset (e.g.
     * {@code 2024-01-01T00:00:00+08:00} or {@code 2024-01-01T00:00:00Z}) and must be at least
     * {@link #HARD_LOWER_BOUND} earlier than {@code now} — closer-to-now cutoffs would race with
     * active writes (see {@code HARD_LOWER_BOUND} javadoc).
     */
    private static long parseCutoff(
            String flag, @Nullable String value, long now, Duration defaultGap) {
        if (StringUtils.isNullOrWhitespaceOnly(value)) {
            return now - defaultGap.toMillis();
        }
        OffsetDateTime parsed;
        try {
            parsed = OffsetDateTime.parse(value);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(
                    flag
                            + " must be an ISO-8601 timestamp with an explicit offset (e.g."
                            + " '2024-01-01T00:00:00+08:00' or '2024-01-01T00:00:00Z'); got: "
                            + value,
                    e);
        }
        long parsedMillis = parsed.toInstant().toEpochMilli();
        long maxAllowed = now - HARD_LOWER_BOUND.toMillis();
        if (parsedMillis > maxAllowed) {
            throw new IllegalArgumentException(
                    flag
                            + " must be at least 1d before now (got "
                            + Instant.ofEpochMilli(parsedMillis)
                            + ", now is "
                            + Instant.ofEpochMilli(now)
                            + "); a closer cutoff would race with mid-write files");
        }
        return parsedMillis;
    }

    private static long parsePositiveRateLimit(
            String flag, @Nullable String value, long defaultValue) {
        if (StringUtils.isNullOrWhitespaceOnly(value)) {
            return defaultValue;
        }
        long rate = Long.parseLong(value);
        if (rate <= 0) {
            throw new IllegalArgumentException(flag + " must be positive");
        }
        return rate;
    }

    @Nullable
    private static Integer parseParallelism(@Nullable String value) {
        if (StringUtils.isNullOrWhitespaceOnly(value)) {
            return null;
        }
        int p = Integer.parseInt(value);
        if (p <= 0) {
            throw new IllegalArgumentException("--parallelism must be positive");
        }
        return p;
    }

    private static LinkedHashMap<String, String> parseConfigs(@Nullable Collection<String> values) {
        LinkedHashMap<String, String> configs = new LinkedHashMap<>();
        if (values == null || values.isEmpty()) {
            return configs;
        }
        for (String kv : values) {
            int eqIdx = kv == null ? -1 : kv.indexOf('=');
            if (eqIdx <= 0) {
                throw new IllegalArgumentException("--conf must be in key=value format");
            }
            String key = kv.substring(0, eqIdx);
            String value = kv.substring(eqIdx + 1);
            if (configs.containsKey(key)) {
                if (!configs.get(key).equals(value)) {
                    throw new IllegalArgumentException("Duplicate --conf key: " + key);
                }
                continue;
            }
            configs.put(key, value);
        }
        return configs;
    }

    private static AuditReporterSpec parseAuditReporterSpec(LinkedHashMap<String, String> configs) {
        String runId = parseRunId(configs.get(AUDIT_RUN_ID));
        String clusterId = parseClusterId(configs.get(AUDIT_CLUSTER_ID));
        List<String> identifiers = parseReporterIdentifiers(configs.get(AUDIT_REPORTERS));
        LinkedHashMap<String, ReporterOptions> optionsByIdentifier = new LinkedHashMap<>();
        for (String identifier : identifiers) {
            optionsByIdentifier.put(identifier, new ReporterOptions());
        }

        for (Map.Entry<String, String> entry : configs.entrySet()) {
            String key = entry.getKey();
            if (!key.startsWith("audit.")
                    || AUDIT_RUN_ID.equals(key)
                    || AUDIT_CLUSTER_ID.equals(key)
                    || AUDIT_REPORTERS.equals(key)) {
                continue;
            }
            if (!key.startsWith(AUDIT_REPORTER_PREFIX)) {
                throw auditConfigError(key);
            }

            String reporterOption = key.substring(AUDIT_REPORTER_PREFIX.length());
            int optionSeparator = reporterOption.indexOf('.');
            if (optionSeparator <= 0 || optionSeparator == reporterOption.length() - 1) {
                throw auditConfigError(key);
            }
            String identifier = reporterOption.substring(0, optionSeparator);
            String option = reporterOption.substring(optionSeparator + 1);
            ReporterOptions reporterOptions = optionsByIdentifier.get(identifier);
            if (reporterOptions == null) {
                throw auditConfigError(key);
            }

            if ("required".equals(option)) {
                reporterOptions.required = parseRequired(key, entry.getValue());
            } else {
                validateReporterOption(key, option, entry.getValue());
                reporterOptions.options.put(option, entry.getValue());
            }
        }

        List<ReporterSpec> reporterSpecs = new ArrayList<>();
        for (Map.Entry<String, ReporterOptions> entry : optionsByIdentifier.entrySet()) {
            ReporterOptions options = entry.getValue();
            reporterSpecs.add(new ReporterSpec(entry.getKey(), options.required, options.options));
        }
        return new AuditReporterSpec(runId, clusterId, reporterSpecs);
    }

    private static String parseRunId(@Nullable String configuredRunId) {
        if (configuredRunId == null) {
            return UUID.randomUUID().toString();
        }
        UUID parsed;
        try {
            parsed = UUID.fromString(configuredRunId);
        } catch (IllegalArgumentException e) {
            throw auditConfigError(AUDIT_RUN_ID);
        }
        if (!parsed.toString().equalsIgnoreCase(configuredRunId)) {
            throw auditConfigError(AUDIT_RUN_ID);
        }
        return configuredRunId;
    }

    @Nullable
    private static String parseClusterId(@Nullable String configuredClusterId) {
        if (configuredClusterId == null) {
            return null;
        }
        if (!CLUSTER_ID_PATTERN.matcher(configuredClusterId).matches()) {
            throw auditConfigError(AUDIT_CLUSTER_ID);
        }
        return configuredClusterId;
    }

    private static List<String> parseReporterIdentifiers(@Nullable String configuredReporters) {
        if (configuredReporters == null || configuredReporters.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> identifiers = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (String identifier : configuredReporters.split(",", -1)) {
            if ("log".equals(identifier)
                    || !REPORTER_IDENTIFIER_PATTERN.matcher(identifier).matches()
                    || !seen.add(identifier)) {
                throw auditConfigError(AUDIT_REPORTERS);
            }
            identifiers.add(identifier);
        }
        return identifiers;
    }

    private static boolean parseRequired(String key, String value) {
        if ("true".equals(value)) {
            return true;
        }
        if ("false".equals(value)) {
            return false;
        }
        throw auditConfigError(key);
    }

    private static void validateReporterOption(String key, String option, String value) {
        String lowercaseOption = option.toLowerCase(Locale.ROOT);
        boolean fileOption = lowercaseOption.endsWith("-file");
        if (fileOption) {
            try {
                if (value.isEmpty() || !Paths.get(value).isAbsolute()) {
                    throw auditConfigError(key);
                }
            } catch (InvalidPathException e) {
                throw auditConfigError(key);
            }
        }
    }

    private static Map<String, String> extractExtraConfigs(LinkedHashMap<String, String> configs) {
        LinkedHashMap<String, String> extraConfigs = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            if (!entry.getKey().startsWith("audit.")) {
                extraConfigs.put(entry.getKey(), entry.getValue());
            }
        }
        return extraConfigs;
    }

    private static IllegalArgumentException auditConfigError(String key) {
        return new IllegalArgumentException(key);
    }

    private static final class ReporterOptions {
        private boolean required = true;
        private final LinkedHashMap<String, String> options = new LinkedHashMap<>();
    }

    /** Returns the bootstrap server list used to connect to Fluss. */
    public String bootstrapServer() {
        return bootstrapServer;
    }

    /** Returns whether the cleanup targets all databases. */
    public boolean allDatabases() {
        return allDatabases;
    }

    /** Returns the single targeted database when the action is not scoped to all databases. */
    public Optional<String> database() {
        return Optional.ofNullable(database);
    }

    /** Returns the optional targeted table name. */
    public Optional<String> table() {
        return Optional.ofNullable(table);
    }

    /**
     * Returns the file-level cutoff as an absolute epoch-millis timestamp, frozen at action
     * startup. A candidate file is deletion-eligible iff its mtime is strictly less than this
     * value. The cutoff does not slide during the run — long scans cannot accidentally pull in
     * files written after startup.
     */
    public long olderThanMillis() {
        return olderThanMillis;
    }

    /** Returns whether the cutoff came from an explicit {@code --older-than} argument. */
    public boolean olderThanConfigured() {
        return olderThanConfigured;
    }

    /** Returns whether the action runs in dry-run mode. */
    public boolean dryRun() {
        return dryRun;
    }

    /**
     * Returns the best-effort job-level target rate for remote filesystem operations per second.
     *
     * <p>The budget is shared by remote filesystem metadata reads, manifest reads, and deletes.
     * Scan subtasks split this value by operator parallelism because Flink does not provide a
     * cross-JVM limiter for this action.
     */
    public long remoteFsOpRateLimitPerSecond() {
        return remoteFsOpRateLimitPerSecond;
    }

    /** Returns the optional parallelism for the ScanAndClean stage. */
    public Optional<Integer> parallelism() {
        return Optional.ofNullable(parallelism);
    }

    /**
     * Opt-in to delete {@code .manifest} files. Default {@code false}: mis-deleting an active
     * manifest leaves the coordinator's manifest pointer dangling and breaks the bucket's metadata
     * chain — the failure mode is catastrophic and asymmetric vs the trivial space cost of keeping
     * orphan manifests (KB-sized files), so deletion is gated behind an explicit operator flag.
     */
    public boolean allowDeleteManifest() {
        return allowDeleteManifest;
    }

    /**
     * Opt-in to recursively clean files inside an orphan-table directory. Default {@code false}:
     * the action only audits the detected orphan dir and leaves its contents untouched, because an
     * id-based misclassification of a freshly-created table as orphan would otherwise be
     * unrecoverable. Operators flip this on once they have reviewed the audit log.
     */
    public boolean allowCleanOrphanTables() {
        return allowCleanOrphanTables;
    }

    /**
     * Opt-in to recursively clean files inside an orphan-partition directory. Same default-audit
     * rationale as {@link #allowCleanOrphanTables()}.
     */
    public boolean allowCleanOrphanPartitions() {
        return allowCleanOrphanPartitions;
    }

    /** Returns the immutable external audit reporter configuration for this cleanup run. */
    public AuditReporterSpec auditReporterSpec() {
        return auditReporterSpec;
    }

    /**
     * Returns extra configuration entries passed via {@code --conf key=value}. These are propagated
     * to {@link org.apache.fluss.fs.FileSystem#initialize} for remote filesystem authentication
     * (e.g. {@code fs.oss.accessKeyId}, {@code fs.oss.accessKeySecret}).
     */
    public Map<String, String> extraConfigs() {
        return extraConfigs;
    }
}
