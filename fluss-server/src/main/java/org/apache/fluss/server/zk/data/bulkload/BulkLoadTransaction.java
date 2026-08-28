/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.fluss.server.zk.data.bulkload;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.BulkLoadAbortReason;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.BulkLoadState;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.regex.Pattern;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Immutable version-1 BulkLoad transaction facts. ZooKeeper Stat.version is its state version. */
@Internal
public final class BulkLoadTransaction {

    private static final int MAX_ABORT_MESSAGE_BYTES = 4096;
    private static final Pattern SHA256_PATTERN = Pattern.compile("[0-9a-f]{64}");

    private final BulkLoadHandle handle;
    private final BulkLoadState state;
    private final String callerToken;
    private final String creatorName;
    private final String creatorType;
    private final String remoteDataDir;
    private final int schemaId;
    private final String metadataPath;
    private final int metadataVersion;
    private final @Nullable long[] snapshotIds;
    private final long createdTimeMs;
    private final long updatedTimeMs;
    private final long buildDeadlineMs;
    private final @Nullable Long commitDecisionDeadlineMs;
    private final @Nullable Long resultExpireTimeMs;
    private final @Nullable String manifestPath;
    private final @Nullable Long manifestLength;
    private final @Nullable String manifestSha256;
    private final @Nullable BulkLoadAbortReason abortReason;
    private final @Nullable String abortMessage;

    /** Creates and validates the complete transaction fact set. */
    public BulkLoadTransaction(
            BulkLoadHandle handle,
            BulkLoadState state,
            String callerToken,
            String creatorName,
            String creatorType,
            String remoteDataDir,
            int schemaId,
            String metadataPath,
            int metadataVersion,
            @Nullable long[] snapshotIds,
            long createdTimeMs,
            long updatedTimeMs,
            long buildDeadlineMs,
            @Nullable Long commitDecisionDeadlineMs,
            @Nullable Long resultExpireTimeMs,
            @Nullable String manifestPath,
            @Nullable Long manifestLength,
            @Nullable String manifestSha256,
            @Nullable BulkLoadAbortReason abortReason,
            @Nullable String abortMessage) {
        this.handle = checkNotNull(handle, "BulkLoad handle must not be null.");
        this.state = checkNotNull(state, "BulkLoad state must not be null.");
        this.callerToken = checkNotNull(callerToken, "BulkLoad caller token must not be null.");
        checkArgument(!callerToken.trim().isEmpty(), "BulkLoad caller token must not be empty.");
        this.creatorName = checkNotNull(creatorName, "BulkLoad creator name must not be null.");
        this.creatorType = checkNotNull(creatorType, "BulkLoad creator type must not be null.");
        this.remoteDataDir =
                checkNotNull(remoteDataDir, "BulkLoad remote data directory must not be null.");
        checkArgument(schemaId >= 0, "BulkLoad schema ID must be non-negative.");
        this.metadataPath = checkNotNull(metadataPath, "BulkLoad metadata path must not be null.");
        checkArgument(metadataVersion >= 0, "BulkLoad metadata version must be non-negative.");
        checkArgument(
                snapshotIds == null || snapshotIds.length > 0,
                "BulkLoad snapshot IDs must not be empty.");
        if (snapshotIds != null) {
            for (long snapshotId : snapshotIds) {
                checkArgument(snapshotId >= 0, "BulkLoad snapshot ID must be non-negative.");
            }
        }
        checkArgument(createdTimeMs >= 0, "BulkLoad created time must be non-negative.");
        checkArgument(updatedTimeMs >= 0, "BulkLoad updated time must be non-negative.");
        checkArgument(buildDeadlineMs >= 0, "BulkLoad build deadline must be non-negative.");
        checkArgument(
                commitDecisionDeadlineMs == null || commitDecisionDeadlineMs >= 0,
                "BulkLoad commit decision deadline must be non-negative.");
        checkArgument(
                resultExpireTimeMs == null || resultExpireTimeMs >= 0,
                "BulkLoad result expiry must be non-negative.");
        checkArgument(
                manifestLength == null || manifestLength > 0,
                "BulkLoad manifest length must be positive.");

        boolean manifestPresent = manifestPath != null;
        checkArgument(
                manifestPresent == (manifestLength != null)
                        && manifestPresent == (manifestSha256 != null)
                        && manifestPresent == (commitDecisionDeadlineMs != null),
                "BulkLoad manifest path, length, SHA-256, and commit decision deadline must form one group.");
        checkArgument(
                manifestPath == null || !manifestPath.isEmpty(),
                "BulkLoad manifest path must not be empty.");
        if (manifestSha256 != null) {
            checkArgument(
                    SHA256_PATTERN.matcher(manifestSha256).matches(),
                    "BulkLoad SHA-256 must be exactly 64 lowercase hexadecimal characters.");
        }
        if (state == BulkLoadState.COMMITTING || state == BulkLoadState.COMMITTED) {
            checkArgument(manifestPresent, state + " requires the frozen manifest group.");
            checkArgument(snapshotIds != null, state + " requires snapshot IDs.");
        }
        checkArgument(
                !manifestPresent || snapshotIds != null,
                "A frozen BulkLoad manifest requires snapshot IDs.");

        boolean terminal = state == BulkLoadState.COMMITTED || state == BulkLoadState.ABORTED;
        checkArgument(
                terminal == (resultExpireTimeMs != null),
                "BulkLoad result expiry must exist exactly in terminal states.");
        checkArgument(
                state != BulkLoadState.ABORTED || abortReason != null,
                "ABORTED BulkLoad requires an abort reason.");
        checkArgument(
                abortReason == null
                        || state == BulkLoadState.BEGUN
                        || state == BulkLoadState.ABORTED,
                "BulkLoad abort reason is only valid while aborting or ABORTED.");
        checkArgument(
                abortMessage == null || abortReason != null,
                "BulkLoad abort message requires an abort reason.");
        this.abortReason = abortReason;

        this.schemaId = schemaId;
        this.metadataVersion = metadataVersion;
        this.snapshotIds = snapshotIds == null ? null : snapshotIds.clone();
        this.createdTimeMs = createdTimeMs;
        this.updatedTimeMs = updatedTimeMs;
        this.buildDeadlineMs = buildDeadlineMs;
        this.commitDecisionDeadlineMs = commitDecisionDeadlineMs;
        this.resultExpireTimeMs = resultExpireTimeMs;
        this.manifestPath = manifestPath;
        this.manifestLength = manifestLength;
        this.manifestSha256 = manifestSha256;
        this.abortMessage = truncateUtf8(abortMessage);
    }

    public BulkLoadHandle getHandle() {
        return handle;
    }

    public String getBulkLoadId() {
        return handle.getBulkLoadId();
    }

    public BulkLoadState getState() {
        return state;
    }

    public String getCallerToken() {
        return callerToken;
    }

    public String getDatabaseName() {
        return handle.getTarget().getDatabaseName();
    }

    public String getTableName() {
        return handle.getTarget().getTableName();
    }

    @Nullable
    public String getPartitionName() {
        return handle.getTarget().getPartitionName();
    }

    public long getTableId() {
        return handle.getTableId();
    }

    @Nullable
    public Long getPartitionId() {
        return handle.getPartitionId();
    }

    public String getCreatorName() {
        return creatorName;
    }

    public String getCreatorType() {
        return creatorType;
    }

    public String getRemoteDataDir() {
        return remoteDataDir;
    }

    public int getSchemaId() {
        return schemaId;
    }

    public String getMetadataPath() {
        return metadataPath;
    }

    public int getMetadataVersion() {
        return metadataVersion;
    }

    public boolean isFenceReady() {
        return snapshotIds != null;
    }

    @Nullable
    public long[] getSnapshotIds() {
        return snapshotIds == null ? null : snapshotIds.clone();
    }

    public long getCreatedTimeMs() {
        return createdTimeMs;
    }

    public long getUpdatedTimeMs() {
        return updatedTimeMs;
    }

    public long getBuildDeadlineMs() {
        return buildDeadlineMs;
    }

    @Nullable
    public Long getCommitDecisionDeadlineMs() {
        return commitDecisionDeadlineMs;
    }

    @Nullable
    public Long getResultExpireTimeMs() {
        return resultExpireTimeMs;
    }

    @Nullable
    public String getManifestPath() {
        return manifestPath;
    }

    @Nullable
    public Long getManifestLength() {
        return manifestLength;
    }

    @Nullable
    public String getManifestSha256() {
        return manifestSha256;
    }

    @Nullable
    public BulkLoadAbortReason getAbortReason() {
        return abortReason;
    }

    @Nullable
    public String getAbortMessage() {
        return abortMessage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BulkLoadTransaction that = (BulkLoadTransaction) o;
        return schemaId == that.schemaId
                && metadataVersion == that.metadataVersion
                && Arrays.equals(snapshotIds, that.snapshotIds)
                && createdTimeMs == that.createdTimeMs
                && updatedTimeMs == that.updatedTimeMs
                && buildDeadlineMs == that.buildDeadlineMs
                && Objects.equals(handle, that.handle)
                && state == that.state
                && Objects.equals(callerToken, that.callerToken)
                && Objects.equals(creatorName, that.creatorName)
                && Objects.equals(creatorType, that.creatorType)
                && Objects.equals(remoteDataDir, that.remoteDataDir)
                && Objects.equals(metadataPath, that.metadataPath)
                && Objects.equals(commitDecisionDeadlineMs, that.commitDecisionDeadlineMs)
                && Objects.equals(resultExpireTimeMs, that.resultExpireTimeMs)
                && Objects.equals(manifestPath, that.manifestPath)
                && Objects.equals(manifestLength, that.manifestLength)
                && Objects.equals(manifestSha256, that.manifestSha256)
                && abortReason == that.abortReason
                && Objects.equals(abortMessage, that.abortMessage);
    }

    @Override
    public int hashCode() {
        int result =
                Objects.hash(
                        handle,
                        state,
                        callerToken,
                        creatorName,
                        creatorType,
                        remoteDataDir,
                        schemaId,
                        metadataPath,
                        metadataVersion,
                        createdTimeMs,
                        updatedTimeMs,
                        buildDeadlineMs,
                        commitDecisionDeadlineMs,
                        resultExpireTimeMs,
                        manifestPath,
                        manifestLength,
                        manifestSha256,
                        abortReason,
                        abortMessage);
        return 31 * result + Arrays.hashCode(snapshotIds);
    }

    @Override
    public String toString() {
        return "BulkLoadTransaction{" + "handle=" + handle + ", state=" + state + '}';
    }

    @Nullable
    private static String truncateUtf8(@Nullable String value) {
        if (value == null
                || value.getBytes(StandardCharsets.UTF_8).length <= MAX_ABORT_MESSAGE_BYTES) {
            return value;
        }
        StringBuilder result = new StringBuilder();
        int used = 0;
        for (int offset = 0; offset < value.length(); ) {
            int codePoint = value.codePointAt(offset);
            int bytes =
                    new String(Character.toChars(codePoint))
                            .getBytes(StandardCharsets.UTF_8)
                            .length;
            if (used + bytes > MAX_ABORT_MESSAGE_BYTES) {
                break;
            }
            result.appendCodePoint(codePoint);
            used += bytes;
            offset += Character.charCount(codePoint);
        }
        return result.toString();
    }
}
