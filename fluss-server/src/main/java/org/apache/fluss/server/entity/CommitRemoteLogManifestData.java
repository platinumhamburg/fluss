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

package org.apache.fluss.server.entity;

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.CommitRemoteLogManifestRequest;

import javax.annotation.Nullable;

import java.util.Objects;

/** The data for request {@link CommitRemoteLogManifestRequest}. */
public class CommitRemoteLogManifestData {

    /** The table bucket that this snapshot belongs to. */
    private final TableBucket tableBucket;

    /** The location where the remote log manifest is stored in remote storage. */
    private final FsPath remoteLogManifestPath;

    /** The start offset of the remote log. */
    private final long remoteLogStartOffset;

    /** The end offset of the remote log. */
    private final long remoteLogEndOffset;

    /** The highest exclusive offset successfully copied to remote storage. */
    private final long highestCopiedEndOffset;

    /** The coordinator epoch when the snapshot is triggered. */
    private final int coordinatorEpoch;

    /** The leader epoch of the bucket when the snapshot is triggered. */
    private final int bucketLeaderEpoch;

    /** Canonical target registration path frozen when tiering started. */
    private final @Nullable String sourceMetadataPath;

    /** Target registration version frozen when tiering started. */
    private final @Nullable Integer sourceMetadataVersion;

    public CommitRemoteLogManifestData(
            TableBucket tableBucket,
            FsPath remoteLogManifestPath,
            long remoteLogStartOffset,
            long remoteLogEndOffset,
            int coordinatorEpoch,
            int bucketLeaderEpoch) {
        this(
                tableBucket,
                remoteLogManifestPath,
                remoteLogStartOffset,
                remoteLogEndOffset,
                remoteLogEndOffset,
                coordinatorEpoch,
                bucketLeaderEpoch,
                null,
                null);
    }

    public CommitRemoteLogManifestData(
            TableBucket tableBucket,
            FsPath remoteLogManifestPath,
            long remoteLogStartOffset,
            long remoteLogEndOffset,
            int coordinatorEpoch,
            int bucketLeaderEpoch,
            @Nullable String sourceMetadataPath,
            @Nullable Integer sourceMetadataVersion) {
        this(
                tableBucket,
                remoteLogManifestPath,
                remoteLogStartOffset,
                remoteLogEndOffset,
                remoteLogEndOffset,
                coordinatorEpoch,
                bucketLeaderEpoch,
                sourceMetadataPath,
                sourceMetadataVersion);
    }

    public CommitRemoteLogManifestData(
            TableBucket tableBucket,
            FsPath remoteLogManifestPath,
            long remoteLogStartOffset,
            long remoteLogEndOffset,
            long highestCopiedEndOffset,
            int coordinatorEpoch,
            int bucketLeaderEpoch) {
        this(
                tableBucket,
                remoteLogManifestPath,
                remoteLogStartOffset,
                remoteLogEndOffset,
                highestCopiedEndOffset,
                coordinatorEpoch,
                bucketLeaderEpoch,
                null,
                null);
    }

    public CommitRemoteLogManifestData(
            TableBucket tableBucket,
            FsPath remoteLogManifestPath,
            long remoteLogStartOffset,
            long remoteLogEndOffset,
            long highestCopiedEndOffset,
            int coordinatorEpoch,
            int bucketLeaderEpoch,
            @Nullable String sourceMetadataPath,
            @Nullable Integer sourceMetadataVersion) {
        this.tableBucket = tableBucket;
        this.remoteLogManifestPath = remoteLogManifestPath;
        this.remoteLogStartOffset = remoteLogStartOffset;
        this.remoteLogEndOffset = remoteLogEndOffset;
        this.highestCopiedEndOffset = highestCopiedEndOffset;
        this.coordinatorEpoch = coordinatorEpoch;
        this.bucketLeaderEpoch = bucketLeaderEpoch;
        this.sourceMetadataPath = sourceMetadataPath;
        this.sourceMetadataVersion = sourceMetadataVersion;
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public FsPath getRemoteLogManifestPath() {
        return remoteLogManifestPath;
    }

    public long getRemoteLogStartOffset() {
        return remoteLogStartOffset;
    }

    public long getRemoteLogEndOffset() {
        return remoteLogEndOffset;
    }

    public long getHighestCopiedEndOffset() {
        return highestCopiedEndOffset;
    }

    public int getCoordinatorEpoch() {
        return coordinatorEpoch;
    }

    public int getBucketLeaderEpoch() {
        return bucketLeaderEpoch;
    }

    public @Nullable String getSourceMetadataPath() {
        return sourceMetadataPath;
    }

    public @Nullable Integer getSourceMetadataVersion() {
        return sourceMetadataVersion;
    }

    @Override
    public String toString() {
        return "CommitRemoteLogManifestData{"
                + "tableBucket="
                + tableBucket
                + ", metadataSnapshotPath="
                + remoteLogManifestPath
                + ", remoteLogEndOffset="
                + remoteLogEndOffset
                + ", highestCopiedEndOffset="
                + highestCopiedEndOffset
                + ", coordinatorEpoch="
                + coordinatorEpoch
                + ", bucketLeaderEpoch="
                + bucketLeaderEpoch
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CommitRemoteLogManifestData that = (CommitRemoteLogManifestData) o;
        return Objects.equals(tableBucket, that.tableBucket)
                && Objects.equals(remoteLogManifestPath, that.remoteLogManifestPath)
                && remoteLogEndOffset == that.remoteLogEndOffset
                && highestCopiedEndOffset == that.highestCopiedEndOffset
                && coordinatorEpoch == that.coordinatorEpoch
                && bucketLeaderEpoch == that.bucketLeaderEpoch
                && Objects.equals(sourceMetadataPath, that.sourceMetadataPath)
                && Objects.equals(sourceMetadataVersion, that.sourceMetadataVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                tableBucket,
                remoteLogManifestPath,
                remoteLogEndOffset,
                highestCopiedEndOffset,
                coordinatorEpoch,
                bucketLeaderEpoch,
                sourceMetadataPath,
                sourceMetadataVersion);
    }
}
