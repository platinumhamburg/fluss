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

package org.apache.fluss.server.tablet.bulkload;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadDataState;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.UUID;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Complete BulkLoad target metadata applied by a TabletServer. */
@Internal
public final class BulkLoadTargetMetadata {

    private final String metadataPath;
    private final TableBucket tableBucket;
    private final int metadataVersion;
    private final BulkLoadDataState dataState;
    private final @Nullable String bulkLoadId;

    /** Creates an immutable applied target record. */
    public BulkLoadTargetMetadata(
            String metadataPath,
            TableBucket tableBucket,
            int metadataVersion,
            BulkLoadDataState dataState,
            @Nullable String bulkLoadId) {
        this.metadataPath = checkNotNull(metadataPath, "Metadata path must not be null.");
        this.tableBucket = checkNotNull(tableBucket, "Table bucket must not be null.");
        checkArgument(!metadataPath.isEmpty(), "Metadata path must not be empty.");
        checkArgument(metadataVersion >= 0, "Metadata version must be non-negative.");
        this.metadataVersion = metadataVersion;
        this.dataState = checkNotNull(dataState, "Data state must not be null.");
        if (dataState == BulkLoadDataState.ACTIVE) {
            checkArgument(bulkLoadId == null, "ACTIVE target must not have a BulkLoad ID.");
        } else {
            checkArgument(isCanonicalUuid(bulkLoadId), "LOADING target requires a canonical UUID.");
        }
        this.bulkLoadId = bulkLoadId;
    }

    public String getMetadataPath() {
        return metadataPath;
    }

    public int getMetadataVersion() {
        return metadataVersion;
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public BulkLoadDataState getDataState() {
        return dataState;
    }

    public @Nullable String getBulkLoadId() {
        return bulkLoadId;
    }

    public boolean isLoading() {
        return dataState == BulkLoadDataState.LOADING;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BulkLoadTargetMetadata)) {
            return false;
        }
        BulkLoadTargetMetadata that = (BulkLoadTargetMetadata) o;
        return metadataVersion == that.metadataVersion
                && metadataPath.equals(that.metadataPath)
                && tableBucket.equals(that.tableBucket)
                && dataState == that.dataState
                && Objects.equals(bulkLoadId, that.bulkLoadId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metadataPath, tableBucket, metadataVersion, dataState, bulkLoadId);
    }

    private static boolean isCanonicalUuid(@Nullable String value) {
        if (value == null) {
            return false;
        }
        try {
            return UUID.fromString(value).toString().equals(value);
        } catch (IllegalArgumentException ignored) {
            return false;
        }
    }
}
