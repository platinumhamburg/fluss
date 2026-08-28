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

package org.apache.fluss.client.bulkload;

import org.apache.fluss.client.bulkload.file.BulkLoadFileHandle;
import org.apache.fluss.client.bulkload.file.BulkLoadManifestFileWriter;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.utils.FlussPaths;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Assembles the ordered outer manifest from one transaction's opaque bucket outputs. */
final class BulkLoadManifestWriter {

    private static final int MANIFEST_VERSION = 1;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private BulkLoadManifestWriter() {}

    static BulkLoadFileHandle write(
            BulkLoadBuildContext context, Collection<BulkLoadBucketFiles> bucketFiles)
            throws IOException {
        checkNotNull(context, "BulkLoad build context must not be null.");
        checkNotNull(bucketFiles, "BulkLoad bucket files must not be null.");
        BulkLoadHandle handle = context.getHandle();
        String remoteDataDir =
                checkNotNull(
                        context.getTableInfo().getRemoteDataDir(),
                        "BulkLoad target remote data directory must not be null.");
        FsPath manifestPath = FlussPaths.bulkLoadManifestPath(remoteDataDir, handle);
        int numBuckets = context.getTableInfo().getNumBuckets();
        checkArgument(
                bucketFiles.size() == numBuckets,
                "BulkLoad manifest buckets must exactly cover [0, %s), got %s bucket outputs.",
                numBuckets,
                bucketFiles.size());

        List<BulkLoadBucketFiles> sorted = new ArrayList<>(bucketFiles);
        sorted.sort(Comparator.comparingInt(BulkLoadBucketFiles::getBucketId));
        for (int expected = 0; expected < sorted.size(); expected++) {
            BulkLoadBucketFiles files = checkNotNull(sorted.get(expected));
            checkArgument(
                    files.getBucketId() == expected,
                    "BulkLoad manifest buckets must exactly cover [0, %s) without duplicates.",
                    numBuckets);
            checkArgument(
                    handle.getBulkLoadId().equals(files.getBulkLoadId()),
                    "BulkLoad files bulkLoadId %s does not match the transaction %s.",
                    files.getBulkLoadId(),
                    handle.getBulkLoadId());
        }

        List<Object> buckets = new ArrayList<>(sorted.size());
        for (BulkLoadBucketFiles files : sorted) {
            buckets.add(bucketJson(files));
        }

        Map<String, Object> root = new LinkedHashMap<>();
        root.put("version", MANIFEST_VERSION);
        root.put("bulk_load_id", handle.getBulkLoadId());
        root.put("buckets", buckets);
        return BulkLoadManifestFileWriter.write(manifestPath, MAPPER.writeValueAsBytes(root));
    }

    private static Map<String, Object> bucketJson(BulkLoadBucketFiles files) {
        Map<String, Object> bucket = new LinkedHashMap<>();
        bucket.put("bucket_id", files.getBucketId());
        bucket.put("snapshot_metadata", handleJson(files.getSnapshotMetadata()));
        return bucket;
    }

    private static Map<String, Object> handleJson(BulkLoadFileHandle handle) {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("path", handle.getPath());
        value.put("length", handle.getLength());
        value.put("sha256", handle.getSha256());
        return value;
    }
}
