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

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.client.bulkload.file.BulkLoadFileHandle;
import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.BulkLoadTargetInfo;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.utils.IOUtils;

import javax.annotation.Nullable;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Builds standard client-owned BulkLoad bucketFiles for public integration tests. */
public final class BulkLoadTestDataBuilder {

    /** Builds final ordinary Snapshot files and publishes the outer manifest last. */
    public BuildResult build(
            BulkLoadTargetInfo targetInfo,
            List<Object[]> logicalRows,
            ChangelogImage changelogImage)
            throws Exception {
        checkNotNull(targetInfo, "BulkLoad target info must not be null.");
        checkNotNull(logicalRows, "BulkLoad logical rows must not be null.");
        checkNotNull(changelogImage, "BulkLoad changelog image must not be null.");

        TableInfo tableInfo = targetInfo.getTableInfo();
        checkArgument(tableInfo.hasPrimaryKey(), "BulkLoad test input requires a primary key.");
        checkArgument(
                tableInfo.getTableConfig().getChangelogImage() == changelogImage,
                "BulkLoad test image differs from the frozen target image.");
        List<Object[]> expectedRows =
                immutableRows(logicalRows, tableInfo.getRowType().getFieldCount());
        List<List<Object[]>> rowsByBucket = distribute(tableInfo, expectedRows);
        BulkLoadBuildContext context = new BulkLoadBuildContext(targetInfo);
        List<BulkLoadBucketFiles> bucketFiles = new ArrayList<>(rowsByBucket.size());
        List<BucketData> buckets = new ArrayList<>(rowsByBucket.size());
        for (int bucketId = 0; bucketId < rowsByBucket.size(); bucketId++) {
            Path workDirectory = Files.createTempDirectory("fluss-bulkload-client-input-");
            List<Object[]> bucketRows = rowsByBucket.get(bucketId);
            try (BulkLoadBucketWriter builder =
                    new BulkLoadBucketWriter(context, bucketId, workDirectory.toFile())) {
                for (Object[] values : bucketRows) {
                    builder.add(DataTestUtils.row(tableInfo.getRowType(), values));
                }
                bucketFiles.add(builder.finish());
            }
            buckets.add(
                    new BucketData(
                            bucketId,
                            changelogImage == ChangelogImage.FULL
                                    ? Long.valueOf(bucketRows.size())
                                    : null,
                            bucketRows));
        }

        BulkLoadFileHandle manifest = BulkLoadManifestWriter.write(context, bucketFiles);
        byte[] manifestBytes = readExact(new FsPath(manifest.getPath()), manifest.getLength());
        return new BuildResult(
                new FsPath(manifest.getPath()),
                manifestBytes,
                manifest.getSha256(),
                buckets,
                expectedRows);
    }

    private static List<List<Object[]>> distribute(TableInfo tableInfo, List<Object[]> rows) {
        List<List<Object[]>> rowsByBucket = new ArrayList<>(tableInfo.getNumBuckets());
        for (int bucket = 0; bucket < tableInfo.getNumBuckets(); bucket++) {
            rowsByBucket.add(new ArrayList<>());
        }
        KeyEncoder primaryKeyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        tableInfo.getRowType(),
                        tableInfo.getPhysicalPrimaryKeys(),
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey());
        KeyEncoder bucketKeyEncoder =
                KeyEncoder.ofBucketKeyEncoder(
                        tableInfo.getRowType(),
                        tableInfo.getBucketKeys(),
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey(),
                        primaryKeyEncoder);
        BucketingFunction bucketingFunction =
                BucketingFunction.of(tableInfo.getTableConfig().getDataLakeFormat().orElse(null));
        for (Object[] values : rows) {
            InternalRow row = DataTestUtils.row(tableInfo.getRowType(), values);
            int bucket =
                    bucketingFunction.bucketing(
                            bucketKeyEncoder.encodeKey(row), tableInfo.getNumBuckets());
            rowsByBucket.get(bucket).add(copyRow(values));
        }
        return rowsByBucket;
    }

    private static byte[] readExact(FsPath path, long length) throws Exception {
        checkArgument(
                length >= 0 && length <= Integer.MAX_VALUE, "Invalid manifest length %s.", length);
        byte[] bytes = new byte[(int) length];
        try (FSDataInputStream input = path.getFileSystem().open(path)) {
            IOUtils.readFully(input, bytes);
            checkArgument(
                    input.read() == -1, "BulkLoad manifest length changed after publication.");
        }
        return bytes;
    }

    private static List<Object[]> immutableRows(List<Object[]> rows, int arity) {
        List<Object[]> copy = new ArrayList<>(rows.size());
        for (Object[] row : rows) {
            checkNotNull(row, "BulkLoad row must not be null.");
            checkArgument(
                    row.length == arity, "BulkLoad row arity differs from the target schema.");
            copy.add(copyRow(row));
        }
        return Collections.unmodifiableList(copy);
    }

    private static List<Object[]> copyRows(List<Object[]> rows) {
        List<Object[]> copy = new ArrayList<>(rows.size());
        for (Object[] row : rows) {
            copy.add(copyRow(row));
        }
        return Collections.unmodifiableList(copy);
    }

    private static Object[] copyRow(Object[] row) {
        return Arrays.copyOf(row, row.length);
    }

    /** Complete generated input and independently copied expected rows. */
    public static final class BuildResult implements AutoCloseable {
        private final FsPath manifestPath;
        private final byte[] manifestBytes;
        private final String manifestSha256;
        private final List<BucketData> buckets;
        private final List<Object[]> expectedKvRows;

        private BuildResult(
                FsPath manifestPath,
                byte[] manifestBytes,
                String manifestSha256,
                List<BucketData> buckets,
                List<Object[]> expectedKvRows) {
            this.manifestPath = manifestPath;
            this.manifestBytes = Arrays.copyOf(manifestBytes, manifestBytes.length);
            this.manifestSha256 = manifestSha256;
            this.buckets = Collections.unmodifiableList(new ArrayList<>(buckets));
            this.expectedKvRows = copyRows(expectedKvRows);
        }

        /** Returns the immutable outer manifest path. */
        public FsPath getManifestPath() {
            return manifestPath;
        }

        /** Returns the outer manifest length. */
        public long getManifestLength() {
            return manifestBytes.length;
        }

        /** Returns the outer manifest SHA-256. */
        public String getManifestSha256() {
            return manifestSha256;
        }

        /** Returns all buckets in ascending order. */
        public List<BucketData> getBuckets() {
            return buckets;
        }

        /** Returns one bucket by its planned ID. */
        public BucketData getBucket(int bucketId) {
            return buckets.get(bucketId);
        }

        /** Returns the independently copied logical KV result. */
        public List<Object[]> getExpectedKvRows() {
            return copyRows(expectedKvRows);
        }

        /** No local files remains after each client writer is closed. */
        @Override
        public void close() {}
    }

    /** Expected logical state for one bucket. */
    public static final class BucketData {
        private final int bucketId;
        private final @Nullable Long rowCount;
        private final List<Object[]> expectedLogicalRows;

        private BucketData(
                int bucketId, @Nullable Long rowCount, List<Object[]> expectedLogicalRows) {
            this.bucketId = bucketId;
            this.rowCount = rowCount;
            this.expectedLogicalRows = copyRows(expectedLogicalRows);
        }

        /** Returns the bucket ID. */
        public int getBucketId() {
            return bucketId;
        }

        /** Returns FULL row count, or null for WAL. */
        @Nullable
        public Long getRowCount() {
            return rowCount;
        }

        /** Returns the exclusive final Remote Log offset. */
        public long getLogEndOffset() {
            return expectedLogicalRows.size();
        }

        /** Returns independently copied logical INSERT expectations. */
        public List<Object[]> getExpectedLogicalRows() {
            return copyRows(expectedLogicalRows);
        }
    }
}
