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

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.KvSnapshotFileMetadata;
import org.apache.fluss.metadata.KvSnapshotFileMetadataJsonSerde;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.rocksdb.RocksIteratorWrapper;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.rpc.messages.AllocateBulkLoadBucketSnapshotResponse;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.concurrent.FutureUtils;

import io.github.fluss_contrib.rocksdb.Checkpoint;
import io.github.fluss_contrib.rocksdb.CompressionType;
import io.github.fluss_contrib.rocksdb.FlushOptions;
import io.github.fluss_contrib.rocksdb.Options;
import io.github.fluss_contrib.rocksdb.RocksDB;
import io.github.fluss_contrib.rocksdb.RocksDBException;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Builds and commits one bucket's initial KV snapshot. Each row must use the full table schema,
 * including partition columns. Later rows with the same primary key replace earlier rows.
 *
 * <p>A bucket with no input still needs to be committed. Across all processes, callers must
 * coordinate a single writer at a time for a bucket of a given BulkLoad transaction.
 */
@PublicEvolving
@NotThreadSafe
public final class BulkLoadBucketWriter implements AutoCloseable {

    private static final String METADATA_FILE_NAME = "_METADATA";

    private final BulkLoadBuildContext.Target target;
    private final TableBucket bucket;
    private final long snapshotId;
    private final long logOffset;
    private final FsPath snapshotDirectory;
    private final Path localWorkDir;
    private final Options rocksDbOptions;
    private final RocksDB db;
    private final KeyEncoder keyEncoder;
    private final RowEncoder rowEncoder;
    private final InternalRow.FieldGetter[] fieldGetters;
    private final short schemaId;
    private final boolean countRows;
    private final Supplier<CompletableFuture<Void>> register;

    private boolean commitStarted;
    private boolean uploaded;
    private boolean closed;
    private CompletableFuture<Void> registration;

    BulkLoadBucketWriter(
            BulkLoadBuildContext.Target target,
            int bucketId,
            AllocateBulkLoadBucketSnapshotResponse allocation,
            Path localRoot,
            Supplier<CompletableFuture<Void>> register)
            throws IOException {
        this.target = target;
        this.bucket = tableBucket(target, bucketId);
        this.snapshotId = allocation.getSnapshotId();
        this.logOffset = allocation.getLogOffset();
        this.snapshotDirectory = snapshotDirectory(target, bucket, snapshotId);
        this.register = register;

        TableInfo table = target.getTableInfo();
        TableConfig tableConfig = table.getTableConfig();
        checkArgument(
                !KvValueLayout.fromTableConfig(tableConfig).hasValueTag()
                        && !tableConfig.getKvTTL().isPresent(),
                "BulkLoad bucket writers do not support tagged KV values or row TTL.");
        this.schemaId = (short) table.getSchemaId();
        this.countRows = tableConfig.getChangelogImage() == ChangelogImage.FULL;
        RowType rowType = table.getRowType();
        this.keyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        rowType,
                        table.getPhysicalPrimaryKeys(),
                        tableConfig,
                        table.isDefaultBucketKey());
        this.fieldGetters = InternalRow.createFieldGetters(rowType);

        RocksDB.loadLibrary();
        this.localWorkDir = Files.createTempDirectory(localRoot, "fluss-bulkload-");
        RowEncoder encoder = null;
        Options options = null;
        try {
            encoder = RowEncoder.create(tableConfig.getKvFormat(), rowType);
            options = new Options();
            options.setCreateIfMissing(true).setCompressionType(CompressionType.LZ4_COMPRESSION);
            this.db = RocksDB.open(options, localWorkDir.resolve("db").toString());
            this.rowEncoder = encoder;
            this.rocksDbOptions = options;
        } catch (RocksDBException | RuntimeException e) {
            IOUtils.closeQuietly(options);
            IOUtils.closeQuietly(encoder);
            FileUtils.deleteDirectoryQuietly(localWorkDir.toFile());
            throw new IOException("Failed to open local RocksDB for " + bucket + ".", e);
        }
    }

    /** Adds a row to this bucket, replacing any previous row with the same primary key. */
    public void add(InternalRow row) {
        checkState(!closed, "BulkLoad bucket writer is closed.");
        checkState(!commitStarted, "BulkLoad bucket commit has already started.");
        checkNotNull(row, "BulkLoad input row must not be null.");
        checkArgument(
                row.getFieldCount() == fieldGetters.length,
                "BulkLoad input row has %s fields but the table schema has %s.",
                row.getFieldCount(),
                fieldGetters.length);
        int rowBucketId = target.bucketOf(row);
        checkArgument(
                rowBucketId == bucket.getBucket(),
                "BulkLoad input row belongs to bucket %s, but this writer owns bucket %s.",
                rowBucketId,
                bucket.getBucket());

        byte[] key = keyEncoder.encodeKey(row);
        rowEncoder.startNewRow();
        for (int field = 0; field < fieldGetters.length; field++) {
            rowEncoder.encodeField(field, fieldGetters[field].getFieldOrNull(row));
        }
        byte[] value = ValueEncoder.encodeValue(schemaId, rowEncoder.finishRow());
        try {
            db.put(key, value);
        } catch (RocksDBException e) {
            throw new IllegalStateException("Failed to add row to " + bucket + ".", e);
        }
    }

    /**
     * Uploads the snapshot on the calling thread and returns the asynchronous registration result.
     * Once an upload succeeds, subsequent calls retry only a failed registration.
     */
    public CompletableFuture<Void> commit() {
        checkState(!closed, "BulkLoad bucket writer is closed.");
        if (!uploaded) {
            checkState(
                    !commitStarted,
                    "Close the failed BulkLoad writer before recovering its bucket.");
            commitStarted = true;
            try {
                uploadSnapshot();
                uploaded = true;
            } catch (IOException | RocksDBException | RuntimeException e) {
                return FutureUtils.completedExceptionally(e);
            }
        }

        if (registration == null || registration.isCompletedExceptionally()) {
            try {
                registration = register.get();
            } catch (RuntimeException e) {
                return FutureUtils.completedExceptionally(e);
            }
        }
        return registration;
    }

    /** Releases the writer's local resources. Uploaded snapshot files remain available. */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            db.close();
        } finally {
            IOUtils.closeQuietly(rocksDbOptions);
            IOUtils.closeQuietly(rowEncoder);
            FileUtils.deleteDirectoryQuietly(localWorkDir.toFile());
        }
    }

    static boolean markerExists(BulkLoadBuildContext.Target target, int bucketId, long snapshotId)
            throws IOException {
        FsPath path =
                metadataPath(snapshotDirectory(target, tableBucket(target, bucketId), snapshotId));
        return path.getFileSystem().exists(path);
    }

    private void uploadSnapshot() throws IOException, RocksDBException {
        try (FlushOptions options = new FlushOptions().setWaitForFlush(true)) {
            db.flush(options);
        }

        Path checkpointPath = localWorkDir.resolve("checkpoint");
        try (Checkpoint checkpoint = Checkpoint.create(db)) {
            checkpoint.createCheckpoint(checkpointPath.toString());
        }

        Long rowCount = countRows ? countKeys() : null;
        List<KvSnapshotFileMetadata.FileHandle> sharedFiles = new ArrayList<>();
        List<KvSnapshotFileMetadata.FileHandle> privateFiles = new ArrayList<>();
        FsPath sharedDirectory = FlussPaths.remoteKvSharedDir(snapshotDirectory.getParent());
        FileSystem fs = snapshotDirectory.getFileSystem();
        String attempt = UUID.randomUUID().toString();
        long totalSize = 0L;
        try (DirectoryStream<Path> files = Files.newDirectoryStream(checkpointPath)) {
            for (Path file : files) {
                String localName = file.getFileName().toString();
                boolean shared = localName.endsWith(".sst");
                FsPath remoteFile =
                        new FsPath(
                                shared ? sharedDirectory : snapshotDirectory,
                                attempt + "-" + localName);
                long fileSize;
                try (InputStream in = Files.newInputStream(file);
                        FSDataOutputStream out =
                                fs.create(remoteFile, FileSystem.WriteMode.NO_OVERWRITE)) {
                    fileSize = IOUtils.copyBytes(in, out, false);
                }
                (shared ? sharedFiles : privateFiles)
                        .add(
                                new KvSnapshotFileMetadata.FileHandle(
                                        remoteFile.toString(), fileSize, localName));
                totalSize = Math.addExact(totalSize, fileSize);
            }
        }

        KvSnapshotFileMetadata metadata =
                new KvSnapshotFileMetadata(
                        bucket,
                        snapshotId,
                        snapshotDirectory.toString(),
                        sharedFiles,
                        privateFiles,
                        totalSize,
                        logOffset,
                        rowCount,
                        null);
        byte[] bytes = KvSnapshotFileMetadataJsonSerde.toJson(metadata);
        try (FSDataOutputStream out =
                fs.create(metadataPath(snapshotDirectory), FileSystem.WriteMode.NO_OVERWRITE)) {
            out.write(bytes);
        }
    }

    private long countKeys() {
        long count = 0L;
        try (RocksIteratorWrapper iterator = new RocksIteratorWrapper(db.newIterator())) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                count++;
            }
        }
        return count;
    }

    private static TableBucket tableBucket(BulkLoadBuildContext.Target target, int bucketId) {
        return new TableBucket(
                target.getTableInfo().getTableId(), target.getPartitionId(), bucketId);
    }

    private static FsPath snapshotDirectory(
            BulkLoadBuildContext.Target target, TableBucket bucket, long snapshotId) {
        FsPath tabletDirectory =
                FlussPaths.remoteKvTabletDir(
                        FlussPaths.remoteKvDir(target.getRemoteDataDir()),
                        target.getPath(),
                        bucket);
        return FlussPaths.remoteKvSnapshotDir(tabletDirectory, snapshotId);
    }

    private static FsPath metadataPath(FsPath directory) {
        return new FsPath(directory, METADATA_FILE_NAME);
    }
}
