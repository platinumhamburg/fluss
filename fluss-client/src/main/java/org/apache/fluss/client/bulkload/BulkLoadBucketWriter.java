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
import org.apache.fluss.client.bulkload.BulkLoadBuildContext.Target;
import org.apache.fluss.client.bulkload.file.BulkLoadKvSnapshotWriter;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.rocksdb.RocksIteratorWrapper;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.IOUtils;

import io.github.fluss_contrib.rocksdb.BlockBasedTableConfig;
import io.github.fluss_contrib.rocksdb.Checkpoint;
import io.github.fluss_contrib.rocksdb.CompressionType;
import io.github.fluss_contrib.rocksdb.FlushOptions;
import io.github.fluss_contrib.rocksdb.Options;
import io.github.fluss_contrib.rocksdb.RocksDB;
import io.github.fluss_contrib.rocksdb.RocksDBException;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Builds one bucket's final KV state and writes an ordinary Snapshot.
 *
 * <p>Rows are deduplicated by primary key in a local RocksDB instance, where each later {@link
 * #add} replaces the earlier row with the same key. {@link #finish()} writes the Snapshot at the
 * final deduplicated row count, while {@link #finishAtLogEndOffset(long)} writes it at a
 * caller-provided log end offset. The caller owns cleanup of uploaded files that Commit has not
 * adopted.
 *
 * <p>One writer owns exactly one bucket and one local RocksDB instance. Callers must route rows to
 * the writer's {@code bucketId}, call {@link #finish()} or {@link #finishAtLogEndOffset(long)}
 * exactly once, and pass the returned {@link BulkLoadBucketFiles} to {@link BulkLoadClient#commit}.
 * Closing the writer releases its local state but does not delete any already-published remote
 * standard files.
 *
 * <p>Writer creation resolves its configured local root directory and atomically creates a unique
 * attempt child below it. The writer owns and deletes only that child. Callers must not run
 * concurrent live attempts for the same BulkLoad handle and bucket because their deterministic
 * final remote object identities are shared. Every new attempt must replay the complete input for
 * its bucket; a writer never resumes or adopts local state from an earlier attempt.
 */
@PublicEvolving
public final class BulkLoadBucketWriter implements AutoCloseable {

    private static final String DB_DIR_NAME = "db";
    private static final String CHECKPOINT_DIR_NAME = "checkpoint";
    private static final String STAGING_DIR_NAME = "staging";

    private final Target targetInfo;
    private final BulkLoadBuildSession buildSession;
    private final int bucketId;
    private final File localWorkDir;
    private final Options rocksDbOptions;
    private final RocksDB db;
    private final KeyEncoder primaryKeyEncoder;
    private final RowEncoder rowEncoder;
    private final InternalRow.FieldGetter[] fieldGetters;
    private final short schemaId;
    private final boolean fullImage;

    private boolean finished;
    private boolean closed;

    /**
     * Opens the local RocksDB state for one bucket.
     *
     * @param buildSession the session owning this writer
     * @param context the frozen transaction build context
     * @param bucketId the bucket owned by this writer
     */
    BulkLoadBucketWriter(BulkLoadBuildSession buildSession, Target context, int bucketId) {
        this.buildSession = checkNotNull(buildSession, "BulkLoad build session must not be null.");
        this.targetInfo = checkNotNull(context, "BulkLoad target must not be null.");
        TableInfo tableInfo = targetInfo.getTableInfo();
        checkArgument(
                tableInfo.hasPrimaryKey(), "BulkLoad input building requires a primary key table.");
        checkArgument(
                bucketId >= 0 && bucketId < tableInfo.getNumBuckets(),
                "BulkLoad bucket id %s is out of range [0, %s).",
                bucketId,
                tableInfo.getNumBuckets());
        this.bucketId = bucketId;
        this.schemaId = (short) tableInfo.getSchemaId();
        this.fullImage = tableInfo.getTableConfig().getChangelogImage() == ChangelogImage.FULL;

        RowType rowType = tableInfo.getRowType();
        this.primaryKeyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        rowType,
                        tableInfo.getPhysicalPrimaryKeys(),
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey());
        this.rowEncoder = RowEncoder.create(tableInfo.getTableConfig().getKvFormat(), rowType);
        this.fieldGetters = new InternalRow.FieldGetter[rowType.getFieldCount()];
        for (int field = 0; field < fieldGetters.length; field++) {
            fieldGetters[field] = InternalRow.createFieldGetter(rowType.getTypeAt(field), field);
        }
        this.localWorkDir =
                createAttemptDirectory(buildSession.localRootDir(bucketId), context, bucketId);

        Options options = null;
        try {
            Path dbDir = this.localWorkDir.toPath().resolve(DB_DIR_NAME);
            Files.createDirectories(dbDir);
            RocksDB.loadLibrary();
            options =
                    new Options()
                            .setCreateIfMissing(true)
                            .setCompressionType(CompressionType.LZ4_COMPRESSION)
                            .setTableFormatConfig(new BlockBasedTableConfig());
            this.db = RocksDB.open(options, dbDir.toString());
            this.rocksDbOptions = options;
        } catch (IOException | RocksDBException e) {
            try {
                IOUtils.closeAll(
                        rowEncoder, options, () -> FileUtils.deleteDirectory(localWorkDir));
            } catch (Exception cleanupFailure) {
                e.addSuppressed(cleanupFailure);
            }
            throw new IllegalStateException(
                    "Failed to open local RocksDB for BulkLoad bucket " + bucketId + ".", e);
        }
    }

    /** Adds one row, replacing any earlier row with the same primary key. */
    public void add(InternalRow row) {
        checkState(!closed, "BulkLoad bucket input builder is already closed.");
        checkState(!finished, "BulkLoad bucket input builder is already finished.");
        checkNotNull(row, "BulkLoad input row must not be null.");
        int rowBucketId = targetInfo.bucketOf(row);
        checkArgument(
                rowBucketId == bucketId,
                "BulkLoad row belongs to bucket %s, but this writer owns bucket %s.",
                rowBucketId,
                bucketId);
        byte[] key = primaryKeyEncoder.encodeKey(row);
        byte[] value = ValueEncoder.encodeValue(schemaId, encodeValue(row));
        try {
            db.put(key, value);
        } catch (RocksDBException e) {
            throw new IllegalStateException(
                    "Failed to write row to local RocksDB for BulkLoad bucket " + bucketId + ".",
                    e);
        }
    }

    /** Writes the final Snapshot at the final primary-key-deduplicated row count. */
    public BulkLoadBucketFiles finish() throws Exception {
        return finishSnapshotOnly(null);
    }

    /** Writes only the final standard Snapshot at the caller-provided log end offset. */
    public BulkLoadBucketFiles finishAtLogEndOffset(long logEndOffset) throws Exception {
        checkArgument(logEndOffset >= 0L, "BulkLoad log end offset must be non-negative.");
        return finishSnapshotOnly(logEndOffset);
    }

    private BulkLoadBucketFiles finishSnapshotOnly(@Nullable Long logEndOffset) throws Exception {
        checkState(!closed, "BulkLoad bucket input builder is already closed.");
        checkState(!finished, "BulkLoad bucket input builder is already finished.");
        Path checkpointDirectory = finishCheckpoint();
        Long rowCount = fullImage || logEndOffset == null ? countFinalState() : null;
        String snapshotMetadata =
                new BulkLoadKvSnapshotWriter(
                                targetInfo,
                                bucketId,
                                localWorkDir.toPath().resolve(STAGING_DIR_NAME))
                        .write(
                                checkpointDirectory,
                                logEndOffset == null ? rowCount : logEndOffset,
                                fullImage ? rowCount : null);
        return new BulkLoadBucketFiles(
                targetInfo.getHandle().getBulkLoadId(),
                targetInfo.getPartitionId(),
                bucketId,
                snapshotMetadata);
    }

    private Path finishCheckpoint() throws Exception {
        finished = true;

        try (FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
            db.flush(flushOptions);
        }
        Path checkpointDirectory = localWorkDir.toPath().resolve(CHECKPOINT_DIR_NAME);
        try (Checkpoint checkpoint = Checkpoint.create(db)) {
            checkpoint.createCheckpoint(checkpointDirectory.toString());
        }
        return checkpointDirectory;
    }

    /** Idempotently releases local RocksDB resources and the writer-owned attempt child. */
    @Override
    public void close() throws Exception {
        if (!closed) {
            closed = true;
            IOUtils.closeAll(
                    rowEncoder, db, rocksDbOptions, () -> FileUtils.deleteDirectory(localWorkDir));
        } else {
            FileUtils.deleteDirectory(localWorkDir);
        }
        buildSession.release(this);
    }

    private static File createAttemptDirectory(File localWorkParent, Target context, int bucketId) {
        Path parent = localWorkParent.toPath();
        try {
            Files.createDirectories(parent);
            return Files.createTempDirectory(
                            parent,
                            "fluss-bulkload-"
                                    + context.getHandle().getBulkLoadId()
                                    + "-bucket-"
                                    + bucketId
                                    + "-")
                    .toFile();
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to create a local attempt directory for BulkLoad bucket "
                            + bucketId
                            + ".",
                    e);
        }
    }

    private long countFinalState() {
        long rowCount = 0L;
        try (RocksIteratorWrapper iterator = new RocksIteratorWrapper(db.newIterator())) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                rowCount = Math.addExact(rowCount, 1L);
                iterator.next();
            }
        }
        return rowCount;
    }

    private BinaryRow encodeValue(InternalRow row) {
        rowEncoder.startNewRow();
        for (int field = 0; field < fieldGetters.length; field++) {
            rowEncoder.encodeField(field, fieldGetters[field].getFieldOrNull(row));
        }
        return rowEncoder.finishRow().copy();
    }
}
