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

package org.apache.fluss.flink.sink.writer.undo;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.ListOffsetsResult;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.writer.Upsert;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.rpc.protocol.AggMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Coordinates undo recovery operations during Flink sink writer initialization.
 *
 * <p>This coordinator ensures exactly-once semantics by reversing any writes that occurred after
 * the last successful checkpoint but before a failure. The recovery process involves:
 *
 * <ol>
 *   <li>Reading changelog records from checkpoint offset to current latest offset
 *   <li>Computing inverse operations for each affected primary key
 *   <li>Applying undo operations using OVERWRITE mode to restore bucket state
 * </ol>
 *
 * <p><b>Undo Logic:</b> For each primary key, only the first change after checkpoint determines the
 * undo action:
 *
 * <ul>
 *   <li>{@code INSERT} → Delete the row (it didn't exist at checkpoint)
 *   <li>{@code UPDATE_BEFORE} → Restore the old value (this was the state at checkpoint)
 *   <li>{@code UPDATE_AFTER} → Ignored (UPDATE_BEFORE already handled the undo)
 *   <li>{@code DELETE} → Re-insert the deleted row (it existed at checkpoint)
 * </ul>
 *
 * <p>This class delegates to specialized components:
 *
 * <ul>
 *   <li>{@link UndoComputer} - Undo operation computation using {@link KeyEncoder} for primary key
 *       encoding
 *   <li>{@link UndoRecoveryExecutor} - Changelog reading and write execution
 * </ul>
 *
 * @see AggMode#OVERWRITE
 */
public class UndoRecoveryCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(UndoRecoveryCoordinator.class);

    private final Table table;
    private final Admin admin;
    private final TablePath tablePath;
    @Nullable private final int[] targetColumnIndexes;

    /**
     * Creates a new UndoRecoveryCoordinator.
     *
     * @param table the Fluss table to perform recovery on
     * @param admin the Admin client for offset queries
     * @param tablePath the table path
     * @param targetColumnIndexes optional target columns for partial update (null for full row)
     */
    public UndoRecoveryCoordinator(
            Table table, Admin admin, TablePath tablePath, @Nullable int[] targetColumnIndexes) {
        this.table = table;
        this.admin = admin;
        this.tablePath = tablePath;
        this.targetColumnIndexes = targetColumnIndexes;

        logPartialUpdateConfig(targetColumnIndexes);
    }

    private void logPartialUpdateConfig(@Nullable int[] targetColumnIndexes) {
        if (targetColumnIndexes != null) {
            LOG.debug(
                    "Undo recovery configured with partial update columns: {}",
                    Arrays.toString(targetColumnIndexes));
        }
    }

    // ==================== Public API ====================

    /**
     * Performs undo recovery for buckets with known offsets.
     *
     * @param bucketOffsets map of bucket to its recovery offset
     * @param subtaskIndex the Flink subtask index (for logging)
     * @param parallelism the total parallelism (for logging)
     * @throws Exception if recovery fails
     */
    public void performUndoRecovery(
            Map<TableBucket, Long> bucketOffsets, int subtaskIndex, int parallelism)
            throws Exception {

        if (bucketOffsets.isEmpty()) {
            LOG.info("No buckets to recover on subtask {}/{}", subtaskIndex, parallelism);
            return;
        }

        LOG.info(
                "Starting undo recovery for {} bucket(s) on subtask {}/{}",
                bucketOffsets.size(),
                subtaskIndex,
                parallelism);

        List<BucketRecoveryContext> contexts = buildRecoveryContexts(bucketOffsets);

        try (LogScanner scanner = createLogScanner()) {
            Upsert recoveryUpsert = table.newUpsert().aggregationMode(AggMode.OVERWRITE);
            if (targetColumnIndexes != null) {
                recoveryUpsert = recoveryUpsert.partialUpdate(targetColumnIndexes);
            }
            UpsertWriter writer = recoveryUpsert.createWriter();

            // Create UndoComputer with writer for streaming execution
            Schema schema = table.getTableInfo().getSchema();
            KeyEncoder keyEncoder =
                    KeyEncoder.of(
                            schema.getRowType(),
                            schema.getPrimaryKey().get().getColumnNames(),
                            null);
            UndoComputer undoComputer = new UndoComputer(keyEncoder, writer);

            UndoRecoveryExecutor executor = new UndoRecoveryExecutor(scanner, writer, undoComputer);
            executor.execute(contexts);
        }

        LOG.info("Completed undo recovery for {} bucket(s)", bucketOffsets.size());
    }

    // ==================== Scanner Factory ====================

    /**
     * Creates a LogScanner for reading changelog records.
     *
     * <p>This method is protected to allow test subclasses to inject custom scanners for fault
     * injection testing.
     *
     * @return a new LogScanner instance
     */
    protected LogScanner createLogScanner() {
        return table.newScan().createLogScanner();
    }

    // ==================== Recovery Context Building ====================

    private List<BucketRecoveryContext> buildRecoveryContexts(Map<TableBucket, Long> bucketOffsets)
            throws Exception {

        List<BucketRecoveryContext> contexts = new ArrayList<>();

        for (Map.Entry<TableBucket, Long> entry : bucketOffsets.entrySet()) {
            TableBucket bucket = entry.getKey();
            long checkpointOffset = entry.getValue();
            long targetOffset = getLatestOffset(bucket);

            BucketRecoveryContext ctx = new BucketRecoveryContext(bucket, checkpointOffset);
            ctx.setTargetOffset(targetOffset);
            contexts.add(ctx);
        }

        return contexts;
    }

    // ==================== Offset Management ====================

    long getLatestOffset(TableBucket bucket) throws Exception {
        List<Integer> bucketIds = Collections.singletonList(bucket.getBucket());

        ListOffsetsResult result =
                bucket.getPartitionId() != null
                        ? admin.listOffsets(
                                tablePath,
                                getPartitionName(bucket.getPartitionId()),
                                bucketIds,
                                new OffsetSpec.LatestSpec())
                        : admin.listOffsets(tablePath, bucketIds, new OffsetSpec.LatestSpec());

        Long offset = result.bucketResult(bucket.getBucket()).get();
        if (offset == null) {
            LOG.warn("Got null offset for bucket {}, defaulting to 0", bucket);
            return 0L;
        }
        return offset;
    }

    private String getPartitionName(long partitionId) throws Exception {
        List<PartitionInfo> partitions = admin.listPartitionInfos(tablePath).get();
        return partitions.stream()
                .filter(p -> p.getPartitionId() == partitionId)
                .findFirst()
                .map(PartitionInfo::getPartitionName)
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "Cannot find partition name for partition ID: "
                                                + partitionId));
    }
}
