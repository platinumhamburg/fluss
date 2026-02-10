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

package org.apache.fluss.flink.sink.undo;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.adapter.RuntimeContextAdapter;
import org.apache.fluss.flink.sink.state.WriterState;
import org.apache.fluss.flink.sink.state.WriterStateSerializer;
import org.apache.fluss.flink.sink.writer.RecoveryOffsetManager;
import org.apache.fluss.flink.sink.writer.undo.UndoRecoveryManager;
import org.apache.fluss.flink.sink.writer.undo.UndoRecoveryManager.UndoOffsets;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.MapUtils;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A Flink stream operator that manages undo recovery state using Union List State.
 *
 * <p>This operator ensures correct state redistribution during scale up/down scenarios by using
 * Union List State instead of the default List State. During recovery, each subtask receives a
 * complete copy of all states from all previous subtasks, then uses {@link RecoveryOffsetManager}
 * to determine the recovery strategy.
 *
 * <p>The operator performs the following functions:
 *
 * <ul>
 *   <li>Manages Union List State for bucket offsets
 *   <li>Uses {@link RecoveryOffsetManager} to determine recovery strategy (checkpoint or producer
 *       offsets)
 *   <li>Executes undo recovery during {@code initializeState()} using {@link UndoRecoveryManager}
 *   <li>Receives offset reports from downstream Writer via {@link OffsetReportContext}
 *   <li>Snapshots state during checkpoints
 *   <li>Cleans up producer offsets after first checkpoint (Task0 only)
 *   <li>Passes through input elements unchanged
 * </ul>
 *
 * <p><b>Recovery Strategy:</b> The operator uses {@link
 * RecoveryOffsetManager#determineRecoveryStrategy} to decide whether to use checkpoint state or
 * producer offsets for recovery. This handles both normal checkpoint recovery and pre-checkpoint
 * failure scenarios.
 *
 * @param <IN> The type of input elements
 * @see OffsetReportContext
 * @see RecoveryOffsetManager
 */
@Internal
public class UndoRecoveryOperator<IN> extends AbstractStreamOperator<IN>
        implements OneInputStreamOperator<IN, IN>, BoundedOneInput, OffsetReportContext {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(UndoRecoveryOperator.class);

    /** State descriptor name for the Union List State. */
    private static final String UNDO_RECOVERY_STATE_NAME = "undo_recovery_state";

    // ==================== Configuration Fields ====================

    /** The table path for the Fluss table. */
    private final TablePath tablePath;

    /** The Fluss configuration. */
    private final Configuration flussConfig;

    /** The row type of the table. */
    private final RowType tableRowType;

    /** Target column indexes for partial update (null for full row). */
    @Nullable private final int[] targetColumnIndexes;

    /** The number of buckets in the table. */
    private final int numBuckets;

    /** Whether the table is partitioned. */
    private final boolean isPartitioned;

    /**
     * The producer ID used for producer offset snapshot management.
     *
     * <p>This is used by {@link RecoveryOffsetManager} to register and retrieve producer offsets
     * for pre-checkpoint failure recovery. If null at construction time, it will be resolved to the
     * Flink job ID during initialization.
     */
    @Nullable private final String configuredProducerId;

    /**
     * The resolved producer ID (either configured or defaulted to Flink job ID).
     *
     * <p>This is set during {@link #initializeState} and used for all producer offset operations.
     */
    private transient String resolvedProducerId;

    /** The polling interval in milliseconds for producer offsets synchronization. */
    private final long producerOffsetsPollIntervalMs;

    // ==================== State Fields ====================

    /** Union List State for storing bucket offsets across checkpoints. */
    private transient ListState<WriterState> undoStateList;

    /**
     * Map from TableBucket to the latest written offset.
     *
     * <p>This map is updated by the downstream SinkWriter via {@link #reportOffset(TableBucket,
     * long)} and is used to create WriterState during checkpoint snapshotting.
     *
     * <p>Uses ConcurrentHashMap for thread-safe updates from async write callbacks.
     *
     * <p>Access to this map is protected by {@link #bucketOffsetsLock} to ensure consistent
     * snapshots during checkpointing.
     */
    private transient Map<TableBucket, Long> bucketOffsets;

    /**
     * Read-write lock for protecting access to {@link #bucketOffsets}.
     *
     * <p>This lock ensures thread-safe access between:
     *
     * <ul>
     *   <li>Write operations: {@link #reportOffset(TableBucket, long)} called from async write
     *       callbacks on multiple threads
     *   <li>Read operations: {@link #snapshotState(StateSnapshotContext)} called from the
     *       checkpoint thread to create a consistent snapshot
     * </ul>
     *
     * <p>Using a read-write lock allows multiple concurrent offset reports (write lock) while
     * ensuring exclusive access during snapshot creation (read lock for iteration).
     *
     * <p>Note: We use write lock for reportOffset and read lock for snapshotState because:
     *
     * <ul>
     *   <li>reportOffset modifies the map (needs write lock)
     *   <li>snapshotState only reads/iterates the map (needs read lock to prevent concurrent
     *       modifications during iteration)
     * </ul>
     */
    private transient ReadWriteLock bucketOffsetsLock;

    /** Flag indicating whether the producer offsets have been deleted after first checkpoint. */
    private transient boolean producerOffsetsDeleted;

    /**
     * Flag indicating whether this operator was restored from a checkpoint.
     *
     * <p>This is used to determine whether to delete producer offsets after the first checkpoint.
     * Producer offsets should only be deleted when we're recovering from a checkpoint, not when
     * starting fresh. This ensures that if the job fails before the first checkpoint, the producer
     * offsets are still available for recovery on the next restart.
     */
    private transient boolean restoredFromCheckpoint;

    /** The subtask index of this operator instance. */
    private transient int subtaskIndex;

    /** The total parallelism of this operator. */
    private transient int parallelism;

    // ==================== Fluss Connection Fields ====================

    /** Fluss connection, lazily initialized only when undo recovery is needed. */
    @Nullable private transient Connection connection;

    /** Fluss table, lazily initialized only when undo recovery is needed. */
    @Nullable private transient Table table;

    // ==================== Constructor ====================

    /**
     * Creates a new UndoRecoveryOperator with StreamOperatorParameters.
     *
     * <p>This constructor is used by {@link UndoRecoveryOperatorFactory} to create the operator
     * instance. It calls {@link #setup} internally to properly initialize the operator with Flink's
     * runtime context.
     *
     * @param parameters the stream operator parameters from Flink runtime
     * @param tablePath the table path for the Fluss table
     * @param flussConfig the Fluss configuration
     * @param tableRowType the row type of the table
     * @param targetColumnIndexes target column indexes for partial update (null for full row)
     * @param numBuckets the number of buckets in the table
     * @param isPartitioned whether the table is partitioned
     * @param producerId the producer ID for producer offset management (null to use Flink job ID)
     * @param producerOffsetsPollIntervalMs the polling interval for producer offsets
     */
    public UndoRecoveryOperator(
            StreamOperatorParameters<IN> parameters,
            TablePath tablePath,
            Configuration flussConfig,
            RowType tableRowType,
            @Nullable int[] targetColumnIndexes,
            int numBuckets,
            boolean isPartitioned,
            @Nullable String producerId,
            long producerOffsetsPollIntervalMs) {
        super();
        this.tablePath = tablePath;
        this.flussConfig = flussConfig;
        this.tableRowType = tableRowType;
        this.targetColumnIndexes = targetColumnIndexes;
        this.numBuckets = numBuckets;
        this.isPartitioned = isPartitioned;
        this.configuredProducerId =
                producerId; // May be null, will be resolved in initializeState()
        this.producerOffsetsPollIntervalMs = producerOffsetsPollIntervalMs;

        // Call setup internally - this is allowed because we're inside the operator class
        this.setup(
                parameters.getContainingTask(),
                parameters.getStreamConfig(),
                parameters.getOutput());
    }

    // ==================== State Initialization ====================

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        // Use RuntimeContextAdapter for Flink version compatibility
        // (getTaskInfo() was added in Flink 1.19, direct methods deprecated in Flink 2.x)
        StreamingRuntimeContext runtimeContext = (StreamingRuntimeContext) getRuntimeContext();
        parallelism = RuntimeContextAdapter.getNumberOfParallelSubtasks(runtimeContext);
        subtaskIndex = RuntimeContextAdapter.getIndexOfThisSubtask(runtimeContext);
        producerOffsetsDeleted = false;
        restoredFromCheckpoint = context.isRestored();

        // Resolve producerId: use configured value or default to Flink job ID
        resolvedProducerId = configuredProducerId;
        if (resolvedProducerId == null) {
            resolvedProducerId = getRuntimeContext().getJobId().toString();
            LOG.info("Using Flink job ID as producerId: {}", resolvedProducerId);
        }

        LOG.info(
                "Initializing UndoRecoveryOperator for table {} (subtask {}/{}, producerId={})",
                tablePath,
                subtaskIndex,
                parallelism,
                resolvedProducerId);

        // Step 1: Get Union List State
        // Union List State ensures each subtask receives a complete copy of all states during
        // recovery
        undoStateList =
                context.getOperatorStateStore()
                        .getUnionListState(
                                new ListStateDescriptor<>(
                                        UNDO_RECOVERY_STATE_NAME, new WriterStateSerializer()));

        // Step 2: Convert Union List State to Collection for RecoveryOffsetManager
        // Note: context.isRestored() == false means fresh start (no checkpoint exists)
        //       context.isRestored() == true means restored from checkpoint
        Collection<WriterState> recoveredState = null;
        if (context.isRestored()) {
            recoveredState = new ArrayList<>();
            for (WriterState state : undoStateList.get()) {
                recoveredState.add(state);
            }
            LOG.debug(
                    "Restored {} WriterState objects from Union List State for subtask {}",
                    recoveredState.size(),
                    subtaskIndex);
            // Log detailed state content for debugging recovery issues
            for (WriterState state : recoveredState) {
                LOG.info(
                        "Subtask {} restored WriterState: checkpointId={}, bucketOffsets={}",
                        subtaskIndex,
                        state.getCheckpointId(),
                        state.getBucketOffsets());
            }
        }

        // Step 3: Use RecoveryOffsetManager to determine recovery strategy
        // This handles both checkpoint recovery and producer offset recovery
        initializeFlussConnection();
        if (table == null) {
            table = connection.getTable(tablePath);
        }

        RecoveryOffsetManager offsetManager =
                new RecoveryOffsetManager(
                        connection.getAdmin(),
                        resolvedProducerId,
                        subtaskIndex,
                        parallelism,
                        producerOffsetsPollIntervalMs,
                        tablePath,
                        table.getTableInfo());

        RecoveryOffsetManager.RecoveryDecision decision =
                offsetManager.determineRecoveryStrategy(recoveredState);

        LOG.info("Recovery decision for subtask {}: {}", subtaskIndex, decision);

        // Step 4: Execute undo recovery if needed
        if (decision.needsUndoRecovery()) {
            Map<TableBucket, UndoOffsets> undoOffsets = decision.getUndoOffsets();
            LOG.info(
                    "Executing undo recovery for subtask {}: undoOffsets={}",
                    subtaskIndex,
                    undoOffsets);

            performUndoRecovery(undoOffsets);

            // Initialize bucket offsets with recovery offsets (checkpoint offsets)
            Map<TableBucket, Long> recoveryOffsets = decision.getRecoveryOffsets();
            LOG.info(
                    "Subtask {} initializing bucketOffsets from recovery: {}",
                    subtaskIndex,
                    recoveryOffsets);
            initializeBucketOffsets(recoveryOffsets);
        } else {
            LOG.info("No undo recovery needed for subtask {}", subtaskIndex);
            // Initialize empty bucket offsets
            initializeBucketOffsets(new HashMap<>());
        }

        LOG.info(
                "UndoRecoveryOperator initialized for subtask {} with {} bucket offsets",
                subtaskIndex,
                bucketOffsets.size());
    }

    /**
     * Initializes the Fluss connection lazily.
     *
     * <p>The connection is only created when needed (e.g., for fetching partition info or
     * performing undo recovery).
     */
    private void initializeFlussConnection() {
        if (connection == null) {
            connection = ConnectionFactory.createConnection(flussConfig);
            LOG.debug("Created Fluss connection for table {}", tablePath);
        }
    }

    // ==================== Undo Recovery Execution ====================

    /**
     * Performs undo recovery for the given bucket offsets.
     *
     * <p>This method reuses {@link UndoRecoveryManager} for the actual recovery logic. The recovery
     * process reads changelog records from the checkpoint offset to the log end offset and applies
     * inverse operations to restore the bucket state.
     *
     * @param undoOffsets the bucket undo offsets containing checkpoint offset and log end offset
     * @throws Exception if recovery fails
     */
    private void performUndoRecovery(Map<TableBucket, UndoOffsets> undoOffsets) throws Exception {
        LOG.info(
                "Performing undo recovery for {} buckets on subtask {}/{}",
                undoOffsets.size(),
                subtaskIndex,
                parallelism);

        // Reuse UndoRecoveryManager for actual recovery
        // UndoOffsets already contains both checkpointOffset and logEndOffset,
        // so no additional listOffset call is needed
        UndoRecoveryManager recoveryManager = new UndoRecoveryManager(table, targetColumnIndexes);
        recoveryManager.performUndoRecovery(undoOffsets, subtaskIndex, parallelism);

        LOG.info(
                "Completed undo recovery for {} buckets on subtask {}/{}",
                undoOffsets.size(),
                subtaskIndex,
                parallelism);
    }

    // ==================== State Snapshotting ====================

    /**
     * Snapshots the current state during checkpoint.
     *
     * <p>This method is called by Flink during checkpoint processing. It clears the existing state
     * list and adds a new {@link WriterState} with the current bucket offsets if the map is not
     * empty.
     *
     * <p>Note: Producer offset cleanup is NOT done here. It is done in {@link
     * #notifyCheckpointComplete(long)} to ensure the checkpoint is fully committed before deleting
     * the producer offsets. This prevents data loss in case of failure between snapshotState and
     * checkpoint completion.
     *
     * <p>Thread-safety: This method acquires a read lock on {@link #bucketOffsetsLock} to ensure a
     * consistent snapshot of bucket offsets while allowing concurrent offset reports to complete.
     *
     * @param context the state snapshot context containing checkpoint information
     * @throws Exception if snapshotting fails
     */
    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        // Clear existing state
        undoStateList.clear();

        // Add new state if bucket offsets is not empty
        // Use read lock to get a consistent snapshot of bucket offsets
        if (bucketOffsets != null && bucketOffsetsLock != null) {
            bucketOffsetsLock.readLock().lock();
            try {
                if (!bucketOffsets.isEmpty()) {
                    // Create a defensive copy while holding the lock
                    WriterState state =
                            new WriterState(
                                    context.getCheckpointId(), new HashMap<>(bucketOffsets));
                    undoStateList.add(state);
                    LOG.info(
                            "Subtask {} snapshot state at checkpoint {}: bucketOffsets={}",
                            subtaskIndex,
                            context.getCheckpointId(),
                            bucketOffsets);
                } else {
                    LOG.info(
                            "Subtask {} snapshot state at checkpoint {}: bucketOffsets is EMPTY",
                            subtaskIndex,
                            context.getCheckpointId());
                }
            } finally {
                bucketOffsetsLock.readLock().unlock();
            }
        }
    }

    /**
     * Called when a checkpoint is completed successfully.
     *
     * <p>This method triggers producer offset cleanup after the first successful checkpoint, but
     * ONLY when the operator was restored from a checkpoint. This is critical for the producer
     * offset recovery scenario:
     *
     * <ul>
     *   <li>If starting fresh (no checkpoint): Do NOT delete producer offsets. They are needed for
     *       recovery if the job fails before the next checkpoint.
     *   <li>If restored from checkpoint: Delete producer offsets after the first successful
     *       checkpoint. The checkpoint state now contains the recovery offsets, so producer offsets
     *       are no longer needed.
     * </ul>
     *
     * @param checkpointId the ID of the completed checkpoint
     * @throws Exception if cleanup fails
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);

        LOG.info(
                "Checkpoint {} completed for subtask {} (restoredFromCheckpoint={}, bucketOffsets={})",
                checkpointId,
                subtaskIndex,
                restoredFromCheckpoint,
                bucketOffsets);

        // Only delete producer offsets if we were restored from a checkpoint.
        // If starting fresh, keep producer offsets for potential recovery on failure.
        if (restoredFromCheckpoint && bucketOffsets != null && !bucketOffsets.isEmpty()) {
            deleteProducerOffsetsIfNeeded();
        }
    }

    /**
     * Deletes producer offsets after first checkpoint (Task0 only).
     *
     * <p>This cleanup is necessary to prevent stale producer offsets from being used in subsequent
     * recovery scenarios. Only Task0 performs the deletion to avoid concurrent cleanup attempts.
     */
    private void deleteProducerOffsetsIfNeeded() {
        if (producerOffsetsDeleted) {
            return;
        }
        producerOffsetsDeleted = true;

        // Only Task0 should delete the offsets
        if (subtaskIndex != 0) {
            return;
        }

        LOG.info("Task0 deleting producer offsets for producerId {}", resolvedProducerId);
        try {
            initializeFlussConnection();
            connection.getAdmin().deleteProducerOffsets(resolvedProducerId).get();
            LOG.info("Successfully deleted producer offsets for producerId {}", resolvedProducerId);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to delete producer offsets for {}: {}",
                    resolvedProducerId,
                    e.getMessage());
        }
    }

    // ==================== OneInputStreamOperator Methods ====================

    /**
     * Processes an input element by passing it through to the output unchanged.
     *
     * <p>This operator does not modify, filter, or buffer any input elements. All elements are
     * emitted to the output immediately in the same order they are received.
     *
     * @param element the input element to process
     * @throws Exception if processing fails
     */
    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        // Pass through unchanged
        output.collect(element);
    }

    // ==================== BoundedOneInput Methods ====================

    /**
     * Called when the input is exhausted (for bounded streams).
     *
     * <p>This is a no-op for the UndoRecoveryOperator as there is no buffered state to flush or
     * finalize when the input ends.
     *
     * @throws Exception if end input processing fails
     */
    @Override
    public void endInput() throws Exception {
        // No-op - nothing to finalize when input ends
    }

    // ==================== OffsetReportContext Methods ====================

    /**
     * Reports a written offset for a bucket.
     *
     * <p>This method is called from async write callbacks on multiple threads, so thread-safety is
     * critical. It uses a write lock to ensure exclusive access during the update operation.
     *
     * <p>The method updates the offset only if the new offset is greater than the existing one,
     * ensuring monotonically increasing offsets for each bucket.
     *
     * @param bucket the bucket that was written to
     * @param offset the offset of the written record
     */
    @Override
    public void reportOffset(TableBucket bucket, long offset) {
        if (bucketOffsets != null && bucketOffsetsLock != null) {
            bucketOffsetsLock.writeLock().lock();
            try {
                bucketOffsets.merge(bucket, offset, Math::max);
                if (LOG.isTraceEnabled()) {
                    LOG.trace(
                            "Reported offset {} for bucket {} (current max: {})",
                            offset,
                            bucket,
                            bucketOffsets.get(bucket));
                }
            } finally {
                bucketOffsetsLock.writeLock().unlock();
            }
        } else {
            LOG.warn(
                    "Received offset report for bucket {} before bucketOffsets was initialized, "
                            + "offset {} will be ignored",
                    bucket,
                    offset);
        }
    }

    // ==================== Lifecycle Methods ====================

    /**
     * Closes the operator and releases all resources.
     *
     * <p>This method performs cleanup in the following order:
     *
     * <ol>
     *   <li>Close the Fluss Table instance (if created)
     *   <li>Close the Fluss Connection instance (if created)
     *   <li>Call super.close() to complete operator cleanup
     * </ol>
     *
     * @throws Exception if super.close() fails
     */
    @Override
    public void close() throws Exception {
        // Close Table instance first (if created)
        if (table != null) {
            try {
                table.close();
                LOG.debug("Closed Fluss table for {}", tablePath);
            } catch (Exception e) {
                LOG.warn("Failed to close Fluss table for {}", tablePath, e);
            } finally {
                table = null;
            }
        }

        // Close Connection instance second (if created)
        if (connection != null) {
            try {
                connection.close();
                LOG.debug("Closed Fluss connection for {}", tablePath);
            } catch (Exception e) {
                LOG.warn("Failed to close Fluss connection for {}", tablePath, e);
            } finally {
                connection = null;
            }
        }

        // Call super.close() at the end
        super.close();
    }

    // ==================== Getters for Testing ====================

    public TablePath getTablePath() {
        return tablePath;
    }

    public Configuration getFlussConfig() {
        return flussConfig;
    }

    public RowType getTableRowType() {
        return tableRowType;
    }

    @Nullable
    public int[] getTargetColumnIndexes() {
        return targetColumnIndexes;
    }

    public int getNumBuckets() {
        return numBuckets;
    }

    public boolean isPartitioned() {
        return isPartitioned;
    }

    public String getProducerId() {
        return resolvedProducerId != null ? resolvedProducerId : configuredProducerId;
    }

    @Nullable
    public String getConfiguredProducerId() {
        return configuredProducerId;
    }

    public long getProducerOffsetsPollIntervalMs() {
        return producerOffsetsPollIntervalMs;
    }

    @Nullable
    public Map<TableBucket, Long> getBucketOffsets() {
        return bucketOffsets;
    }

    /**
     * Initializes the bucket offsets map and its associated lock.
     *
     * @param initialOffsets the initial bucket offsets
     */
    protected void initializeBucketOffsets(Map<TableBucket, Long> initialOffsets) {
        this.bucketOffsetsLock = new ReentrantReadWriteLock();
        this.bucketOffsets = MapUtils.newConcurrentHashMap();
        this.bucketOffsets.putAll(initialOffsets);
    }
}
