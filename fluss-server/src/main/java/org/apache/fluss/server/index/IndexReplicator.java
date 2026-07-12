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

package org.apache.fluss.server.index;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.DefaultKvRecord;
import org.apache.fluss.record.FencedKvRecordBatch;
import org.apache.fluss.record.FencedKvRecordBatchBuilder;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.WriterKey;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.log.FetchDataInfo;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.utils.CloseableIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * WAL-driven index replicator that reads committed WAL entries, derives index mutations, and stages
 * them as pre-encoded {@link IndexBatch}es in the server-global {@link IndexAccumulator}. No
 * intermediate heap objects ({@code IndexMutation}) are created — derivation writes directly to
 * per-target-bucket {@link FencedKvRecordBatchBuilder}s.
 *
 * <p>Each secondary index has its own pushed offset and at most one {@link IndexWindow} in flight.
 * {@link #poll()} reads the next valid window for every index that is currently ready. Window ends
 * may differ after failover because they depend on the fetched input and derived output size; the
 * target-side WriterState fence provides idempotence across those valid trajectories.
 *
 * <p>Driven by {@link #poll()} calls from an {@code IndexReplicatorPool} read worker.
 */
@Internal
public final class IndexReplicator implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexReplicator.class);

    private static final int PAGE_SIZE = 4096;

    /** Receives sync and all-index progress after an index window advances. */
    @FunctionalInterface
    public interface IndexProgressListener {
        /** Called with the current sync ack watermark and conservative all-index replay floor. */
        void onProgress(long syncIndexPushedOffset, long allIndexPushedOffset);
    }

    @Nullable private final SourceWal sourceWal;
    private final List<IndexProgressState> indexStates;
    private final Map<String, IndexProgressState> indexStatesByName;
    private final IndexAccumulator accumulator;
    private final LogRecordReadContext readContext;
    private final IndexProgressListener onProgress;
    private final int maxWindowBytes;
    private final long preferredMaxRequestBytes;

    /** Progress used only by empty test owners; production replicators always have index states. */
    private volatile long emptyIndexPushedOffset;

    private final AtomicBoolean closed;
    private final AtomicReference<Throwable> terminalFailure;

    /** Signal fired to wake the owning read-pool worker so it polls again promptly. */
    @Nullable private volatile Runnable wakeupSignal;

    public IndexReplicator(
            LogTablet logTablet,
            List<IndexSpec> indexSpecs,
            IndexAccumulator accumulator,
            LogRecordReadContext readContext,
            long initialOffset,
            int maxWindowBytes,
            IndexProgressListener onProgress) {
        this(
                logTablet,
                indexSpecs,
                accumulator,
                readContext,
                initialOffset,
                maxWindowBytes,
                maxWindowBytes,
                onProgress);
    }

    public IndexReplicator(
            LogTablet logTablet,
            List<IndexSpec> indexSpecs,
            IndexAccumulator accumulator,
            LogRecordReadContext readContext,
            long initialOffset,
            int maxWindowBytes,
            long preferredMaxRequestBytes,
            IndexProgressListener onProgress) {
        this(
                logTablet == null ? null : new LogTabletSourceWal(logTablet),
                indexSpecs,
                accumulator,
                readContext,
                initialOffset,
                maxWindowBytes,
                preferredMaxRequestBytes,
                onProgress);
    }

    private IndexReplicator(
            @Nullable SourceWal sourceWal,
            List<IndexSpec> indexSpecs,
            IndexAccumulator accumulator,
            LogRecordReadContext readContext,
            long initialOffset,
            int maxWindowBytes,
            long preferredMaxRequestBytes,
            IndexProgressListener onProgress) {
        this.sourceWal = sourceWal;
        this.indexStates = new ArrayList<>(indexSpecs.size());
        this.indexStatesByName = new LinkedHashMap<>();
        for (IndexSpec indexSpec : indexSpecs) {
            IndexProgressState state = new IndexProgressState(indexSpec, initialOffset);
            if (indexStatesByName.put(indexSpec.getIndexName(), state) != null) {
                throw new IllegalArgumentException(
                        "Duplicate secondary index name: " + indexSpec.getIndexName());
            }
            indexStates.add(state);
        }
        this.accumulator = accumulator;
        this.readContext = readContext;
        this.emptyIndexPushedOffset = initialOffset;
        this.maxWindowBytes = maxWindowBytes;
        this.preferredMaxRequestBytes = preferredMaxRequestBytes;
        this.closed = new AtomicBoolean(false);
        this.terminalFailure = new AtomicReference<>();
        this.onProgress = onProgress;
    }

    @VisibleForTesting
    static IndexReplicator forTesting(
            SourceWal sourceWal,
            List<IndexSpec> indexSpecs,
            IndexAccumulator accumulator,
            LogRecordReadContext readContext,
            long initialOffset,
            int maxWindowBytes,
            long preferredMaxRequestBytes,
            IndexProgressListener onProgress) {
        return new IndexReplicator(
                sourceWal,
                indexSpecs,
                accumulator,
                readContext,
                initialOffset,
                maxWindowBytes,
                preferredMaxRequestBytes,
                onProgress);
    }

    /** Sets the wake-up signal used to nudge the read-pool worker after a window completes. */
    void setWakeupSignal(Runnable wakeupSignal) {
        this.wakeupSignal = wakeupSignal;
    }

    /**
     * Poll the WAL for one window. Strict single-window serialization: if a window is already in
     * flight, returns immediately without reading. Otherwise reads one valid window starting at
     * each index's pushed offset, derives index batches, and stages them in the {@link
     * IndexAccumulator}. Each index's pushed offset is advanced only when every batch of that
     * index's window is acknowledged, preserving at-least-once delivery.
     *
     * @return {@code true} if a window was read (work was done), {@code false} otherwise
     */
    public boolean poll() {
        if (closed.get() || terminalFailure.get() != null) {
            return false;
        }

        // Back-pressure is scoped to this producer. A stalled target index bucket should stop this
        // replicator from deriving more windows, but must not stop unrelated table buckets.
        if (accumulator.isFull(this)) {
            return false;
        }

        long hw = sourceWal.highWatermark();
        boolean polled = false;
        for (IndexProgressState state : indexStates) {
            if (closed.get() || accumulator.isFull(this)) {
                break;
            }
            if (state.inFlightWindow != null) {
                continue;
            }
            if (state.pushedOffset < 0) {
                state.pushedOffset = sourceWal.logStartOffset();
            }
            long readOffset = nextReadOffset(state);
            if (readOffset >= hw) {
                continue;
            }
            polled |= pollOneWindow(state, readOffset);
        }
        return polled;
    }

    /**
     * Read and process a single window of WAL records. Returns {@code true} if a window was read.
     */
    private boolean pollOneWindow(IndexProgressState state, long readOffset) {
        FetchDataInfo fetchData;
        try {
            fetchData =
                    sourceWal.read(readOffset, maxWindowBytes, FetchIsolation.HIGH_WATERMARK, true);
        } catch (IOException e) {
            LOG.warn("Failed to read WAL at offset {}: {}", readOffset, e.getMessage());
            return false;
        }

        LogRecords records = fetchData.getRecords();
        Map<TableBucket, BucketBatchBuilder> builders = new HashMap<>();
        long lastProcessedOffset = state.pushedOffset;
        long currentWindowEncodedSize = 0L;
        boolean windowFull = false;
        for (LogRecordBatch batch : records.batches()) {
            validateSourceBatch(batch);
            try (CloseableIterator<LogRecord> iter = batch.records(readContext)) {
                while (iter.hasNext()) {
                    LogRecord record = iter.next();
                    if (record.logOffset() < readOffset) {
                        continue;
                    }
                    MutationGroup group = readMutationGroup(state.spec, iter, record);
                    MutationPlan plan = deriveMutationPlan(state.spec, group);
                    long groupEncodedDelta = plan.encodedDelta(builders);
                    long projectedSize = saturatedAdd(currentWindowEncodedSize, groupEncodedDelta);
                    if (lastProcessedOffset > state.pushedOffset
                            && projectedSize > preferredMaxRequestBytes) {
                        windowFull = true;
                        break;
                    }
                    plan.appendTo(state.spec, builders);
                    currentWindowEncodedSize = projectedSize;
                    lastProcessedOffset = group.endOffset;
                }
            }
            if (windowFull) {
                break;
            }
            lastProcessedOffset = Math.max(lastProcessedOffset, batch.nextLogOffset());
        }

        // No records advanced: nothing to do this cycle.
        if (lastProcessedOffset <= state.pushedOffset) {
            return false;
        }

        // Encode all accumulated per-bucket batches. If any bucket fails to encode, abandon the
        // whole window without advancing the pushed offset: the same WAL range is re-read and
        // re-encoded on the next cycle. Skipping the failed bucket while still advancing the offset
        // would silently drop that bucket's index mutations (permanent data loss), so a failure
        // must stall the window rather than leak past it.
        Map<TableBucket, BytesView> encoded = new HashMap<>(builders.size());
        WriterKey writerKey = IndexWriterKey.encode(sourceWal.tableBucket());
        for (Map.Entry<TableBucket, BucketBatchBuilder> entry : builders.entrySet()) {
            try {
                encoded.put(
                        entry.getKey(), entry.getValue().finish(writerKey, lastProcessedOffset));
            } catch (IOException e) {
                LOG.error(
                        "Failed to encode index batch for {} at window ending {}; abandoning the "
                                + "window and retrying without advancing the pushed offset",
                        entry.getKey(),
                        lastProcessedOffset,
                        e);
                return false;
            }
        }

        // A window that derives no index mutations is trivially complete: advance immediately so
        // the pushed offset does not stall behind index-irrelevant WAL records.
        if (encoded.isEmpty()) {
            advanceOnEmptyWindow(state, lastProcessedOffset);
            return true;
        }

        // Stage the window: create it first so each batch can reference it, then publish batches to
        // the accumulator. The per-index in-flight window is set before publishing to enforce one
        // outstanding window per index.
        IndexWindow window =
                new IndexWindow(
                        state.spec.getIndexName(), lastProcessedOffset, encoded.size(), this);
        state.inFlightWindow = window;
        List<IndexBatch> batches = new ArrayList<>(encoded.size());
        for (Map.Entry<TableBucket, BytesView> entry : encoded.entrySet()) {
            batches.add(new IndexBatch(entry.getKey(), entry.getValue(), window));
        }
        for (IndexBatch batch : batches) {
            accumulator.append(batch);
        }
        return true;
    }

    private void validateSourceBatch(LogRecordBatch batch) {
        try {
            batch.ensureValid();
        } catch (RuntimeException failure) {
            throw corruption("record batch failed integrity validation", failure);
        }
    }

    private void advanceOnEmptyWindow(IndexProgressState state, long windowEndOffset) {
        if (advanceIndexState(state, windowEndOffset)) {
            notifyProgress();
        }
    }

    @VisibleForTesting
    long nextReadOffset() {
        if (indexStates.isEmpty()) {
            return emptyIndexPushedOffset;
        }
        return nextReadOffset(indexStates.get(0));
    }

    private long nextReadOffset(IndexProgressState state) {
        return state.pushedOffset;
    }

    /**
     * Called by {@link IndexWindow} when all of its batches have been acknowledged. Advances that
     * index's pushed offset to the window end, clears the per-index in-flight window, notifies the
     * owning replica, and wakes the read-pool worker so it can poll the next ready window.
     */
    void onWindowComplete(String indexName, long windowEndOffset) {
        IndexProgressState state = indexStatesByName.get(indexName);
        if (state == null) {
            if (indexStates.isEmpty() && windowEndOffset > emptyIndexPushedOffset) {
                emptyIndexPushedOffset = windowEndOffset;
            }
        } else {
            advanceIndexState(state, windowEndOffset);
            state.inFlightWindow = null;
        }
        notifyProgress();
        Runnable signal = this.wakeupSignal;
        if (signal != null) {
            signal.run();
        }
    }

    void onWindowFailed(Throwable failure) {
        if (terminalFailure.compareAndSet(null, failure)) {
            LOG.error(
                    "Index replication for source bucket {} failed terminally at pushed offset {}",
                    sourceWal == null ? "unknown" : sourceWal.tableBucket(),
                    getAllIndexPushedOffset(),
                    failure);
        }
    }

    @VisibleForTesting
    @Nullable
    Throwable terminalFailure() {
        return terminalFailure.get();
    }

    private MutationGroup readMutationGroup(
            IndexSpec spec, CloseableIterator<LogRecord> records, LogRecord first) {
        ChangeType changeType = first.getChangeType();
        if (changeType == ChangeType.UPDATE_AFTER) {
            throw corruption(
                    "UPDATE_AFTER at offset "
                            + first.logOffset()
                            + " has no adjacent UPDATE_BEFORE in the same source batch");
        }
        if (changeType != ChangeType.UPDATE_BEFORE) {
            return MutationGroup.single(spec, first);
        }
        OldIndexEntry oldEntry = OldIndexEntry.from(spec, first.getRow());
        if (!records.hasNext()) {
            throw corruption(
                    "UPDATE_BEFORE at offset "
                            + first.logOffset()
                            + " is not completed in the same source batch");
        }
        LogRecord after = records.next();
        if (after.getChangeType() != ChangeType.UPDATE_AFTER
                || after.logOffset() != first.logOffset() + 1) {
            throw corruption(
                    "UPDATE_BEFORE at offset "
                            + first.logOffset()
                            + " is not followed by adjacent UPDATE_AFTER in the same source batch");
        }
        return MutationGroup.update(oldEntry, after);
    }

    private IndexSourceWalCorruptionException corruption(String message) {
        IndexSourceWalCorruptionException failure =
                new IndexSourceWalCorruptionException(
                        "Corrupt source WAL for " + sourceWal.tableBucket() + ": " + message);
        terminalFailure.compareAndSet(null, failure);
        return failure;
    }

    private IndexSourceWalCorruptionException corruption(String message, Throwable cause) {
        IndexSourceWalCorruptionException failure =
                new IndexSourceWalCorruptionException(
                        "Corrupt source WAL for " + sourceWal.tableBucket() + ": " + message,
                        cause);
        terminalFailure.compareAndSet(null, failure);
        return failure;
    }

    private static long saturatedAdd(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException ignored) {
            return Long.MAX_VALUE;
        }
    }

    @VisibleForTesting
    int appendOneSpec(
            IndexSpec spec,
            @Nullable InternalRow oldRow,
            @Nullable InternalRow newRow,
            Map<TableBucket, BucketBatchBuilder> builders) {
        MutationPlan plan =
                deriveMutationPlan(spec, OldIndexEntry.fromNullable(spec, oldRow), newRow);
        plan.appendTo(spec, builders);
        return plan.operationCount();
    }

    private MutationPlan deriveMutationPlan(IndexSpec spec, MutationGroup group) {
        switch (group.changeType) {
            case INSERT:
            case UPDATE_BEFORE:
                return deriveMutationPlan(spec, group.oldEntry, group.newRow);
            case DELETE:
                return deriveMutationPlan(spec, group.oldEntry, null);
            default:
                return new MutationPlan();
        }
    }

    private MutationPlan deriveMutationPlan(
            IndexSpec spec, OldIndexEntry oldEntry, @Nullable InternalRow newRow) {
        boolean oldHasIdx = oldEntry.hasIndexColumns;
        boolean newHasIdx = newRow != null && spec.hasIndexColumns(newRow);

        IndexSpec.IndexEntry newEntry = newHasIdx ? spec.encodeEntry(newRow) : null;
        boolean keysDiffer =
                oldEntry.key != null
                        && (newEntry == null || !Arrays.equals(oldEntry.key, newEntry.key()));

        MutationPlan plan = new MutationPlan();
        if (oldHasIdx && keysDiffer) {
            TableBucket tb = new TableBucket(spec.getIndexTableId(), oldEntry.targetBucket);
            plan.addDelete(tb, oldEntry.key);
        }
        if (newEntry != null && (!oldHasIdx || keysDiffer)) {
            TableBucket tb = new TableBucket(spec.getIndexTableId(), newEntry.targetBucket());
            plan.addUpsert(tb, newEntry.key(), newEntry.value());
        }
        return plan;
    }

    public long getSyncIndexPushedOffset() {
        long min = Long.MAX_VALUE;
        boolean hasSyncIndex = false;
        for (IndexProgressState state : indexStates) {
            if (state.spec.isSync()) {
                hasSyncIndex = true;
                min = Math.min(min, state.pushedOffset);
            }
        }
        if (hasSyncIndex) {
            return min;
        }
        return indexStates.isEmpty() ? emptyIndexPushedOffset : Long.MAX_VALUE;
    }

    public long getAllIndexPushedOffset() {
        if (indexStates.isEmpty()) {
            return emptyIndexPushedOffset;
        }
        long min = Long.MAX_VALUE;
        for (IndexProgressState state : indexStates) {
            min = Math.min(min, state.pushedOffset);
        }
        return min;
    }

    private boolean advanceIndexState(IndexProgressState state, long newOffset) {
        if (newOffset > state.pushedOffset) {
            state.pushedOffset = newOffset;
            return true;
        }
        return false;
    }

    private void notifyProgress() {
        onProgress.onProgress(getSyncIndexPushedOffset(), getAllIndexPushedOffset());
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true) && readContext != null) {
            readContext.close();
        }
    }

    boolean isClosed() {
        return closed.get();
    }

    @VisibleForTesting
    interface SourceWal {
        TableBucket tableBucket();

        long highWatermark();

        long logStartOffset();

        FetchDataInfo read(
                long offset, int maxBytes, FetchIsolation isolation, boolean minOneMessage)
                throws IOException;
    }

    private static final class LogTabletSourceWal implements SourceWal {
        private final LogTablet logTablet;

        private LogTabletSourceWal(LogTablet logTablet) {
            this.logTablet = logTablet;
        }

        @Override
        public TableBucket tableBucket() {
            return logTablet.getTableBucket();
        }

        @Override
        public long highWatermark() {
            return logTablet.getHighWatermark();
        }

        @Override
        public long logStartOffset() {
            return logTablet.logStartOffset();
        }

        @Override
        public FetchDataInfo read(
                long offset, int maxBytes, FetchIsolation isolation, boolean minOneMessage)
                throws IOException {
            return logTablet.read(offset, maxBytes, isolation, minOneMessage);
        }
    }

    private static final class IndexProgressState {
        private final IndexSpec spec;
        private volatile long pushedOffset;
        @Nullable private volatile IndexWindow inFlightWindow;

        private IndexProgressState(IndexSpec spec, long initialOffset) {
            this.spec = spec;
            this.pushedOffset = initialOffset;
        }
    }

    private static final class MutationGroup {
        private final ChangeType changeType;
        private final OldIndexEntry oldEntry;
        @Nullable private final InternalRow newRow;
        private final long endOffset;

        private MutationGroup(
                ChangeType changeType,
                OldIndexEntry oldEntry,
                @Nullable InternalRow newRow,
                long endOffset) {
            this.changeType = changeType;
            this.oldEntry = oldEntry;
            this.newRow = newRow;
            this.endOffset = endOffset;
        }

        private static MutationGroup single(IndexSpec spec, LogRecord record) {
            return record.getChangeType() == ChangeType.DELETE
                    ? new MutationGroup(
                            ChangeType.DELETE,
                            OldIndexEntry.from(spec, record.getRow()),
                            null,
                            record.logOffset() + 1)
                    : new MutationGroup(
                            record.getChangeType(),
                            OldIndexEntry.EMPTY,
                            record.getRow(),
                            record.logOffset() + 1);
        }

        private static MutationGroup update(OldIndexEntry oldEntry, LogRecord after) {
            return new MutationGroup(
                    ChangeType.UPDATE_BEFORE,
                    oldEntry,
                    after.getRow(),
                    after.logOffset() + 1);
        }
    }

    private static final class OldIndexEntry {
        private static final OldIndexEntry EMPTY = new OldIndexEntry(false, null, -1);

        private final boolean hasIndexColumns;
        @Nullable private final byte[] key;
        private final int targetBucket;

        private OldIndexEntry(boolean hasIndexColumns, @Nullable byte[] key, int targetBucket) {
            this.hasIndexColumns = hasIndexColumns;
            this.key = key;
            this.targetBucket = targetBucket;
        }

        private static OldIndexEntry from(IndexSpec spec, InternalRow row) {
            if (!spec.hasIndexColumns(row)) {
                return new OldIndexEntry(false, null, -1);
            }
            IndexSpec.IndexEntry entry = spec.encodeEntry(row);
            return new OldIndexEntry(true, entry.key(), entry.targetBucket());
        }

        private static OldIndexEntry fromNullable(IndexSpec spec, @Nullable InternalRow row) {
            if (row == null) {
                return EMPTY;
            }
            return from(spec, row);
        }
    }

    private static final class MutationPlan {
        private final List<Mutation> operations = new ArrayList<>(2);

        private void addDelete(TableBucket targetBucket, byte[] key) {
            operations.add(
                    new Mutation(
                            targetBucket, key, null, DefaultKvRecord.sizeOf(key, null)));
        }

        private void addUpsert(TableBucket targetBucket, byte[] key, BinaryRow value) {
            operations.add(
                    new Mutation(
                            targetBucket, key, value, DefaultKvRecord.sizeOf(key, value)));
        }

        private int operationCount() {
            return operations.size();
        }

        private long encodedDelta(Map<TableBucket, BucketBatchBuilder> builders) {
            long delta = 0L;
            TableBucket firstNewBucket = null;
            for (Mutation operation : operations) {
                if (!builders.containsKey(operation.targetBucket)
                        && !operation.targetBucket.equals(firstNewBucket)) {
                    delta += FencedKvRecordBatch.RECORD_BATCH_HEADER_SIZE;
                    firstNewBucket = operation.targetBucket;
                }
                delta += operation.recordSize;
            }
            return delta;
        }

        private void appendTo(
                IndexSpec spec, Map<TableBucket, BucketBatchBuilder> builders) {
            for (Mutation operation : operations) {
                BucketBatchBuilder builder =
                        builders.computeIfAbsent(
                                operation.targetBucket,
                                ignored ->
                                        new BucketBatchBuilder(
                                                (short) spec.getIndexSchemaId(),
                                                spec.getIndexKvFormat()));
                if (operation.value == null) {
                    builder.appendDelete(operation.key);
                } else {
                    builder.appendUpsert(operation.key, operation.value);
                }
            }
        }
    }

    private static final class Mutation {
        private final TableBucket targetBucket;
        private final byte[] key;
        @Nullable private final BinaryRow value;
        private final int recordSize;

        private Mutation(
                TableBucket targetBucket,
                byte[] key,
                @Nullable BinaryRow value,
                int recordSize) {
            this.targetBucket = targetBucket;
            this.key = key;
            this.value = value;
            this.recordSize = recordSize;
        }
    }

    /** Per-target-bucket builder that directly encodes KV records. */
    static final class BucketBatchBuilder {
        final FencedKvRecordBatchBuilder builder;

        BucketBatchBuilder(short schemaId, KvFormat kvFormat) {
            UnmanagedPagedOutputView output = new UnmanagedPagedOutputView(PAGE_SIZE);
            this.builder =
                    FencedKvRecordBatchBuilder.builder(
                            schemaId, Integer.MAX_VALUE, output, kvFormat);
        }

        void appendUpsert(byte[] key, BinaryRow value) {
            try {
                builder.append(key, value);
            } catch (IOException e) {
                throw new RuntimeException("Failed to append upsert to batch", e);
            }
        }

        void appendDelete(byte[] key) {
            try {
                builder.append(key, null);
            } catch (IOException e) {
                throw new RuntimeException("Failed to append delete to batch", e);
            }
        }

        BytesView finish(WriterKey writerKey, long windowEndOffset) throws IOException {
            builder.setWriterState(writerKey, windowEndOffset);
            return builder.build();
        }
    }
}
