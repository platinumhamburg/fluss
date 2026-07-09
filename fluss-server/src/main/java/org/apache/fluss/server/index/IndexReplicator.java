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
import org.apache.fluss.record.KvRecordBatchBuilder;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
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

/**
 * WAL-driven index replicator that reads committed WAL entries, derives index mutations, and stages
 * them as pre-encoded {@link IndexBatch}es in the server-global {@link IndexAccumulator}. No
 * intermediate heap objects ({@code IndexMutation}) are created — derivation writes directly to
 * per-target-bucket {@link KvRecordBatchBuilder}s.
 *
 * <p>Each secondary index has its own pushed offset and at most one {@link IndexWindow} in flight.
 * {@link #poll()} reads the next deterministic window for every index that is currently ready. This
 * lets SYNC indexes keep advancing even when an ASYNC index is retrying an older window, while the
 * all-index watermark remains conservative for snapshot/recovery floors.
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

    private final LogTablet logTablet;
    private final List<IndexProgressState> indexStates;
    private final Map<String, IndexProgressState> indexStatesByName;
    private final IndexAccumulator accumulator;
    private final LogRecordReadContext readContext;
    private final IndexProgressListener onProgress;
    private final int maxWindowBytes;

    /** Progress used only by empty test owners; production replicators always have index states. */
    private volatile long emptyIndexPushedOffset;
    private volatile boolean closed;

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
        this.logTablet = logTablet;
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
        this.closed = false;
        this.onProgress = onProgress;
    }

    /** Sets the wake-up signal used to nudge the read-pool worker after a window completes. */
    void setWakeupSignal(Runnable wakeupSignal) {
        this.wakeupSignal = wakeupSignal;
    }

    /**
     * Poll the WAL for one window. Strict single-window serialization: if a window is already in
     * flight, returns immediately without reading. Otherwise reads one deterministic window
     * starting at each index's pushed offset, derives index batches, and stages them in the {@link
     * IndexAccumulator}. Each index's pushed offset is advanced only when every batch of that
     * index's window is acknowledged, preserving at-least-once delivery.
     *
     * @return {@code true} if a window was read (work was done), {@code false} otherwise
     */
    public boolean poll() {
        if (closed) {
            return false;
        }

        // Back-pressure: if the send layer is saturated, stop deriving new windows so the
        // accumulator does not grow unbounded. The owning read worker will retry after its backoff.
        if (accumulator.isFull()) {
            return false;
        }

        long hw = logTablet.getHighWatermark();
        boolean polled = false;
        for (IndexProgressState state : indexStates) {
            if (closed || accumulator.isFull()) {
                break;
            }
            if (state.inFlightWindow != null) {
                continue;
            }
            if (state.pushedOffset < 0) {
                state.pushedOffset = logTablet.logStartOffset();
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
                    logTablet.read(readOffset, maxWindowBytes, FetchIsolation.HIGH_WATERMARK, true);
        } catch (IOException e) {
            LOG.warn("Failed to read WAL at offset {}: {}", readOffset, e.getMessage());
            return false;
        }

        LogRecords records = fetchData.getRecords();
        Map<TableBucket, BucketBatchBuilder> builders = new HashMap<>();
        long lastProcessedOffset = state.pushedOffset;

        boolean stoppedAtUnmatchedUpdateBefore = false;
        for (LogRecordBatch batch : records.batches()) {
            try (CloseableIterator<LogRecord> iter = batch.records(readContext)) {
                while (iter.hasNext()) {
                    LogRecord record = iter.next();
                    long processedOffset = deriveAndAppend(state, record, builders);
                    if (processedOffset < 0) {
                        stoppedAtUnmatchedUpdateBefore = true;
                        break;
                    }
                    lastProcessedOffset = Math.max(lastProcessedOffset, processedOffset);
                }
            }
            if (stoppedAtUnmatchedUpdateBefore) {
                break;
            }
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
        for (Map.Entry<TableBucket, BucketBatchBuilder> entry : builders.entrySet()) {
            try {
                encoded.put(entry.getKey(), entry.getValue().builder.build());
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
                new IndexWindow(state.spec.getIndexName(), lastProcessedOffset, encoded.size(), this);
        state.inFlightWindow = window;
        for (Map.Entry<TableBucket, BytesView> entry : encoded.entrySet()) {
            accumulator.append(new IndexBatch(entry.getKey(), entry.getValue(), window));
        }
        return true;
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
        if (state.pendingUpdateBefore == null) {
            return state.pushedOffset;
        }
        return state.pendingUpdateBefore.offset + 1;
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

    @VisibleForTesting
    long deriveAndAppend(LogRecord record, Map<TableBucket, BucketBatchBuilder> builders) {
        if (indexStates.isEmpty()) {
            return record.logOffset() + 1;
        }
        return deriveAndAppend(indexStates.get(0), record, builders);
    }

    private long deriveAndAppend(
            IndexProgressState state,
            LogRecord record,
            Map<TableBucket, BucketBatchBuilder> builders) {
        ChangeType changeType = record.getChangeType();
        InternalRow row = record.getRow();
        long offset = record.logOffset();

        if (state.pendingUpdateBefore != null) {
            if (changeType == ChangeType.UPDATE_BEFORE
                    && state.pendingUpdateBefore.offset == offset) {
                state.pendingUpdateBefore = PendingUpdateBefore.from(offset, state.spec, row);
                return offset;
            }
            if (changeType != ChangeType.UPDATE_AFTER
                    || offset != state.pendingUpdateBefore.offset + 1) {
                LOG.warn(
                        "Index replication found UPDATE_BEFORE at offset {} not followed by "
                                + "adjacent UPDATE_AFTER before offset {}; stop this window to "
                                + "avoid advancing past an incomplete update pair",
                        state.pendingUpdateBefore.offset,
                        offset);
                return -1L;
            }
        }

        switch (changeType) {
            case INSERT:
                appendOneSpec(state.spec, (InternalRow) null, row, builders);
                clearPendingUpdateBefore(state);
                return offset + 1;
            case UPDATE_BEFORE:
                state.pendingUpdateBefore = PendingUpdateBefore.from(offset, state.spec, row);
                return offset;
            case UPDATE_AFTER:
                if (state.pendingUpdateBefore == null) {
                    appendOneSpec(state.spec, (InternalRow) null, row, builders);
                } else {
                    appendOneSpec(state.spec, state.pendingUpdateBefore.oldEntry, row, builders);
                }
                clearPendingUpdateBefore(state);
                return offset + 1;
            case DELETE:
                appendOneSpec(state.spec, row, null, builders);
                clearPendingUpdateBefore(state);
                return offset + 1;
            default:
                clearPendingUpdateBefore(state);
                return offset + 1;
        }
    }

    private void clearPendingUpdateBefore(IndexProgressState state) {
        state.pendingUpdateBefore = null;
    }

    @VisibleForTesting
    int appendOneSpec(
            IndexSpec spec,
            @Nullable InternalRow oldRow,
            @Nullable InternalRow newRow,
            Map<TableBucket, BucketBatchBuilder> builders) {
        return appendOneSpec(spec, OldIndexEntry.fromNullable(spec, oldRow), newRow, builders);
    }

    private int appendOneSpec(
            IndexSpec spec,
            OldIndexEntry oldEntry,
            @Nullable InternalRow newRow,
            Map<TableBucket, BucketBatchBuilder> builders) {
        boolean oldHasIdx = oldEntry.hasIndexColumns;
        boolean newHasIdx = newRow != null && spec.hasIndexColumns(newRow);

        byte[] newKey = null;
        if (newHasIdx) {
            newKey = spec.getKeyEncoder().encodeKey(newRow);
        }
        boolean keysDiffer =
                oldEntry.key != null && (newKey == null || !Arrays.equals(oldEntry.key, newKey));

        int count = 0;
        if (oldHasIdx && keysDiffer) {
            TableBucket tb = new TableBucket(spec.getIndexTableId(), oldEntry.bucket);
            getBuilder(tb, spec, builders).appendDelete(oldEntry.key);
            count++;
        }
        if (newHasIdx) {
            // Skip an UPDATE upsert when the index key/value is byte-for-byte identical.
            boolean valueUnchanged = oldHasIdx && !keysDiffer;
            if (!valueUnchanged) {
                BinaryRow value = spec.getValueEncoder().encode(newRow);
                int bucket = spec.getBucketAssigner().applyAsInt(newRow);
                TableBucket tb = new TableBucket(spec.getIndexTableId(), bucket);
                getBuilder(tb, spec, builders).appendUpsert(newKey, value);
                count++;
            }
        }
        return count;
    }

    private BucketBatchBuilder getBuilder(
            TableBucket tb, IndexSpec spec, Map<TableBucket, BucketBatchBuilder> builders) {
        return builders.computeIfAbsent(
                tb,
                k ->
                        new BucketBatchBuilder(
                                (short) spec.getIndexSchemaId(), spec.getIndexKvFormat()));
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
        closed = true;
    }

    boolean isClosed() {
        return closed;
    }

    private static final class IndexProgressState {
        private final IndexSpec spec;
        private volatile long pushedOffset;
        @Nullable private volatile IndexWindow inFlightWindow;
        @Nullable private PendingUpdateBefore pendingUpdateBefore;

        private IndexProgressState(IndexSpec spec, long initialOffset) {
            this.spec = spec;
            this.pushedOffset = initialOffset;
        }
    }

    private static final class PendingUpdateBefore {
        private final long offset;
        private final OldIndexEntry oldEntry;

        private PendingUpdateBefore(long offset, OldIndexEntry oldEntry) {
            this.offset = offset;
            this.oldEntry = oldEntry;
        }

        private static PendingUpdateBefore from(long offset, IndexSpec indexSpec, InternalRow row) {
            return new PendingUpdateBefore(offset, OldIndexEntry.from(indexSpec, row));
        }
    }

    private static final class OldIndexEntry {
        private final boolean hasIndexColumns;
        @Nullable private final byte[] key;
        private final int bucket;

        private OldIndexEntry(boolean hasIndexColumns, @Nullable byte[] key, int bucket) {
            this.hasIndexColumns = hasIndexColumns;
            this.key = key;
            this.bucket = bucket;
        }

        private static OldIndexEntry from(IndexSpec spec, InternalRow row) {
            if (!spec.hasIndexColumns(row)) {
                return new OldIndexEntry(false, null, 0);
            }
            return new OldIndexEntry(
                    true,
                    spec.getKeyEncoder().encodeKey(row),
                    spec.getBucketAssigner().applyAsInt(row));
        }

        private static OldIndexEntry fromNullable(IndexSpec spec, @Nullable InternalRow row) {
            if (row == null) {
                return new OldIndexEntry(false, null, 0);
            }
            return from(spec, row);
        }
    }

    /** Per-target-bucket builder that directly encodes KV records. */
    static final class BucketBatchBuilder {
        final KvRecordBatchBuilder builder;
        int count;

        BucketBatchBuilder(short schemaId, KvFormat kvFormat) {
            UnmanagedPagedOutputView output = new UnmanagedPagedOutputView(PAGE_SIZE);
            this.builder =
                    KvRecordBatchBuilder.builder(schemaId, Integer.MAX_VALUE, output, kvFormat);
            this.count = 0;
        }

        void appendUpsert(byte[] key, BinaryRow value) {
            try {
                builder.append(key, value);
                count++;
            } catch (IOException e) {
                throw new RuntimeException("Failed to append upsert to batch", e);
            }
        }

        void appendDelete(byte[] key) {
            try {
                builder.append(key, null);
                count++;
            } catch (IOException e) {
                throw new RuntimeException("Failed to append delete to batch", e);
            }
        }
    }
}
