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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * WAL-driven index replicator that reads committed WAL entries, derives index mutations, and stages
 * them as pre-encoded {@link IndexBatch}es in the server-global {@link IndexAccumulator}. No
 * intermediate heap objects ({@code IndexMutation}) are created — derivation writes directly to
 * per-target-bucket {@link KvRecordBatchBuilder}s.
 *
 * <p>Strict single-window serialization: a replicator has at most one {@link IndexWindow} in flight
 * at a time. {@link #poll()} reads the next deterministic window only after the previous window's
 * batches have all been acknowledged, at which point {@link #onWindowComplete(long)} advances the
 * pushed offset. This decouples offset advancement from derivation and guarantees at-least-once
 * delivery: a send failure re-enqueues the batch without advancing the offset.
 *
 * <p>Driven by {@link #poll()} calls from an {@code IndexReplicatorPool} read worker.
 */
@Internal
public final class IndexReplicator implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexReplicator.class);

    private static final int PAGE_SIZE = 4096;

    private final LogTablet logTablet;
    private final List<IndexSpec> indexSpecs;
    private final IndexAccumulator accumulator;
    private final LogRecordReadContext readContext;
    private final java.util.function.LongConsumer onOffsetAdvanced;
    private final int maxWindowBytes;

    private volatile long indexPushedOffset;
    private volatile boolean closed;

    /**
     * The single in-flight window, or {@code null} if no window is currently being replicated.
     * Enforces strict single-window serialization: a new window is read only after the previous
     * window's batches have all been acknowledged.
     */
    @Nullable private volatile IndexWindow inFlightWindow;

    /** Signal fired to wake the owning read-pool worker so it polls again promptly. */
    @Nullable private volatile Runnable wakeupSignal;

    /**
     * Carries the already-derived old index keys from an unmatched {@link
     * ChangeType#UPDATE_BEFORE}. Holding encoded keys instead of the source row avoids depending on
     * record-batch row lifetimes when a {@code -U/+U} pair spans a batch or fetch window.
     */
    @Nullable private PendingUpdateBefore pendingUpdateBefore;

    public IndexReplicator(
            LogTablet logTablet,
            List<IndexSpec> indexSpecs,
            IndexAccumulator accumulator,
            LogRecordReadContext readContext,
            long initialOffset,
            int maxWindowBytes,
            java.util.function.LongConsumer onOffsetAdvanced) {
        this.logTablet = logTablet;
        this.indexSpecs = indexSpecs;
        this.accumulator = accumulator;
        this.readContext = readContext;
        this.indexPushedOffset = initialOffset;
        this.maxWindowBytes = maxWindowBytes;
        this.closed = false;
        this.onOffsetAdvanced = onOffsetAdvanced;
    }

    /** Sets the wake-up signal used to nudge the read-pool worker after a window completes. */
    void setWakeupSignal(Runnable wakeupSignal) {
        this.wakeupSignal = wakeupSignal;
    }

    /**
     * Poll the WAL for one window. Strict single-window serialization: if a window is already in
     * flight, returns immediately without reading. Otherwise reads one deterministic window
     * starting at {@code indexPushedOffset}, derives index batches, and stages them in the {@link
     * IndexAccumulator}. The pushed offset is advanced only when every batch of the window is
     * acknowledged (see {@link #onWindowComplete(long)}), preserving at-least-once delivery.
     *
     * @return {@code true} if a window was read (work was done), {@code false} otherwise
     */
    public boolean poll() {
        if (closed || inFlightWindow != null) {
            return false;
        }

        // Back-pressure: if the send layer is saturated, stop deriving new windows so the
        // accumulator does not grow unbounded. The owning read worker will retry after its backoff.
        if (accumulator.isFull()) {
            return false;
        }

        if (indexPushedOffset < 0) {
            indexPushedOffset = logTablet.logStartOffset();
        }

        long readOffset = nextReadOffset();
        long hw = logTablet.getHighWatermark();
        if (readOffset >= hw) {
            return false;
        }

        return pollOneWindow(readOffset);
    }

    /**
     * Read and process a single window of WAL records. Returns {@code true} if a window was read.
     */
    private boolean pollOneWindow(long readOffset) {
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
        long lastProcessedOffset = indexPushedOffset;

        boolean stoppedAtUnmatchedUpdateBefore = false;
        for (LogRecordBatch batch : records.batches()) {
            try (CloseableIterator<LogRecord> iter = batch.records(readContext)) {
                while (iter.hasNext()) {
                    LogRecord record = iter.next();
                    long processedOffset = deriveAndAppend(record, builders);
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
        if (lastProcessedOffset <= indexPushedOffset) {
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
        // the
        // pushed offset does not stall behind index-irrelevant WAL records.
        if (encoded.isEmpty()) {
            advanceOnEmptyWindow(lastProcessedOffset);
            return true;
        }

        // Stage the window: create it first so each batch can reference it, then publish batches to
        // the accumulator. inFlightWindow is set before publishing to enforce single-window serial.
        IndexWindow window = new IndexWindow(lastProcessedOffset, encoded.size(), this);
        this.inFlightWindow = window;
        for (Map.Entry<TableBucket, BytesView> entry : encoded.entrySet()) {
            accumulator.append(new IndexBatch(entry.getKey(), entry.getValue(), window));
        }
        return true;
    }

    private void advanceOnEmptyWindow(long windowEndOffset) {
        if (windowEndOffset > indexPushedOffset) {
            indexPushedOffset = windowEndOffset;
            onOffsetAdvanced.accept(windowEndOffset);
        }
    }

    @VisibleForTesting
    long nextReadOffset() {
        if (pendingUpdateBefore == null) {
            return indexPushedOffset;
        }
        return pendingUpdateBefore.offset + 1;
    }

    /**
     * Called by {@link IndexWindow} when all of its batches have been acknowledged. Advances the
     * pushed offset to the window end, clears the in-flight window, notifies the owning replica,
     * and wakes the read-pool worker so it can poll the next window.
     */
    void onWindowComplete(long windowEndOffset) {
        if (windowEndOffset > indexPushedOffset) {
            indexPushedOffset = windowEndOffset;
        }
        this.inFlightWindow = null;
        onOffsetAdvanced.accept(windowEndOffset);
        Runnable signal = this.wakeupSignal;
        if (signal != null) {
            signal.run();
        }
    }

    @VisibleForTesting
    long deriveAndAppend(LogRecord record, Map<TableBucket, BucketBatchBuilder> builders) {
        ChangeType changeType = record.getChangeType();
        InternalRow row = record.getRow();
        long offset = record.logOffset();

        if (pendingUpdateBefore != null) {
            if (changeType == ChangeType.UPDATE_BEFORE && pendingUpdateBefore.offset == offset) {
                pendingUpdateBefore = PendingUpdateBefore.from(offset, row, indexSpecs);
                return offset;
            }
            if (changeType != ChangeType.UPDATE_AFTER || offset != pendingUpdateBefore.offset + 1) {
                LOG.warn(
                        "Index replication found UPDATE_BEFORE at offset {} not followed by "
                                + "adjacent UPDATE_AFTER before offset {}; stop this window to "
                                + "avoid advancing past an incomplete update pair",
                        pendingUpdateBefore.offset,
                        offset);
                return -1L;
            }
        }

        switch (changeType) {
            case INSERT:
                appendForRow(null, row, builders);
                clearPendingUpdateBefore();
                return offset + 1;
            case UPDATE_BEFORE:
                pendingUpdateBefore = PendingUpdateBefore.from(offset, row, indexSpecs);
                return offset;
            case UPDATE_AFTER:
                if (pendingUpdateBefore == null) {
                    appendForRow(null, row, builders);
                } else {
                    appendForPendingUpdate(pendingUpdateBefore, row, builders);
                }
                clearPendingUpdateBefore();
                return offset + 1;
            case DELETE:
                appendForRow(row, null, builders);
                clearPendingUpdateBefore();
                return offset + 1;
            default:
                clearPendingUpdateBefore();
                return offset + 1;
        }
    }

    private void clearPendingUpdateBefore() {
        pendingUpdateBefore = null;
    }

    private int appendForRow(
            @Nullable InternalRow oldRow,
            @Nullable InternalRow newRow,
            Map<TableBucket, BucketBatchBuilder> builders) {
        if (oldRow == null && newRow == null) {
            return 0;
        }
        int count = 0;
        for (IndexSpec spec : indexSpecs) {
            count += appendOneSpec(spec, oldRow, newRow, builders);
        }
        return count;
    }

    private int appendForPendingUpdate(
            PendingUpdateBefore pending,
            InternalRow newRow,
            Map<TableBucket, BucketBatchBuilder> builders) {
        int count = 0;
        for (int i = 0; i < indexSpecs.size(); i++) {
            count += appendOneSpec(indexSpecs.get(i), pending.oldEntries[i], newRow, builders);
        }
        return count;
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

    public long getIndexPushedOffset() {
        return indexPushedOffset;
    }

    public void advanceIndexPushedOffset(long newOffset) {
        if (newOffset > indexPushedOffset) {
            indexPushedOffset = newOffset;
        }
    }

    @Override
    public void close() {
        closed = true;
    }

    boolean isClosed() {
        return closed;
    }

    private static final class PendingUpdateBefore {
        private final long offset;
        private final OldIndexEntry[] oldEntries;

        private PendingUpdateBefore(long offset, OldIndexEntry[] oldEntries) {
            this.offset = offset;
            this.oldEntries = oldEntries;
        }

        private static PendingUpdateBefore from(
                long offset, InternalRow row, List<IndexSpec> indexSpecs) {
            OldIndexEntry[] oldEntries = new OldIndexEntry[indexSpecs.size()];
            for (int i = 0; i < indexSpecs.size(); i++) {
                oldEntries[i] = OldIndexEntry.from(indexSpecs.get(i), row);
            }
            return new PendingUpdateBefore(offset, oldEntries);
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
