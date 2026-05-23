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
import org.apache.fluss.metadata.IndexMutation;
import org.apache.fluss.row.InternalRow;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongConsumer;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Per-Replica orchestrator wiring {@link IndexMutationExtractor}, {@link IndexPushTracker}, and
 * {@link IndexPushSender} for the secondary-index push pipeline (FIP V2 §3).
 *
 * <p>One instance is owned by each data-bucket leader {@code Replica}. For every base-row mutation
 * applied to the KV store, {@link #submit(long, InternalRow, InternalRow)}:
 *
 * <ol>
 *   <li>derives {@link IndexMutation}s via {@link IndexMutationExtractor#extract},
 *   <li>computes the set of target Index buckets and {@linkplain IndexPushTracker#register
 *       registers} it with the tracker,
 *   <li>hands the mutations to the {@link IndexPushSender} for asynchronous dispatch.
 * </ol>
 *
 * <p>Once every target for a given source offset acks, the tracker advances its watermark and this
 * scheduler invokes the supplied {@code indexPushedOffsetAdvancedCallback} so the surrounding
 * {@code Replica} can update its persisted {@code indexPushedOffset}. The callback fires at most
 * once per advancement and never with a value that has already been published.
 *
 * <p>Construction uses a {@link Function} factory so the scheduler can build its own ack callback
 * before materializing the sender, avoiding awkward late-bound setters.
 */
@Internal
@ThreadSafe
public final class IndexPushScheduler implements AutoCloseable {

    private final IndexMutationExtractor.Context extractorContext;
    private final IndexPushTracker tracker;
    private final IndexPushSender sender;
    private final LongConsumer indexPushedOffsetAdvancedCallback;

    /** Serializes ack handling so {@code prev/new} offset comparisons are race-free. */
    private final Object advanceLock = new Object();

    public IndexPushScheduler(
            IndexMutationExtractor.Context extractorContext,
            IndexPushTracker tracker,
            Function<BiConsumer<Long, IndexPushTracker.Target>, IndexPushSender> senderFactory,
            LongConsumer indexPushedOffsetAdvancedCallback) {
        this.extractorContext = checkNotNull(extractorContext, "extractorContext");
        this.tracker = checkNotNull(tracker, "tracker");
        this.indexPushedOffsetAdvancedCallback =
                checkNotNull(
                        indexPushedOffsetAdvancedCallback,
                        "indexPushedOffsetAdvancedCallback");
        checkNotNull(senderFactory, "senderFactory");
        this.sender =
                checkNotNull(
                        senderFactory.apply(this::onAck),
                        "senderFactory returned null IndexPushSender");
    }

    /**
     * Submit a {@code (oldRow, newRow)} record from the KV apply path. Extracts mutations,
     * registers the resulting target set with the tracker, and hands the mutations to the sender.
     *
     * <p>Returns immediately after registration / enqueue; the actual push happens asynchronously
     * via the sender and acks advance the watermark in the background.
     */
    public void submit(
            long sourceOffset, @Nullable InternalRow oldRow, @Nullable InternalRow newRow) {
        List<IndexMutation> mutations =
                IndexMutationExtractor.extract(extractorContext, oldRow, newRow, sourceOffset);
        Set<IndexPushTracker.Target> targets = new HashSet<>();
        for (IndexMutation m : mutations) {
            targets.add(new IndexPushTracker.Target(m.getIndexTableId(), m.getIndexBucket()));
        }
        tracker.register(sourceOffset, targets);
        sender.send(mutations);
    }

    /** Returns the most recent {@code indexPushedOffset} published by the tracker. */
    public long getIndexPushedOffset() {
        return tracker.getIndexPushedOffset();
    }

    /**
     * Stops accepting new submissions and waits for the underlying sender to drain in-flight
     * batches. After {@code close()} returns, any further {@link #submit} will fail with {@link
     * IllegalStateException} thrown by the sender.
     */
    @Override
    public void close() {
        sender.close();
    }

    /**
     * Bridges sender acks to tracker advancement. Holds {@link #advanceLock} so the {@code (prev,
     * new)} comparison is atomic with respect to concurrent acks, ensuring the advance callback is
     * never invoked with a stale value or for a value already published.
     */
    private void onAck(long sourceOffset, IndexPushTracker.Target target) {
        synchronized (advanceLock) {
            long prev = tracker.getIndexPushedOffset();
            long newOffset = tracker.ack(sourceOffset, target);
            if (newOffset > prev) {
                indexPushedOffsetAdvancedCallback.accept(newOffset);
            }
        }
    }
}
