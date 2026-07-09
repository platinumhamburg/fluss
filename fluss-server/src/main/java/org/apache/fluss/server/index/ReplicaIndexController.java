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
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.utils.IndexTableUtils;

import org.rocksdb.AbstractCompactionFilter;
import org.rocksdb.AbstractCompactionFilterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToLongFunction;

/**
 * Per-{@code Replica} controller that owns all index-related state and lifecycle.
 *
 * <p>Extracted from {@code Replica} to break the God Class anti-pattern: the index logic
 * (IndexReplicator lifecycle, partition-tombstone filtering, compaction-filter factory creation,
 * index-pushed-offset management) lives here instead of being scattered across a 2600-line class.
 *
 * <p>This controller does not drive replication itself. It registers the leader-side {@link
 * IndexReplicator} into the server-global {@link IndexReplicatorPool} (the read layer); the pool
 * workers poll the replicator, which stages encoded batches into the shared {@link
 * IndexAccumulator}, from which the {@link IndexSender} workers dispatch them. HW advances merely
 * signal the owning pool worker to poll.
 *
 * <h3>State machine</h3>
 *
 * <pre>
 *   NOT_STARTED --&gt; DEFERRED   (metadata not yet available)
 *   NOT_STARTED --&gt; RUNNING    (metadata available, replicator started)
 *   DEFERRED   --&gt; RUNNING    (metadata arrived, retry succeeded)
 *   RUNNING    --&gt; NOT_STARTED (become follower / close)
 * </pre>
 */
@Internal
public final class ReplicaIndexController {

    /** Lifecycle state of the index replicator for this replica. */
    enum State {
        NOT_STARTED,
        DEFERRED,
        RUNNING
    }

    private static final Logger LOG = LoggerFactory.getLogger(ReplicaIndexController.class);

    private final TableInfo tableInfo;
    private final TableBucket tableBucket;
    private final TabletServerMetadataCache metadataCache;
    @Nullable private final IndexReplicatorPool replicatorPool;
    @Nullable private final IndexAccumulator accumulator;
    private final TabletServerMetricGroup metrics;
    private final AtomicReference<State> state = new AtomicReference<>(State.NOT_STARTED);

    @Nullable private IndexReplicator indexReplicator;

    /**
     * The partition-tombstone judge for Index Tables; {@code null} for main tables and for Index
     * Tables that are either non-partitioned or have not yet entered the leader role.
     */
    @Nullable private TombstonedPartitionDiscriminator tombstoneDiscriminator;

    public ReplicaIndexController(
            TableInfo tableInfo,
            TableBucket tableBucket,
            TabletServerMetadataCache metadataCache,
            @Nullable IndexReplicatorPool replicatorPool,
            @Nullable IndexAccumulator accumulator,
            TabletServerMetricGroup metrics) {
        this.tableInfo = tableInfo;
        this.tableBucket = tableBucket;
        this.metadataCache = metadataCache;
        this.replicatorPool = replicatorPool;
        this.accumulator = accumulator;
        this.metrics = metrics;
    }

    // ------------------------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------------------------

    /**
     * Prepares leader-only index table state before the {@link KvTablet} is opened. RocksDB
     * compaction filters must be installed while the column family is created, so the tombstone
     * discriminator cannot be initialized after {@code Replica#createKv()}.
     */
    public void prepareForLeader() {
        this.tombstoneDiscriminator =
                TombstonedPartitionDiscriminator.forIndexTable(tableInfo, metadataCache);
    }

    /**
     * Called when the replica becomes leader. If the table has secondary indexes and the metadata
     * is available, starts the {@link IndexReplicator} immediately; otherwise transitions to {@link
     * State#DEFERRED}.
     *
     * <p>For Index Tables, initialises the partition-tombstone filter state used during compaction,
     * scan and prefix-lookup.
     *
     * @param logTablet the log tablet of the leader replica
     * @param schemaGetter the schema getter for the table
     * @param onProgress callback fired after each window completes with sync/all index progress;
     *     typically wired to {@code Replica::advanceIndexProgress}
     * @param initialOffset the seeded index offset (from snapshot restore), or {@code -1L}
     */
    public void onBecomeLeader(
            LogTablet logTablet,
            SchemaGetter schemaGetter,
            IndexReplicator.IndexProgressListener onProgress,
            long initialOffset) {
        prepareForLeader();

        // Start IndexReplicator for main tables with secondary indexes
        List<Schema.Index> indexes = tableInfo.getSchema().getIndexes();
        if (indexes.isEmpty()) {
            return;
        }

        maybeStartIndexReplicator(
                logTablet, schemaGetter, indexes, onProgress, initialOffset);
    }

    /**
     * Completes leader initialisation after the leader KV tablet has been opened. This keeps the
     * leader-side index lifecycle ordering inside the controller: start/defer the WAL-driven index
     * replicator for main tables and install tombstone filtering for partitioned Index Tables.
     */
    public void onLeaderKvReady(
            LogTablet logTablet,
            SchemaGetter schemaGetter,
            KvTablet kvTablet,
            IndexReplicator.IndexProgressListener onProgress,
            long initialOffset) {
        onBecomeLeader(logTablet, schemaGetter, onProgress, initialOffset);
        installValueFilter(kvTablet);
    }

    /** Called when the replica becomes follower. Stops the index replicator if running. */
    public void onBecomeFollower() {
        stopIndexReplicator();
        this.tombstoneDiscriminator = null;
    }

    /**
     * Retries starting the index replicator if it was previously deferred due to missing metadata.
     * Called by {@code ReplicaManager} after a metadata update.
     */
    public void retryStart(
            LogTablet logTablet,
            SchemaGetter schemaGetter,
            IndexReplicator.IndexProgressListener onProgress,
            long initialOffset) {
        if (state.get() != State.DEFERRED) {
            return;
        }
        List<Schema.Index> indexes = tableInfo.getSchema().getIndexes();
        if (indexes.isEmpty()) {
            return;
        }
        maybeStartIndexReplicator(
                logTablet, schemaGetter, indexes, onProgress, initialOffset);
    }

    /** Called when the replica is being deleted. */
    public void close() {
        stopIndexReplicator();
        this.tombstoneDiscriminator = null;
    }

    // ------------------------------------------------------------------------------------
    // High-watermark callback
    // ------------------------------------------------------------------------------------

    /**
     * Called after the leader HW advances. Signals the read-pool worker owning this replicator so
     * it polls the newly committed WAL window. No-op when no replicator is active.
     */
    public void onHighWatermarkAdvanced() {
        if (indexReplicator != null && replicatorPool != null) {
            replicatorPool.signal(tableBucket);
        }
    }

    // ------------------------------------------------------------------------------------
    // Index-pushed-offset
    // ------------------------------------------------------------------------------------

    /** Returns the current sync index-pushed-offset, or {@code -1L} if no replicator is active. */
    public long getSyncIndexPushedOffset() {
        IndexReplicator r = this.indexReplicator;
        return r != null ? r.getSyncIndexPushedOffset() : -1L;
    }

    /** Returns the conservative all-index replay floor, or {@code -1L} if no replicator is active. */
    public long getAllIndexPushedOffset() {
        IndexReplicator r = this.indexReplicator;
        return r != null ? r.getAllIndexPushedOffset() : -1L;
    }

    // ------------------------------------------------------------------------------------
    // Index Table: compaction filter factory
    // ------------------------------------------------------------------------------------

    /**
     * Creates a compaction filter factory if this replica is for a partitioned Index Table;
     * otherwise returns {@code null}.
     */
    @Nullable
    public AbstractCompactionFilterFactory<? extends AbstractCompactionFilter<?>>
            createCompactionFilterFactory() {
        TombstonedPartitionDiscriminator d = this.tombstoneDiscriminator;
        return d == null ? null : d.createCompactionFilterFactory();
    }

    /**
     * Creates a tag extractor function for v3 value encoding if this replica is for a partitioned
     * Index Table; otherwise returns {@code null}.
     */
    @Nullable
    public ToLongFunction<BinaryRow> createTagExtractor() {
        TombstonedPartitionDiscriminator d = this.tombstoneDiscriminator;
        return d == null ? null : d.createTagExtractor();
    }

    // ------------------------------------------------------------------------------------
    // Index Table: value filter installation
    // ------------------------------------------------------------------------------------

    /**
     * Installs a partition-tombstone value filter on the given {@link KvTablet} if this replica is
     * for a partitioned Index Table. The tombstone judgment is thread-safe (see {@link
     * TombstonedPartitionDiscriminator#isTombstoned(byte[])}).
     */
    public void installValueFilter(KvTablet kvTablet) {
        TombstonedPartitionDiscriminator d = this.tombstoneDiscriminator;
        if (d == null || kvTablet == null) {
            return;
        }
        kvTablet.setValueFilter(
                valueBytes -> {
                    boolean drop = d.isTombstoned(valueBytes);
                    if (drop) {
                        metrics.partitionTombstoneApplyDrops().inc();
                    }
                    return drop;
                });
        LOG.info(
                "Index Table partition-tombstone filter installed for {} (mainTableId={})",
                tableBucket,
                d.mainTableId());
    }

    // ------------------------------------------------------------------------------------
    // Main Table: prefix-lookup tombstone filtering
    // ------------------------------------------------------------------------------------

    /**
     * Filters prefix-lookup results for an Index Table, removing rows whose partition is
     * tombstoned. Returns the input list unchanged if this is not an Index Table or no tombstones
     * exist.
     */
    public List<byte[]> filterTombstonedEntries(List<byte[]> rawResults) {
        TombstonedPartitionDiscriminator d = this.tombstoneDiscriminator;
        if (rawResults.isEmpty() || d == null) {
            return rawResults;
        }
        if (!d.hasTombstonedPartitions()) {
            return rawResults;
        }
        List<byte[]> filtered = new ArrayList<>(rawResults.size());
        for (byte[] value : rawResults) {
            if (!d.isTombstoned(value)) {
                filtered.add(value);
            }
        }
        return filtered;
    }

    // ------------------------------------------------------------------------------------
    // State queries
    // ------------------------------------------------------------------------------------

    public State getState() {
        return state.get();
    }

    /** Returns {@code true} when the replicator is waiting for metadata to become available. */
    public boolean isDeferred() {
        return state.get() == State.DEFERRED;
    }

    @VisibleForTesting
    public IndexReplicator getIndexReplicator() {
        return indexReplicator;
    }

    // ------------------------------------------------------------------------------------
    // Internal
    // ------------------------------------------------------------------------------------

    private void maybeStartIndexReplicator(
            LogTablet logTablet,
            SchemaGetter schemaGetter,
            List<Schema.Index> indexes,
            IndexReplicator.IndexProgressListener onProgress,
            long initialOffset) {
        if (replicatorPool == null || accumulator == null) {
            return;
        }

        // Check that all index tables are visible in the metadata cache
        for (Schema.Index index : indexes) {
            TablePath indexPath =
                    TablePath.of(
                            tableInfo.getTablePath().getDatabaseName(),
                            IndexTableUtils.indexTableName(
                                    tableInfo.getTablePath().getTableName(), index.getIndexName()));
            OptionalLong indexTableId = metadataCache.getTableId(indexPath);
            if (!indexTableId.isPresent()) {
                state.set(State.DEFERRED);
                LOG.info(
                        "Index table {} not yet visible in cache -- deferring IndexReplicator; "
                                + "will retry on next metadata update.",
                        indexPath);
                return;
            }
        }

        List<IndexSpec> indexSpecs =
                IndexSpecFactory.buildIndexSpecs(tableInfo, tableBucket, metadataCache);
        LogRecordReadContext readContext =
                LogRecordReadContext.createReadContext(tableInfo, false, null, schemaGetter);
        // Use the already-seeded offset (from snapshot restore or -1) as the starting point.
        IndexReplicator replicator =
                new IndexReplicator(
                        logTablet,
                        indexSpecs,
                        accumulator,
                        readContext,
                        initialOffset,
                        replicatorPool.maxWindowBytes(),
                        onProgress);
        this.indexReplicator = replicator;
        state.set(State.RUNNING);
        // Register with the read pool; the pool worker will catch up on any WAL entries already
        // committed before this replicator was created.
        replicatorPool.register(tableBucket, replicator);
        LOG.info("IndexReplicator (WAL-driven) registered for {}", tableBucket);
    }

    private void stopIndexReplicator() {
        IndexReplicator r = this.indexReplicator;
        if (r != null) {
            if (replicatorPool != null) {
                replicatorPool.unregister(tableBucket);
            }
            try {
                r.close();
            } catch (Exception e) {
                LOG.warn("Error closing IndexReplicator for {}", tableBucket, e);
            }
            // Drop any batches still queued for this replicator. Once it is gone (table dropped or
            // leadership moved) those batches can never resolve a leader and would otherwise loop
            // forever in the sender's at-least-once retry, pinning memory and holding
            // back-pressure.
            if (accumulator != null) {
                int dropped = accumulator.dropForReplicator(r);
                if (dropped > 0) {
                    LOG.info(
                            "Discarded {} queued index batch(es) for {} after stopping its "
                                    + "replicator",
                            dropped,
                            tableBucket);
                }
            }
            this.indexReplicator = null;
        }
        state.set(State.NOT_STARTED);
    }
}
