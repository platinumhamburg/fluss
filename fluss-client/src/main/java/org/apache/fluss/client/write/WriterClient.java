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

package org.apache.fluss.client.write;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.metrics.WriterMetricGroup;
import org.apache.fluss.client.write.RecordAccumulator.RecordAppendResult;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.IllegalConfigurationException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TableOrPartition;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;
import org.apache.fluss.utils.AutoPartitionStrategy;
import org.apache.fluss.utils.CopyOnWriteMap;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.EOFException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.config.ConfigOptions.NoKeyAssigner.ROUND_ROBIN;
import static org.apache.fluss.config.ConfigOptions.NoKeyAssigner.STICKY;
import static org.apache.fluss.utils.ExceptionUtils.toException;
import static org.apache.fluss.utils.PartitionUtils.HISTORICAL_PARTITION_VALUE;
import static org.apache.fluss.utils.PartitionUtils.generateAutoPartitionTime;

/**
 * A client that write records to server.
 *
 * <p>The writer consists of a pool of buffer space that holds records that haven't yet been
 * transmitted to the tablet server as well as a background I/O thread that is responsible for
 * turning these records into requests and transmitting them to the cluster. Failure to close the
 * {@link WriterClient} after use will leak these resources.
 *
 * <p>The send method does not wait for acknowledgment. It may block while waiting for buffer
 * capacity, or for partition routing when a record cannot be buffered within the pending budget.
 * Buffered records are routed and sent in the background without requiring another send or flush.
 */
@ThreadSafe
@Internal
public class WriterClient {
    private static final Logger LOG = LoggerFactory.getLogger(WriterClient.class);

    public static final String SENDER_THREAD_PREFIX = "fluss-write-sender";
    /**
     * {@link ConfigOptions#CLIENT_WRITER_MAX_INFLIGHT_REQUESTS_PER_BUCKET} should be less than or
     * equal to this value when idempotence producer enabled to ensure message ordering.
     */
    private static final int MAX_IN_FLIGHT_REQUESTS_PER_BUCKET_FOR_IDEMPOTENCE = 5;

    private final Configuration conf;
    private final int maxRequestSize;
    private final RecordAccumulator accumulator;
    private final Sender sender;
    private final ExecutorService ioThreadPool;
    private final MetadataUpdater metadataUpdater;
    private final Map<TableOrPartition, BucketAssigner> bucketAssigners = new CopyOnWriteMap<>();
    private final IdempotenceManager idempotenceManager;
    private final WriterMetricGroup writerMetricGroup;
    private final DynamicPartitionCreator dynamicPartitionCreator;

    private final Object pendingLock = new Object();
    private final Map<PhysicalTablePath, PendingPartition> pendingPartitions = new HashMap<>();
    private final TreeMap<Long, AcceptedWrite> acceptedWrites = new TreeMap<>();
    private final ExecutorService transferExecutor;
    private final long pendingCapacity;
    // All admission, queue ownership and completion bookkeeping is guarded by pendingLock.
    private long pendingBytes;
    private long nextSequence;
    private boolean closed;
    private Exception failure;

    public WriterClient(
            Configuration conf,
            MetadataUpdater metadataUpdater,
            ClientMetricGroup clientMetricGroup,
            Admin admin) {
        this(conf, metadataUpdater, new WriterMetricGroup(clientMetricGroup), admin);
    }

    public WriterClient(
            Configuration conf,
            MetadataUpdater metadataUpdater,
            WriterMetricGroup writerMetricGroup,
            Admin admin) {
        int maxRequestSizeLocal = -1;
        IdempotenceManager idempotenceManagerLocal = null;
        try {
            this.conf = conf;
            this.metadataUpdater = metadataUpdater;
            this.pendingCapacity =
                    conf.get(ConfigOptions.CLIENT_WRITER_PENDING_BUFFER_MEMORY_SIZE).getBytes();
            if (pendingCapacity < 256) {
                throw new IllegalConfigurationException(
                        "Pending buffer must hold at least 256 bytes.");
            }
            this.transferExecutor =
                    Executors.newSingleThreadExecutor(
                            new ExecutorThreadFactory("fluss-write-partition-transfer"));
            maxRequestSizeLocal =
                    (int) conf.get(ConfigOptions.CLIENT_WRITER_REQUEST_MAX_SIZE).getBytes();
            this.maxRequestSize = maxRequestSizeLocal;
            this.writerMetricGroup = writerMetricGroup;
            idempotenceManagerLocal = buildIdempotenceManager();
            this.idempotenceManager = idempotenceManagerLocal;

            short acks = configureAcks(idempotenceManager.idempotenceEnabled());
            int retries = configureRetries(idempotenceManager.idempotenceEnabled());
            this.accumulator =
                    new RecordAccumulator(
                            conf, idempotenceManager, writerMetricGroup, SystemClock.getInstance());
            this.sender = newSender(acks, retries);
            this.ioThreadPool = createThreadPool();
            ioThreadPool.submit(sender);

            this.dynamicPartitionCreator =
                    new DynamicPartitionCreator(
                            metadataUpdater,
                            admin,
                            conf.get(ConfigOptions.CLIENT_WRITER_DYNAMIC_CREATE_PARTITION_ENABLED),
                            this::maybeAbortBatches,
                            conf.get(ConfigOptions.CLIENT_REQUEST_TIMEOUT).toMillis());
        } catch (Throwable t) {
            LOG.error("Failed to construct writer.", t);
            close(Duration.ofMillis(0));
            throw new FlussRuntimeException(
                    String.format(
                            "Failed to construct writer. Max request size: %d bytes, Idempotence enabled: %b",
                            maxRequestSizeLocal,
                            idempotenceManagerLocal != null
                                    && idempotenceManagerLocal.idempotenceEnabled()),
                    t);
        }
    }

    /**
     * Asynchronously send a record to a table and invoke the provided callback when to send has
     * been acknowledged.
     */
    public void send(WriteRecord record, WriteCallback callback) {
        doSend(record, callback);
    }

    /**
     * Invoking this method makes all buffered records immediately available to send (even if <code>
     * linger.ms</code> is greater than 0) and blocks on the completion of the requests associated
     * with these records. The post-condition of <code>flush()</code> is that any previously sent
     * record will have completed (e.g. <code>Future.isDone() == true</code>). A request is
     * considered completed when it is successfully acknowledged according to the <code>acks</code>
     * configuration you have specified or else it results in an error.
     *
     * <p>Other threads can continue sending records while one thread is blocked waiting for a flush
     * call to complete, however no guarantee is made about the completion of records sent after the
     * flush call begins.
     */
    public void flush() {
        LOG.trace("Flushing accumulated records in writer.");
        long start = System.currentTimeMillis();
        accumulator.beginFlush();
        sender.wakeup();
        try {
            synchronized (pendingLock) {
                long cutoff = nextSequence;
                while (!acceptedWrites.isEmpty() && acceptedWrites.firstKey() <= cutoff) {
                    pendingLock.wait();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlussRuntimeException("Interrupted while flushing writer", e);
        } finally {
            accumulator.endFlush();
        }
        LOG.trace(
                "Flushed accumulated records in writer in {} ms.",
                System.currentTimeMillis() - start);
    }

    private void doSend(WriteRecord record, WriteCallback callback) {
        try {
            synchronized (pendingLock) {
                throwIfWriterClosed();
            }

            TableInfo tableInfo = record.getTableInfo();
            PhysicalTablePath physicalTablePath = record.getPhysicalTablePath();
            // The path the record is physically written to. A retired partition's records land in
            // the historical partition, whose own bucket count must drive the assignment.
            PhysicalTablePath routingPath = physicalTablePath;
            if (tableInfo.isPartitioned()) {
                boolean historicalPartitionEnabled =
                        accumulator.checkAndCacheHistoricalPartitionEnabled(tableInfo);
                if (historicalPartitionEnabled
                        && mayBeExpiredHistoricalPartition(
                                physicalTablePath, tableInfo, Instant.now())) {
                    routingPath = resolveHistoricalWriteTarget(physicalTablePath);
                }
            }
            appendOrWait(record, callback, routingPath);
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Failed to send record to table %s. Writer state: %s",
                            record.getPhysicalTablePath(),
                            sender != null && sender.isRunning() ? "running" : "closed"),
                    e);
        }
    }

    private boolean isBucketCountKnown(
            TableInfo tableInfo, PhysicalTablePath path, Cluster snapshot) {
        if (!tableInfo.isPartitioned()) {
            return true;
        }
        return snapshot.getTableId(path.getTablePath())
                        .filter(id -> id == tableInfo.getTableId())
                        .isPresent()
                && snapshot.getPartitionId(path)
                        .flatMap(id -> snapshot.getBucketCount(TableOrPartition.ofPartition(id)))
                        .filter(count -> count > 0)
                        .isPresent();
    }

    private CompletableFuture<Cluster> resolvePartition(TableInfo info, PhysicalTablePath path) {
        return FutureUtils.orTimeout(
                dynamicPartitionCreator
                        .checkAndCreatePartitionAsync(path, info)
                        .thenCompose(ignored -> metadataUpdater.ensurePartitionMetadataAsync(path))
                        .thenApply(
                                snapshot -> {
                                    if (!isBucketCountKnown(info, path, snapshot)) {
                                        throw new FlussRuntimeException(
                                                "Partition metadata does not belong to table "
                                                        + info.getTableId()
                                                        + ": "
                                                        + path);
                                    }
                                    return snapshot;
                                }),
                conf.get(ConfigOptions.CLIENT_REQUEST_TIMEOUT).toMillis(),
                TimeUnit.MILLISECONDS,
                "Timed out resolving write partition " + path);
    }

    private void appendOrWait(WriteRecord record, WriteCallback callback, PhysicalTablePath path)
            throws Exception {
        long start = System.nanoTime();
        PendingPartition partition;
        AcceptedWrite write;
        boolean resolve;
        boolean direct;
        synchronized (pendingLock) {
            while (true) {
                throwIfWriterClosed();
                partition = pendingPartitions.get(record.getPhysicalTablePath());
                Cluster snapshot = metadataUpdater.getCluster();
                boolean ready =
                        partition == null
                                && isBucketCountKnown(record.getTableInfo(), path, snapshot);
                long size = record.pendingSizeInBytes();
                direct = ready || size > pendingCapacity;
                long charge = ready ? 0 : direct ? 256 : size;
                if (pendingBytes + charge > pendingCapacity) {
                    awaitPending(start);
                    continue;
                }
                resolve = partition == null && !ready;
                if (partition == null) {
                    partition =
                            new PendingPartition(
                                    record.getPhysicalTablePath(),
                                    path,
                                    record.getTableInfo().getTableId(),
                                    ready ? snapshot : null);
                    pendingPartitions.put(record.getPhysicalTablePath(), partition);
                } else if (partition.tableId != record.getTableInfo().getTableId()) {
                    throw new FlussRuntimeException(
                            "Table was replaced while writes were pending: " + path);
                }
                write = new AcceptedWrite(++nextSequence, callback, charge, direct);
                acceptedWrites.put(write.sequence, write);
                partition.writes.addLast(write);
                pendingBytes += charge;
                break;
            }
        }
        PendingPartition target = partition;
        try {
            if (resolve) {
                resolvePartition(record.getTableInfo(), path)
                        .whenComplete(
                                (snapshot, error) -> {
                                    if (error != null) {
                                        failPartition(target, error);
                                    } else {
                                        synchronized (pendingLock) {
                                            target.snapshot = snapshot;
                                            scheduleTransfer(target);
                                            pendingLock.notifyAll();
                                        }
                                    }
                                });
            }
            if (direct) {
                synchronized (pendingLock) {
                    while (failure == null
                            && partition.error == null
                            && (partition.snapshot == null
                                    || partition.writes.peekFirst() != write)) {
                        awaitPending(start);
                    }
                    if (failure != null) {
                        throw failure;
                    }
                    if (partition.error != null) {
                        throw partition.error;
                    }
                }
                try {
                    routeRecord(record, write, partition.snapshot, partition.routingPath);
                } finally {
                    finishTransfer(partition, write);
                }
            } else {
                WriteRecord copy = record.copy();
                synchronized (pendingLock) {
                    if (failure != null) {
                        throw failure;
                    }
                    if (partition.error == null) {
                        write.record = copy;
                        scheduleTransfer(partition);
                    }
                }
            }
        } catch (Exception error) {
            finishTransfer(partition, write);
            write.onCompletion(null, -1L, error);
            throw error;
        }
    }

    private void awaitPending(long start) throws InterruptedException, EOFException {
        long remaining =
                conf.get(ConfigOptions.CLIENT_WRITER_BUFFER_WAIT_TIMEOUT).toNanos()
                        - (System.nanoTime() - start);
        if (remaining <= 0) {
            throw new EOFException(
                    "Timed out waiting for partition routing or pending buffer space");
        }
        TimeUnit.NANOSECONDS.timedWait(pendingLock, remaining);
    }

    // Called under pendingLock. Each partition has at most one transfer task, and the head stays
    // in the queue until append has registered it in the accumulator, including memory waits.
    private void scheduleTransfer(PendingPartition partition) {
        AcceptedWrite head = partition.writes.peekFirst();
        if (failure == null
                && !partition.transferring
                && partition.snapshot != null
                && head != null
                && !head.direct
                && head.record != null) {
            partition.transferring = true;
            transferExecutor.execute(() -> transfer(partition));
        }
    }

    private void transfer(PendingPartition partition) {
        while (true) {
            AcceptedWrite write;
            WriteRecord record;
            synchronized (pendingLock) {
                write = partition.writes.peekFirst();
                if (failure != null || write == null || write.direct || write.record == null) {
                    partition.transferring = false;
                    pendingLock.notifyAll();
                    return;
                }
                record = write.record;
            }
            Exception appendError = null;
            try {
                routeRecord(record, write, partition.snapshot, partition.routingPath);
            } catch (Exception error) {
                appendError = error;
            } finally {
                finishTransfer(partition, write);
            }
            if (appendError != null) {
                write.onCompletion(null, -1L, appendError);
            }
        }
    }

    private void finishTransfer(PendingPartition partition, AcceptedWrite write) {
        synchronized (pendingLock) {
            if (partition.writes.remove(write)) {
                pendingBytes -= write.charge;
                write.record = null;
            }
            if (partition.writes.isEmpty()) {
                pendingPartitions.remove(partition.originalPath, partition);
            } else {
                scheduleTransfer(partition);
            }
            pendingLock.notifyAll();
        }
    }

    /** Owns completion continuously from admission through routing, batching and acknowledgment. */
    private final class AcceptedWrite implements WriteCallback {
        private final long sequence;
        private final WriteCallback callback;
        private final long charge;
        private final boolean direct;
        private WriteRecord record;
        private boolean completing;

        private AcceptedWrite(long sequence, WriteCallback callback, long charge, boolean direct) {
            this.sequence = sequence;
            this.callback = callback;
            this.charge = charge;
            this.direct = direct;
        }

        @Override
        public void onCompletion(TableBucket bucket, long offset, Exception error) {
            synchronized (pendingLock) {
                if (completing) {
                    return;
                }
                completing = true;
            }
            try {
                callback.onCompletion(bucket, offset, error);
            } catch (Throwable callbackError) {
                LOG.warn("Write callback failed", callbackError);
            } finally {
                synchronized (pendingLock) {
                    acceptedWrites.remove(sequence);
                    pendingLock.notifyAll();
                }
            }
        }
    }

    private final class PendingPartition {
        private final PhysicalTablePath originalPath;
        private final PhysicalTablePath routingPath;
        private final long tableId;
        private final Deque<AcceptedWrite> writes = new ArrayDeque<>();
        private Cluster snapshot;
        private Exception error;
        private boolean transferring;

        private PendingPartition(
                PhysicalTablePath originalPath,
                PhysicalTablePath routingPath,
                long tableId,
                Cluster snapshot) {
            this.originalPath = originalPath;
            this.routingPath = routingPath;
            this.tableId = tableId;
            this.snapshot = snapshot;
        }
    }

    private void routeRecord(
            WriteRecord record,
            WriteCallback callback,
            Cluster cluster,
            PhysicalTablePath routingPath)
            throws Exception {
        TableInfo tableInfo = record.getTableInfo();
        long tableId = tableInfo.getTableId();
        Long partitionId =
                tableInfo.isPartitioned() ? cluster.getPartitionIdOrElseThrow(routingPath) : null;
        int bucketCount =
                partitionId == null
                        ? tableInfo.getNumBuckets()
                        : cluster.getBucketCount(TableOrPartition.ofPartition(partitionId))
                                .orElseThrow(
                                        () ->
                                                new FlussRuntimeException(
                                                        "Bucket count missing for " + routingPath));
        BucketAssigner bucketAssigner =
                bucketAssigners.computeIfAbsent(
                        TableOrPartition.of(tableId, partitionId),
                        k -> createBucketAssigner(tableInfo, routingPath, bucketCount, conf));

        int bucketId = bucketAssigner.assignBucket(record.getBucketKey(), cluster);
        RecordAppendResult result =
                accumulator.append(
                        record,
                        callback,
                        cluster,
                        bucketId,
                        bucketCount,
                        bucketAssigner.abortIfBatchFull());

        if (result.abortRecordForNewBatch) {
            int prevBucketId = bucketId;
            bucketAssigner.onNewBatch(cluster, prevBucketId);
            bucketId = bucketAssigner.assignBucket(record.getBucketKey(), cluster);
            LOG.trace(
                    "Retrying append due to new batch creation for table {} bucket {}, the old bucket was {}.",
                    routingPath,
                    bucketId,
                    prevBucketId);
            result = accumulator.append(record, callback, cluster, bucketId, bucketCount, false);
        }

        if (result.batchIsFull || result.newBatchCreated) {
            LOG.trace(
                    "Waking up the sender since table {} bucket {} is either full or getting a new batch",
                    routingPath,
                    bucketId);
            sender.wakeup();
        }
    }

    /**
     * Returns whether a partition of a historical-partition-enabled table is old enough that it may
     * have expired under its retention policy.
     *
     * <p>This client-side precheck uses the time zone resolved from the table configuration. A
     * {@code true} result does not confirm that the partition is missing; the caller must refresh
     * metadata before routing the write to a historical partition. If the table does not explicitly
     * configure a time zone and the Client and Coordinator use different defaults, they may
     * classify partitions near the retention boundary differently. Late classification may fail a
     * write to an already removed original partition, while early classification only causes an
     * extra metadata refresh.
     */
    static boolean mayBeExpiredHistoricalPartition(
            PhysicalTablePath physicalTablePath, TableInfo tableInfo, Instant now) {
        // TODO: Move this per-record configuration and time calculation off the hot path by using
        // periodically refreshed, server-authoritative partition status; see
        // https://github.com/apache/fluss/issues/4161.
        String partitionName = physicalTablePath.getPartitionName();
        if (partitionName == null) {
            return false;
        }

        AutoPartitionStrategy strategy = tableInfo.getTableConfig().getAutoPartitionStrategy();
        if (strategy.numToRetain() < 0) {
            return false;
        }

        ZonedDateTime currentDateTime =
                ZonedDateTime.ofInstant(now, strategy.timeZone().toZoneId());
        String earliestRetainedPartition =
                generateAutoPartitionTime(
                        currentDateTime, -strategy.numToRetain(), strategy.timeUnit(), strategy);
        return partitionName.compareTo(earliestRetainedPartition) < 0;
    }

    /** Returns the path the records of this original partition are physically written to. */
    private PhysicalTablePath resolveHistoricalWriteTarget(PhysicalTablePath originalPath) {
        // Keep refreshing while the target is still the original partition so its retirement can
        // be detected before more records are appended to the stale route. Ideally, the Client
        // should learn the server-authoritative partition status without synchronously refreshing
        // metadata on the per-record path; see https://github.com/apache/fluss/issues/4161.
        if (accumulator.hasHistoricalWriteTarget(originalPath)) {
            return PhysicalTablePath.of(originalPath.getTablePath(), HISTORICAL_PARTITION_VALUE);
        }

        PhysicalTablePath targetPath = originalPath;
        // The time check only limits metadata traffic. Invalidate a potentially stale cached route
        // and authoritatively choose the target before the record enters the queue.
        metadataUpdater.invalidPhysicalTableBucketAndPartitionMeta(
                Collections.singleton(originalPath));
        try {
            if (!metadataUpdater.checkAndUpdatePartitionMetadata(originalPath)) {
                throw new FlussRuntimeException(
                        "Failed to resolve write target for " + originalPath + '.');
            }
        } catch (PartitionNotExistException ignored) {
            targetPath =
                    PhysicalTablePath.of(originalPath.getTablePath(), HISTORICAL_PARTITION_VALUE);
            // TODO: Activate this target only after the lake-aware partition retirement protocol
            // guarantees that all accepted original writes are readable from the lake; see
            // https://github.com/apache/fluss/pull/3820.
            if (!metadataUpdater.checkAndUpdatePartitionMetadata(targetPath)) {
                throw new PartitionNotExistException(
                        "Historical partition " + targetPath + " does not exist.");
            }
        }

        accumulator.routeWritesTo(
                originalPath, targetPath, metadataUpdater.getPartitionIdOrElseThrow(targetPath));
        return targetPath;
    }

    private void failPartition(PendingPartition partition, Throwable error) {
        List<AcceptedWrite> writes;
        Exception cause =
                toException(org.apache.fluss.utils.ExceptionUtils.stripCompletionException(error));
        synchronized (pendingLock) {
            partition.error = cause;
            writes = new ArrayList<>(partition.writes);
            for (AcceptedWrite write : writes) {
                write.record = null;
                pendingBytes -= write.charge;
            }
            partition.writes.clear();
            pendingPartitions.remove(partition.originalPath, partition);
            pendingLock.notifyAll();
        }
        for (AcceptedWrite write : writes) {
            write.onCompletion(null, -1L, cause);
        }
    }

    private void maybeAbortBatches(Throwable error) {
        List<AcceptedWrite> writes;
        synchronized (pendingLock) {
            if (failure != null) {
                return;
            }
            failure =
                    toException(
                            org.apache.fluss.utils.ExceptionUtils.stripCompletionException(error));
            writes = new ArrayList<>(acceptedWrites.values());
            for (PendingPartition partition : pendingPartitions.values()) {
                for (AcceptedWrite write : partition.writes) {
                    write.record = null;
                }
                partition.writes.clear();
            }
            pendingPartitions.clear();
            pendingBytes = 0;
            pendingLock.notifyAll();
        }
        if (sender != null) {
            sender.recordFatalError(failure);
        } else if (accumulator != null) {
            accumulator.close();
        }
        for (AcceptedWrite write : writes) {
            write.onCompletion(null, -1L, failure);
        }
    }

    // Verify that writer instance has not been closed. This method throws IllegalStateException if
    // writer has already been closed.
    private void throwIfWriterClosed() {
        if (failure != null) {
            throw new FlussRuntimeException("Writer failed", failure);
        }
        if (closed || sender == null || !sender.isRunning()) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot perform write operation after writer has been closed. Sender running: %b, Thread pool shutdown: %b",
                            sender != null && sender.isRunning(),
                            ioThreadPool == null || ioThreadPool.isShutdown()));
        }
    }

    private IdempotenceManager buildIdempotenceManager() {
        boolean idempotenceEnabled =
                conf.getBoolean(ConfigOptions.CLIENT_WRITER_ENABLE_IDEMPOTENCE);
        int maxInflightRequestPerBucket =
                conf.getInt(ConfigOptions.CLIENT_WRITER_MAX_INFLIGHT_REQUESTS_PER_BUCKET);
        if (idempotenceEnabled
                && maxInflightRequestPerBucket
                        > MAX_IN_FLIGHT_REQUESTS_PER_BUCKET_FOR_IDEMPOTENCE) {
            throw new IllegalConfigurationException(
                    String.format(
                            "Invalid configuration for idempotent writer. The value of %s (%d) should be less than or equal to %d when idempotence is enabled to ensure message ordering",
                            ConfigOptions.CLIENT_WRITER_MAX_INFLIGHT_REQUESTS_PER_BUCKET.key(),
                            maxInflightRequestPerBucket,
                            MAX_IN_FLIGHT_REQUESTS_PER_BUCKET_FOR_IDEMPOTENCE));
        }

        TabletServerGateway tabletServerGateway = metadataUpdater.newRandomTabletServerClient();
        return idempotenceEnabled
                ? new IdempotenceManager(
                        true, maxInflightRequestPerBucket, tabletServerGateway, metadataUpdater)
                : new IdempotenceManager(
                        false, maxInflightRequestPerBucket, tabletServerGateway, metadataUpdater);
    }

    private short configureAcks(boolean idempotenceEnabled) {
        String acks = conf.get(ConfigOptions.CLIENT_WRITER_ACKS);
        short ack;
        if (acks.equals("all")) {
            ack = Short.parseShort("-1");
        } else {
            ack = Short.parseShort(acks);
        }

        if (idempotenceEnabled && ack != -1) {
            throw new IllegalConfigurationException(
                    String.format(
                            "Invalid acks configuration for idempotent writer. Must set %s to 'all' (current value: '%s') in order to use the idempotent writer. Otherwise we cannot guarantee idempotence",
                            ConfigOptions.CLIENT_WRITER_ACKS.key(), acks));
        }

        return ack;
    }

    private int configureRetries(boolean idempotenceEnabled) {
        int retries = conf.getInt(ConfigOptions.CLIENT_WRITER_RETRIES);
        if (idempotenceEnabled && retries == 0) {
            throw new IllegalConfigurationException(
                    String.format(
                            "Invalid retries configuration for idempotent writer. Must set %s to non-zero (current value: %d) when using the idempotent writer. Otherwise we cannot guarantee idempotence",
                            ConfigOptions.CLIENT_WRITER_RETRIES.key(), retries));
        }
        return retries;
    }

    private Sender newSender(short acks, int retries) {
        return new Sender(
                accumulator,
                (int) conf.get(ConfigOptions.CLIENT_REQUEST_TIMEOUT).toMillis(),
                maxRequestSize,
                acks,
                retries,
                metadataUpdater,
                idempotenceManager,
                writerMetricGroup,
                this::maybeAbortBatches);
    }

    public void close(Duration timeout) {
        long budget = TimeUnit.MILLISECONDS.toNanos(Math.max(0, timeout.toMillis()));
        long start = System.nanoTime();
        synchronized (pendingLock) {
            if (closed) {
                return;
            }
            closed = true;
            pendingLock.notifyAll();
        }
        if (accumulator != null) {
            accumulator.beginFlush();
        }
        if (sender != null) {
            sender.wakeup();
        }
        try {
            synchronized (pendingLock) {
                while (!acceptedWrites.isEmpty()) {
                    long remaining = budget - (System.nanoTime() - start);
                    if (remaining <= 0) {
                        break;
                    }
                    TimeUnit.NANOSECONDS.timedWait(pendingLock, remaining);
                }
            }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
        } finally {
            if (accumulator != null) {
                accumulator.endFlush();
            }
        }
        boolean unfinished;
        synchronized (pendingLock) {
            unfinished = !acceptedWrites.isEmpty();
        }
        if (unfinished) {
            maybeAbortBatches(
                    new FlussRuntimeException("Writer closed before all writes completed"));
        }
        if (transferExecutor != null) {
            transferExecutor.shutdownNow();
        }
        if (sender != null) {
            sender.forceClose();
        }
        if (ioThreadPool != null) {
            ioThreadPool.shutdownNow();
            try {
                long remaining = budget - (System.nanoTime() - start);
                if (remaining > 0) {
                    ioThreadPool.awaitTermination(remaining, TimeUnit.NANOSECONDS);
                }
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
            }
        }
        if (writerMetricGroup != null) {
            writerMetricGroup.close();
        }
    }

    private ExecutorService createThreadPool() {
        return Executors.newFixedThreadPool(1, new ExecutorThreadFactory(SENDER_THREAD_PREFIX));
    }

    private BucketAssigner createBucketAssigner(
            TableInfo tableInfo,
            PhysicalTablePath physicalTablePath,
            int bucketCount,
            Configuration conf) {
        List<String> bucketKeys = tableInfo.getBucketKeys();
        if (!bucketKeys.isEmpty()) {
            BucketingFunction function =
                    BucketingFunction.of(
                            tableInfo.getTableConfig().getDataLakeFormat().orElse(null));
            return new HashBucketAssigner(bucketCount, function);
        } else {
            ConfigOptions.NoKeyAssigner noKeyAssigner =
                    conf.get(ConfigOptions.CLIENT_WRITER_BUCKET_NO_KEY_ASSIGNER);
            if (noKeyAssigner == ROUND_ROBIN) {
                return new RoundRobinBucketAssigner(physicalTablePath, bucketCount);
            } else if (noKeyAssigner == STICKY) {
                return new StickyBucketAssigner(physicalTablePath, bucketCount);
            } else {
                throw new IllegalArgumentException(
                        "Unsupported append only row bucket assigner: " + noKeyAssigner);
            }
        }
    }
}
