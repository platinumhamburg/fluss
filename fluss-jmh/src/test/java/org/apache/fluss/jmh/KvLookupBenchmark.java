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

package org.apache.fluss.jmh;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.server.kv.KvFlushScheduler;
import org.apache.fluss.server.kv.KvManager;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.kv.autoinc.AutoIncrementManager;
import org.apache.fluss.server.kv.autoinc.TestingSequenceGeneratorFactory;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.server.kv.rowmerger.RowMerger;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.LogTestUtils;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.utils.ByteArraySlice;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import io.github.fluss_contrib.rocksdb.FlushOptions;
import io.github.fluss_contrib.rocksdb.HyperClockCache;
import io.github.fluss_contrib.rocksdb.RocksDB;
import io.github.fluss_contrib.rocksdb.RocksDBException;
import io.github.fluss_contrib.rocksdb.TickerType;
import io.github.fluss_contrib.rocksdb.WriteBatch;
import io.github.fluss_contrib.rocksdb.WriteOptions;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.ThreadParams;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH_PK;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Measures the actual tablet lookup path alone and against a native writer using the tablet's
 * guarded executor. Writes isolate the KV storage critical section; they do not benchmark WAL or
 * RPC processing. Cache misses here can hit the OS page cache and are not cold-disk reads.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 8, time = 2)
@Fork(5)
public class KvLookupBenchmark {

    /** A real tablet and SST dataset shared by readers and the writer. */
    @State(Scope.Benchmark)
    public static class Store {
        @Param({"1", "32", "256", "1024"})
        public int batchSize;

        @Param({"4", "128"})
        public int cacheMiB;

        @Param({"262144"})
        public int keyCount;

        @Param({"256"})
        public int valueSize;

        private Path directory;
        private RootAllocator allocator;
        private HyperClockCache cache;
        private KvFlushScheduler scheduler;
        private KvTablet tablet;
        private LogTablet log;
        private FlussScheduler logScheduler;
        private RocksDBKv rocks;
        private byte[][] keys;
        private byte[][] writeValues;
        private Executor writer;
        private long hits;
        private long misses;

        /** Creates and warms a deterministic dataset outside the measurement. */
        @Setup(Level.Trial)
        public void setup() throws Exception {
            try {
                setupStore();
            } catch (Throwable t) {
                try {
                    close();
                } catch (Throwable closeFailure) {
                    t.addSuppressed(closeFailure);
                }
                throw t;
            }
        }

        private void setupStore() throws Exception {
            checkState(Integer.bitCount(keyCount) == 1, "keyCount must be a power of two");
            RocksDB.loadLibrary();
            directory = Files.createTempDirectory("fluss-lookup-jmh-");
            Configuration conf = new Configuration();
            conf.set(ConfigOptions.KV_WRITE_BUFFER_SIZE, MemorySize.ofMebiBytes(16));
            conf.set(ConfigOptions.KV_CACHE_INDEX_AND_FILTER_BLOCKS, true);
            conf.set(ConfigOptions.KV_PARTITIONED_INDEX_FILTERS, true);
            allocator = new RootAllocator(Long.MAX_VALUE);
            cache = new HyperClockCache(cacheMiB * 1024L * 1024L, 0, -1, false);
            scheduler = new KvFlushScheduler(conf);
            TestingSchemaGetter schemas =
                    new TestingSchemaGetter(new SchemaInfo(DATA1_SCHEMA_PK, 1));
            TableConfig tableConfig = new TableConfig(new Configuration());
            logScheduler = new FlussScheduler(1);
            log =
                    LogTablet.create(
                            directory.toFile(),
                            DATA1_PHYSICAL_TABLE_PATH_PK,
                            LogTestUtils.makeRandomLogTabletDir(
                                    directory.toFile(), "lookup", DATA1_TABLE_ID_PK, "benchmark"),
                            conf,
                            new AtomicBoolean(false),
                            TestingMetricGroups.TABLET_SERVER_METRICS,
                            0,
                            logScheduler,
                            LogFormat.ARROW,
                            1,
                            true,
                            SystemClock.getInstance(),
                            true);
            tablet =
                    KvTablet.create(
                            DATA1_PHYSICAL_TABLE_PATH_PK,
                            new TableBucket(DATA1_TABLE_ID_PK, 1),
                            log,
                            directory.toFile(),
                            conf,
                            TestingMetricGroups.TABLET_SERVER_METRICS,
                            allocator,
                            new TestingMemorySegmentPool(10 * 1024),
                            KvFormat.COMPACTED,
                            RowMerger.create(tableConfig, KvFormat.COMPACTED, schemas),
                            DEFAULT_COMPRESSION,
                            schemas,
                            tableConfig.getChangelogImage(),
                            KvManager.getDefaultRateLimiter(),
                            cache,
                            scheduler,
                            null,
                            new AutoIncrementManager(
                                    schemas,
                                    DATA1_PHYSICAL_TABLE_PATH_PK.getTablePath(),
                                    tableConfig,
                                    new TestingSequenceGeneratorFactory()),
                            SystemClock.getInstance(),
                            tableConfig);
            rocks = tablet.getRocksDBKv();
            writer = tablet.getGuardedExecutor();
            keys = new byte[keyCount][];
            writeValues = new byte[1024][];
            Random random = new Random(4749001L);
            try (WriteOptions options = new WriteOptions().setDisableWAL(true);
                    WriteBatch batch = new WriteBatch()) {
                for (int i = 0; i < keyCount; i++) {
                    byte[] key = new byte[16];
                    for (int j = 0; j < 4; j++) {
                        key[j] = (byte) (i >>> (24 - j * 8));
                    }
                    keys[i] = key;
                    byte[] value = new byte[valueSize];
                    random.nextBytes(value);
                    if (i < writeValues.length) {
                        writeValues[i] = value;
                    }
                    batch.put(key, value);
                    if ((i & 1023) == 1023) {
                        rocks.getDb().write(options, batch);
                        batch.clear();
                    }
                }
                if (batch.count() > 0) {
                    rocks.getDb().write(options, batch);
                }
            }
            try (FlushOptions options = new FlushOptions().setWaitForFlush(true)) {
                rocks.getDb().flush(options);
            }
            rocks.getDb().compactRange();
            // Warm through the actual tablet path. Small-cache runs intentionally cannot retain
            // the full dataset; subsequent random reads exercise block-cache churn.
            for (int i = 0; i < keyCount; i += 256) {
                List<byte[]> batch =
                        java.util.Arrays.asList(keys).subList(i, Math.min(i + 256, keyCount));
                checkState(tablet.multiGet(batch).size() == batch.size(), "Incomplete lookup");
            }
            checkState(
                    tablet.multiGet(Collections.singletonList(new byte[] {-1})).get(0) == null,
                    "Missing key must remain absent");
            System.out.printf(
                    "FIXTURE keys=%d value=%d cacheMiB=%d sstBytes=%d source=%s%n",
                    keyCount,
                    valueSize,
                    cacheMiB,
                    rocks.liveSstFilesSize(),
                    KvTablet.class.getProtectionDomain().getCodeSource().getLocation());
        }

        /** Captures cache counters before each iteration. */
        @Setup(Level.Iteration)
        public void startIteration() {
            hits = rocks.getStatistics().getTickerCount(TickerType.BLOCK_CACHE_DATA_HIT);
            misses = rocks.getStatistics().getTickerCount(TickerType.BLOCK_CACHE_DATA_MISS);
        }

        /** Reports cache behavior and checks that lookups released their native read views. */
        @TearDown(Level.Iteration)
        public void audit() throws Exception {
            long dataHits =
                    rocks.getStatistics().getTickerCount(TickerType.BLOCK_CACHE_DATA_HIT) - hits;
            long dataMisses =
                    rocks.getStatistics().getTickerCount(TickerType.BLOCK_CACHE_DATA_MISS) - misses;
            checkState(rocks.getResourceGuard().getLeaseCount() == 0, "Leaked resource lease");
            checkState(
                    rocks.getDb().getLongProperty("rocksdb.num-snapshots") == 0, "Leaked snapshot");
            System.out.printf("CACHE_AUDIT hits=%d misses=%d%n", dataHits, dataMisses);
        }

        /** Releases all native resources after the trial. */
        @TearDown(Level.Trial)
        public void close() throws Exception {
            IOUtils.closeAll(
                    () -> {
                        if (tablet != null) {
                            tablet.close();
                        }
                    },
                    scheduler,
                    () -> {
                        if (log != null) {
                            log.close();
                        }
                    },
                    () -> {
                        if (logScheduler != null) {
                            logScheduler.shutdown();
                        }
                    },
                    cache,
                    allocator);
            if (directory != null) {
                FileUtils.deleteDirectory(directory.toFile());
            }
        }
    }

    /** Per-thread request state, prepared without allocating a list for every lookup. */
    @State(Scope.Thread)
    public static class Requests {
        private Random random;
        private List<byte[]> batch;
        private int offset;
        private WriteBatch writeBatch;
        private WriteOptions writeOptions;
        private Runnable write;
        private long readOps;
        private long writeOps;
        private boolean measureWait;
        private long waitStarted;
        private long lockAcquired;
        private long nextWriteNanos;
        private final long[] waitNanos = new long[4096];
        private int waitCount;

        /** Prepares thread-local inputs and the native writer outside measurement. */
        @Setup(Level.Trial)
        public void setup(Store store, ThreadParams thread) {
            random = new Random(4749002L + thread.getThreadIndex());
            batch =
                    new AbstractList<byte[]>() {
                        @Override
                        public byte[] get(int index) {
                            return store.keys[(offset + index * 8191) & (store.keyCount - 1)];
                        }

                        @Override
                        public int size() {
                            return store.batchSize;
                        }
                    };
            writeBatch = new WriteBatch();
            writeOptions = new WriteOptions().setDisableWAL(true);
            write =
                    () -> {
                        if (measureWait) {
                            lockAcquired = System.nanoTime();
                        }
                        try {
                            store.rocks.getDb().write(writeOptions, writeBatch);
                        } catch (RocksDBException e) {
                            throw new IllegalStateException(e);
                        }
                    };
        }

        /** Starts operation accounting for this iteration. */
        @Setup(Level.Iteration)
        public void reset() {
            readOps = 0;
            writeOps = 0;
            measureWait = false;
            waitCount = 0;
            nextWriteNanos = 0;
        }

        /**
         * Emits completed operation counts so reader and writer progress are independently visible.
         */
        @TearDown(Level.Iteration)
        public void audit() {
            System.out.printf("OPS_AUDIT reads=%d writes=%d%n", readOps, writeOps);
            if (waitCount > 0) {
                checkState(waitCount == writeOps, "Every paced write needs a wait sample");
                long[] waits = java.util.Arrays.copyOf(waitNanos, waitCount);
                java.util.Arrays.sort(waits);
                checkState(waits[0] >= 0, "Lock wait must be nonnegative");
                long sum = 0;
                for (long wait : waits) {
                    sum += wait;
                }
                System.out.printf(
                        "WAIT_AUDIT count=%d meanNs=%.3f p50Ns=%d p99Ns=%d maxNs=%d%n",
                        waitCount,
                        (double) sum / waitCount,
                        waits[(waitCount - 1) / 2],
                        waits[(int) ((waitCount - 1) * 0.99)],
                        waits[waitCount - 1]);
            }
        }

        /** Releases the reusable writer objects. */
        @TearDown(Level.Trial)
        public void close() {
            writeBatch.close();
            writeOptions.close();
        }
    }

    /** Measures the explicit read-view lifecycle without reading data. */
    @Benchmark
    public void snapshotLifecycle(Store store) throws Exception {
        try (RocksDBKv.MultiGetLookup ignored = store.rocks.openMultiGet()) {}
    }

    /** Measures lookup without a competing writer. */
    @Benchmark
    public List<ByteArraySlice> readOnly(Store store, Requests requests) throws Exception {
        requests.offset = requests.random.nextInt(store.keyCount);
        List<ByteArraySlice> values = store.tablet.multiGet(requests.batch);
        requests.readOps++;
        return values;
    }

    /** Measures lookup while a writer competes for the same tablet's KV lock. */
    @Benchmark
    @Group("mixed")
    @GroupThreads(7)
    public List<ByteArraySlice> mixedRead(Store store, Requests requests) throws Exception {
        return readOnly(store, requests);
    }

    /** Writes 32 keys atomically under the real tablet write lock. */
    @Benchmark
    @Group("mixed")
    @GroupThreads(1)
    public void mixedWrite(Store store, Requests requests) throws Exception {
        int offset = requests.random.nextInt(store.keyCount);
        requests.writeBatch.clear();
        for (int i = 0; i < 32; i++) {
            requests.writeBatch.put(
                    store.keys[(offset + i * 8191) & (store.keyCount - 1)],
                    store.writeValues[(offset + i) & 1023]);
        }
        if (requests.measureWait) {
            requests.waitStarted = System.nanoTime();
        }
        store.writer.execute(requests.write);
        if (requests.measureWait && requests.waitCount < requests.waitNanos.length) {
            requests.waitNanos[requests.waitCount++] = requests.lockAcquired - requests.waitStarted;
        }
        requests.writeOps++;
    }

    /** Measures lookup against a writer targeting 100 native write batches per second. */
    @Benchmark
    @Group("pacedMixed")
    @GroupThreads(7)
    public List<ByteArraySlice> pacedRead(Store store, Requests requests) throws Exception {
        return readOnly(store, requests);
    }

    /**
     * Paces writes outside the measured lock-wait interval. JMH latency for this method includes
     * pacing; WAIT_AUDIT reports the separate time spent acquiring the tablet write lock.
     */
    @Benchmark
    @Group("pacedMixed")
    @GroupThreads(1)
    public void pacedWrite(Store store, Requests requests) throws Exception {
        long period = TimeUnit.MILLISECONDS.toNanos(10);
        long now = System.nanoTime();
        if (requests.nextWriteNanos == 0 || now - requests.nextWriteNanos > period) {
            requests.nextWriteNanos = now;
        }
        long remaining;
        while ((remaining = requests.nextWriteNanos - System.nanoTime()) > 0) {
            LockSupport.parkNanos(remaining);
        }
        requests.nextWriteNanos += period;
        requests.measureWait = true;
        mixedWrite(store, requests);
    }
}
