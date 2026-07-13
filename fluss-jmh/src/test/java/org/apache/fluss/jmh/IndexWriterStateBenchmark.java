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

import org.apache.fluss.metadata.KvIdempotenceProtocol;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.WriterKey;
import org.apache.fluss.server.log.FencedWriterAppendInfo;
import org.apache.fluss.server.log.LogOffsetMetadata;
import org.apache.fluss.server.log.WriterAppendInfo;
import org.apache.fluss.server.log.WriterStateEntry;
import org.apache.fluss.server.log.WriterStateManager;
import org.apache.fluss.utils.FileUtils;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
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

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Capacity curves for the existing V0 and offset-fenced V1 WriterState representations. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Fork(
        value = 1,
        jvmArgsAppend = {"-Xms512m", "-Xmx4g"})
public class IndexWriterStateBenchmark {

    enum WriterKeyUse {
        RETAINED_STATE,
        INCOMING_REQUEST
    }

    interface WriterKeyObserver {
        void onHeapBaselineCaptured();

        void onWriterKeyCreated(WriterKeyUse use, WriterKey writerKey);
    }

    @Benchmark
    public long freshAppendValidation(TopologyState state) {
        WriterStateManager manager = state.nextFreshManager();
        int writer = state.nextFreshWriter();
        if (state.protocol == KvIdempotenceProtocol.V0_COMPACT) {
            WriterAppendInfo update = manager.prepareUpdate(writer);
            update.appendDataBatch(
                    5, new LogOffsetMetadata(5L), 5L, false, true, state.stateTimestampMs);
            return update.toEntry().lastBatchSequence();
        }
        FencedWriterAppendInfo update =
                manager.prepareFencedUpdate(
                        state.newWriterKey(writer, WriterKeyUse.INCOMING_REQUEST));
        update.append(1L, 1L, state.stateTimestampMs);
        return update.updatedEntry().lastSequence();
    }

    @Benchmark
    public long snapshotSerialization(TopologyState state) throws Exception {
        WriterStateManager manager = state.nextSnapshotManager();
        manager.updateMapEndOffset(state.nextSnapshotOffset());
        manager.takeSnapshot();
        return manager.latestSnapshotBytes();
    }

    @Benchmark
    public long snapshotReload(TopologyState state) throws Exception {
        int bucket = state.nextReloadBucket();
        WriterStateManager reloaded =
                new WriterStateManager(
                        new TableBucket(1L, bucket),
                        state.managerDirs[bucket],
                        Integer.MAX_VALUE,
                        state.protocol);
        reloaded.truncateAndReload(0L, 1L, state.stateTimestampMs);
        long reloadedWriters = reloaded.writerIdCount();
        if (reloadedWriters != state.sourceWriters) {
            throw new AssertionError(
                    String.format(
                            "%s reload for bucket %d retained %d writers, expected %d",
                            state.protocol, bucket, reloadedWriters, state.sourceWriters));
        }
        return reloadedWriters;
    }

    @Benchmark
    public long staleFenceLookup(StaleState state) {
        WriterStateManager manager = state.nextStaleManager();
        int writer = state.nextStaleWriter();
        return manager.findStaleFencedBatch(
                        state.newWriterKey(writer, WriterKeyUse.INCOMING_REQUEST), 0L)
                .orElseThrow(AssertionError::new)
                .lastSequence();
    }

    abstract static class BaseState {
        protected int sourceWriters;
        protected int targetBuckets;
        protected KvIdempotenceProtocol protocol;
        protected WriterStateManager[] managers;
        protected File[] managerDirs;
        protected long stateTimestampMs;
        private File rootDir;
        private int freshManagerCursor;
        private int freshWriterCursor;
        private int snapshotManagerCursor;
        private int reloadBucketCursor;
        private int staleManagerCursor;
        private int staleWriterCursor;
        private final AtomicLong snapshotOffset = new AtomicLong(1L);
        private WriterKeyObserver writerKeyObserver;

        protected void initialize(
                String protocolName, int configuredSourceWriters, int configuredTargetBuckets)
                throws Exception {
            protocol = KvIdempotenceProtocol.valueOf(protocolName);
            sourceWriters = configuredSourceWriters;
            targetBuckets = configuredTargetBuckets;
            rootDir = Files.createTempDirectory("index-writer-state-jmh").toFile();
            managerDirs = new File[targetBuckets];
            managers = new WriterStateManager[targetBuckets];
            stateTimestampMs = System.currentTimeMillis();

            long heapBefore = captureHeapBaseline();
            for (int bucket = 0; bucket < targetBuckets; bucket++) {
                File managerDir = new File(rootDir, "bucket-" + bucket);
                Files.createDirectories(managerDir.toPath());
                managerDirs[bucket] = managerDir;
                WriterStateManager manager =
                        new WriterStateManager(
                                new TableBucket(1L, bucket),
                                managerDir,
                                Integer.MAX_VALUE,
                                protocol);
                populate(manager);
                manager.updateMapEndOffset(1L);
                manager.takeSnapshot();
                managers[bucket] = manager;
            }
            assertProductionRepresentation();
            assertAndResetFreshTraversalCoverage();
            // Process-level retained-state estimate: manager maps, target-local WriterKeys, and
            // their entry graphs are allocated between two forced-GC readings. This is
            // reproducible per fork but is not a heap-dump dominator measurement, so setup noise
            // is reported as a limitation with the benchmark results.
            forceGc();
            long retainedHeap = Math.max(0L, usedHeap() - heapBefore);
            long snapshotBytes = 0L;
            for (WriterStateManager manager : managers) {
                snapshotBytes += manager.latestSnapshotBytes();
            }
            System.out.printf(
                    "CAPACITY_STATE protocol=%s sourceWriters=%d targetBuckets=%d entries=%d retainedHeapBytes=%d snapshotBytes=%d%n",
                    protocol,
                    sourceWriters,
                    targetBuckets,
                    (long) sourceWriters * targetBuckets,
                    retainedHeap,
                    snapshotBytes);
        }

        private void populate(WriterStateManager manager) {
            if (protocol == KvIdempotenceProtocol.V0_COMPACT) {
                for (int writer = 0; writer < sourceWriters; writer++) {
                    WriterStateEntry entry = WriterStateEntry.empty(writer);
                    for (int sequence = 0; sequence < 5; sequence++) {
                        entry.addBath(sequence, sequence, 0, stateTimestampMs);
                    }
                    manager.loadWriterEntry(entry);
                }
            } else {
                for (int writer = 0; writer < sourceWriters; writer++) {
                    WriterKey writerKey = newWriterKey(writer, WriterKeyUse.RETAINED_STATE);
                    FencedWriterAppendInfo update = manager.prepareFencedUpdate(writerKey);
                    update.append(0L, 0L, stateTimestampMs);
                    manager.updateFenced(update);
                }
            }
        }

        private void assertProductionRepresentation() throws Exception {
            for (WriterStateManager manager : managers) {
                if (manager.writerIdCount() != sourceWriters) {
                    throw new AssertionError(
                            String.format(
                                    "%s setup retained %d writers, expected %d",
                                    protocol, manager.writerIdCount(), sourceWriters));
                }
            }
            if (protocol == KvIdempotenceProtocol.V0_COMPACT) {
                Field writers = WriterStateManager.class.getDeclaredField("writers");
                if (!writers.getGenericType().getTypeName().contains("java.lang.Long")) {
                    throw new AssertionError("V0 WriterState no longer uses Map<Long,...>");
                }
                writers.setAccessible(true);
                if (!(writers.get(managers[0]) instanceof Map)) {
                    throw new AssertionError("V0 WriterState map representation is missing");
                }
                String snapshot =
                        Files.readString(managers[0].fetchSnapshot(1L).orElseThrow().toPath());
                if (!snapshot.contains("writer_id_entries")
                        || snapshot.contains("kv_idempotence_protocol_version")) {
                    throw new AssertionError("V0 snapshot representation changed");
                }
            }
        }

        protected WriterStateManager nextFreshManager() {
            return managers[nextFreshBucket()];
        }

        protected int nextFreshWriter() {
            return Math.floorMod(freshWriterCursor++, sourceWriters);
        }

        protected WriterStateManager nextSnapshotManager() {
            return managers[Math.floorMod(snapshotManagerCursor++, targetBuckets)];
        }

        protected int nextReloadBucket() {
            return Math.floorMod(reloadBucketCursor++, targetBuckets);
        }

        protected WriterStateManager nextStaleManager() {
            return managers[Math.floorMod(staleManagerCursor++, targetBuckets)];
        }

        protected int nextStaleWriter() {
            return Math.floorMod(staleWriterCursor++, sourceWriters);
        }

        protected void assertAndResetFreshTraversalCoverage() {
            boolean[] managersVisited = new boolean[targetBuckets];
            boolean[] writersVisited = new boolean[sourceWriters];
            int validationInvocations = Math.max(targetBuckets, sourceWriters);
            for (int invocation = 0; invocation < validationInvocations; invocation++) {
                managersVisited[nextFreshBucket()] = true;
                writersVisited[nextFreshWriter()] = true;
            }
            for (int bucket = 0; bucket < targetBuckets; bucket++) {
                if (!managersVisited[bucket]) {
                    throw new AssertionError("fresh traversal misses target bucket " + bucket);
                }
            }
            for (int writer = 0; writer < sourceWriters; writer++) {
                if (!writersVisited[writer]) {
                    throw new AssertionError("fresh traversal misses source writer " + writer);
                }
            }
            freshManagerCursor = 0;
            freshWriterCursor = 0;
        }

        private int nextFreshBucket() {
            return Math.floorMod(freshManagerCursor++, targetBuckets);
        }

        protected long nextSnapshotOffset() {
            return snapshotOffset.incrementAndGet();
        }

        void setWriterKeyObserver(WriterKeyObserver writerKeyObserver) {
            this.writerKeyObserver = writerKeyObserver;
        }

        WriterKey newWriterKey(int writer, WriterKeyUse use) {
            WriterKey writerKey = new WriterKey(0L, writer);
            WriterKeyObserver observer = writerKeyObserver;
            if (observer != null) {
                observer.onWriterKeyCreated(use, writerKey);
            }
            return writerKey;
        }

        @TearDown(Level.Trial)
        public void tearDown() throws Exception {
            if (rootDir != null) {
                FileUtils.deleteDirectory(rootDir);
            }
        }

        private static long usedHeap() {
            MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
            return memory.getHeapMemoryUsage().getUsed();
        }

        private static void forceGc() {
            System.gc();
            System.runFinalization();
            System.gc();
        }

        private long captureHeapBaseline() {
            forceGc();
            long heapBefore = usedHeap();
            WriterKeyObserver observer = writerKeyObserver;
            if (observer != null) {
                observer.onHeapBaselineCaptured();
            }
            return heapBefore;
        }
    }

    /** State shared by the three protocol-comparison benchmark methods. */
    @State(Scope.Benchmark)
    public static class TopologyState extends BaseState {
        @Param({"V0_COMPACT", "V1_FENCED"})
        public String protocolName;

        @Param({"64", "1024", "16384"})
        public int configuredSourceWriters;

        @Param({"1", "16", "128"})
        public int configuredTargetBuckets;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            initialize(protocolName, configuredSourceWriters, configuredTargetBuckets);
        }
    }

    /** V1-only state for stale fence lookup measurements. */
    @State(Scope.Benchmark)
    public static class StaleState extends BaseState {
        @Param({"64", "1024", "16384"})
        public int configuredSourceWriters;

        @Param({"1", "16", "128"})
        public int configuredTargetBuckets;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            initialize("V1_FENCED", configuredSourceWriters, configuredTargetBuckets);
        }
    }
}
