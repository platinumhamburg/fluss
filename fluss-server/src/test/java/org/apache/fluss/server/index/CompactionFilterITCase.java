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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PartitionTombstone;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.server.kv.KvManager;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.kv.autoinc.AutoIncrementManager;
import org.apache.fluss.server.kv.autoinc.TestingSequenceGeneratorFactory;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.server.kv.rowmerger.RowMerger;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.LogTestUtils;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.utils.UnsafeUtils;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDB;

import java.io.File;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToLongFunction;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for the {@link FloorSetCompactionFilterFactory} (backed by {@code
 * FloorSetCompactionFilter} from the RocksDB fork).
 *
 * <p>Creates a KvTablet with a {@link FloorSetCompactionFilterFactory} installed, writes raw
 * key-value pairs with v3 value format (schemaId + tag) directly to RocksDB, flushes to SST,
 * updates the tombstone state, triggers manual compaction, and verifies that entries for tombstoned
 * partitions are physically removed while entries for live partitions survive.
 *
 * <p>The full pipeline tested: Java → JNI (tagOffset + floor + explicitSet) → C++ {@code
 * FloorSetCompactionFilter} → RocksDB compaction.
 */
class CompactionFilterITCase {

    private static final short SCHEMA_ID = 1;
    private static final int TEST_PID_POS = 0;

    @TempDir File tempLogDir;
    @TempDir File tempKvDir;

    @Test
    void testCompactionRemovesTombstonedPartitionEntries() throws Exception {
        RocksDB.loadLibrary();

        AtomicReference<PartitionTombstone> tombstoneRef =
                new AtomicReference<>(PartitionTombstone.EMPTY);

        FloorSetCompactionFilterFactory filterFactory =
                new FloorSetCompactionFilterFactory(tombstoneRef::get);

        TablePath tablePath = TablePath.of("test_db", "idx_test");
        PhysicalTablePath physicalTablePath = PhysicalTablePath.of(tablePath);
        Schema schema = DATA1_SCHEMA_PK;
        Configuration conf = new Configuration();

        File logTabletDir =
                LogTestUtils.makeRandomLogTabletDir(tempLogDir, "test_db", 1L, "idx_test");
        LogTablet logTablet =
                LogTablet.create(
                        tempLogDir,
                        physicalTablePath,
                        logTabletDir,
                        conf,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        0,
                        new FlussScheduler(1),
                        LogFormat.ARROW,
                        1,
                        true,
                        SystemClock.getInstance(),
                        true);

        TableConfig tableConf = new TableConfig(new Configuration());
        TestingSchemaGetter schemaGetter =
                new TestingSchemaGetter(new SchemaInfo(schema, SCHEMA_ID));
        RowMerger rowMerger = RowMerger.create(tableConf, KvFormat.COMPACTED, schemaGetter);
        AutoIncrementManager autoIncrementManager =
                new AutoIncrementManager(
                        schemaGetter, tablePath, tableConf, new TestingSequenceGeneratorFactory());

        KvTablet kvTablet =
                KvTablet.create(
                        physicalTablePath,
                        logTablet.getTableBucket(),
                        logTablet,
                        tempKvDir,
                        conf,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        new RootAllocator(Long.MAX_VALUE),
                        new TestingMemorySegmentPool(10 * 1024),
                        KvFormat.COMPACTED,
                        rowMerger,
                        DEFAULT_COMPRESSION,
                        schemaGetter,
                        tableConf.getChangelogImage(),
                        tableConf.getKvFormatVersion().orElse(ConfigOptions.KV_FORMAT_VERSION_2),
                        KvManager.getDefaultRateLimiter(),
                        autoIncrementManager,
                        filterFactory,
                        (ToLongFunction<BinaryRow>) null);

        try {
            RocksDBKv rocksDBKv = kvTablet.getRocksDBKv();

            // -- Phase 1: write entries with two different partition IDs directly to RocksDB. --
            final long livePartitionId = 1000L;
            final long droppedPartitionId = 2000L;

            byte[] keyLive = new byte[] {0x01, 0x02, 0x03};
            byte[] keyDropped = new byte[] {0x04, 0x05, 0x06};

            byte[] valueLive = buildIndexValue(SCHEMA_ID, livePartitionId);
            byte[] valueDropped = buildIndexValue(SCHEMA_ID, droppedPartitionId);

            rocksDBKv.put(keyLive, valueLive);
            rocksDBKv.put(keyDropped, valueDropped);

            assertThat(rocksDBKv.get(keyLive)).isNotNull();
            assertThat(rocksDBKv.get(keyDropped)).isNotNull();

            // -- Phase 2: flush to SST so entries are eligible for compaction. --
            kvTablet.flush(Long.MAX_VALUE, NOPErrorHandler.INSTANCE);

            // -- Phase 3: update tombstone to mark droppedPartitionId as tombstoned. --
            tombstoneRef.set(
                    new PartitionTombstone(-1L, Collections.singleton(droppedPartitionId), 1L));

            // -- Phase 4: trigger manual compaction → native filter removes tombstoned entries. --
            rocksDBKv.getDb().compactRange();

            // -- Phase 5: verify. --
            assertThat(rocksDBKv.get(keyLive))
                    .as("Entry for live partition must survive compaction")
                    .isNotNull();

            assertThat(rocksDBKv.get(keyDropped))
                    .as("Entry for tombstoned partition must be physically removed")
                    .isNull();

            // -- Phase 6: verify floor-based removal works too. --
            // Write another entry with a partition ID that will fall below the floor.
            final long oldPartitionId = 500L;
            byte[] keyOld = new byte[] {0x07, 0x08, 0x09};
            byte[] valueOld = buildIndexValue(SCHEMA_ID, oldPartitionId);
            rocksDBKv.put(keyOld, valueOld);
            kvTablet.flush(Long.MAX_VALUE, NOPErrorHandler.INSTANCE);

            tombstoneRef.set(new PartitionTombstone(999L, Collections.<Long>emptySet(), 2L));

            rocksDBKv.getDb().compactRange();

            assertThat(rocksDBKv.get(keyOld))
                    .as("Entry with partition ID <= floor must be removed")
                    .isNull();
            assertThat(rocksDBKv.get(keyLive))
                    .as("Entry with partition ID > floor must survive")
                    .isNotNull();
        } finally {
            kvTablet.close();
            logTablet.close();
        }
    }

    /**
     * Builds a value in v3 format: schemaId (2 bytes, native endian) + tag (8 bytes, native endian)
     * + dummy payload. The FloorSetCompactionFilter reads the tag at offset 2.
     */
    private static byte[] buildIndexValue(short schemaId, long partitionId) {
        // v3 format: [schemaId(2)][tag(8)][payload]
        // payload can be anything - the compaction filter only cares about the tag
        byte[] dummyPayload = new byte[] {0x00, 0x00, 0x00, 0x00};
        byte[] result =
                new byte
                        [ValueEncoder.SCHEMA_ID_LENGTH
                                + ValueEncoder.TAG_LENGTH
                                + dummyPayload.length];
        UnsafeUtils.putShort(result, 0, schemaId);
        UnsafeUtils.putLong(result, ValueEncoder.TAG_OFFSET, partitionId);
        System.arraycopy(
                dummyPayload,
                0,
                result,
                ValueEncoder.SCHEMA_ID_LENGTH + ValueEncoder.TAG_LENGTH,
                dummyPayload.length);
        return result;
    }
}
