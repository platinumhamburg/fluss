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

package org.apache.fluss.server.kv.snapshot;

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.kv.autoinc.AutoIncIDRange;
import org.apache.fluss.utils.json.JsonSerdeTestBase;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link org.apache.fluss.server.kv.snapshot.CompletedSnapshotJsonSerde}. */
class CompletedSnapshotJsonSerdeTest extends JsonSerdeTestBase<CompletedSnapshot> {

    protected CompletedSnapshotJsonSerdeTest() {
        super(CompletedSnapshotJsonSerde.INSTANCE);
    }

    @Override
    protected CompletedSnapshot[] createObjects() {
        List<KvFileHandleAndLocalPath> sharedFileHandles =
                Arrays.asList(
                        KvFileHandleAndLocalPath.of(
                                new KvFileHandle("oss://bucket/snapshot/shared/t1.sst", 1),
                                "localPath1"),
                        KvFileHandleAndLocalPath.of(
                                new KvFileHandle("oss://bucket/snapshot/shared/t2.sst", 2),
                                "localPath2"));
        List<KvFileHandleAndLocalPath> privateFileHandles =
                Arrays.asList(
                        KvFileHandleAndLocalPath.of(
                                new KvFileHandle("oss://bucket/snapshot/snapshot1/t3", 3),
                                "localPath3"),
                        KvFileHandleAndLocalPath.of(
                                new KvFileHandle("oss://bucket/snapshot/snapshot1/t4", 4),
                                "localPath4"));
        CompletedSnapshot completedSnapshot1 =
                new CompletedSnapshot(
                        new TableBucket(1, 1),
                        1,
                        new FsPath("oss://bucket/snapshot"),
                        new KvSnapshotHandle(sharedFileHandles, privateFileHandles, 5),
                        10,
                        null,
                        null);
        CompletedSnapshot completedSnapshot2 =
                new CompletedSnapshot(
                        new TableBucket(1, 10L, 1),
                        1,
                        new FsPath("oss://bucket/snapshot"),
                        new KvSnapshotHandle(sharedFileHandles, privateFileHandles, 5),
                        10,
                        1234L,
                        Collections.singletonList(new AutoIncIDRange(2, 10000, 20000)));

        // Snapshot with index replication offsets (non-partitioned data bucket)
        Map<TableBucket, Long> indexOffsets3 = new HashMap<>();
        indexOffsets3.put(new TableBucket(1, 0), 100L);
        CompletedSnapshot completedSnapshot3 =
                new CompletedSnapshot(
                        new TableBucket(2, 0),
                        2,
                        new FsPath("oss://bucket/snapshot"),
                        new KvSnapshotHandle(sharedFileHandles, privateFileHandles, 5),
                        20,
                        null,
                        null,
                        indexOffsets3);

        // Snapshot with index replication offsets (partitioned data bucket)
        Map<TableBucket, Long> indexOffsets4 = new HashMap<>();
        indexOffsets4.put(new TableBucket(1, 10L, 0), 200L);
        CompletedSnapshot completedSnapshot4 =
                new CompletedSnapshot(
                        new TableBucket(2, 10L, 0),
                        3,
                        new FsPath("oss://bucket/snapshot"),
                        new KvSnapshotHandle(sharedFileHandles, privateFileHandles, 5),
                        30,
                        null,
                        null,
                        indexOffsets4);

        return new CompletedSnapshot[] {
            completedSnapshot1, completedSnapshot2, completedSnapshot3, completedSnapshot4
        };
    }

    @Override
    protected String[] expectedJsons() {
        String kvSnapshotHandleJson =
                "\"kv_snapshot_handle\":{"
                        + "\"shared_file_handles\":[{\"kv_file_handle\":{\"path\":\"oss://bucket/snapshot/shared/t1.sst\",\"size\":1},\"local_path\":\"localPath1\"},"
                        + "{\"kv_file_handle\":{\"path\":\"oss://bucket/snapshot/shared/t2.sst\",\"size\":2},\"local_path\":\"localPath2\"}],"
                        + "\"private_file_handles\":[{\"kv_file_handle\":{\"path\":\"oss://bucket/snapshot/snapshot1/t3\",\"size\":3},\"local_path\":\"localPath3\"},"
                        + "{\"kv_file_handle\":{\"path\":\"oss://bucket/snapshot/snapshot1/t4\",\"size\":4},\"local_path\":\"localPath4\"}],"
                        + "\"snapshot_incremental_size\":5}";
        return new String[] {
            "{\"version\":1,"
                    + "\"table_id\":1,\"bucket_id\":1,"
                    + "\"snapshot_id\":1,"
                    + "\"snapshot_location\":\"oss://bucket/snapshot\","
                    + kvSnapshotHandleJson
                    + ",\"log_offset\":10}",
            "{\"version\":1,"
                    + "\"table_id\":1,\"partition_id\":10,\"bucket_id\":1,"
                    + "\"snapshot_id\":1,"
                    + "\"snapshot_location\":\"oss://bucket/snapshot\","
                    + kvSnapshotHandleJson
                    + ",\"log_offset\":10,\"row_count\":1234,\"auto_inc_id_range\":[{\"column_id\":2,\"start\":10000,\"end\":20000}]}",
            "{\"version\":1,"
                    + "\"table_id\":2,\"bucket_id\":0,"
                    + "\"snapshot_id\":2,"
                    + "\"snapshot_location\":\"oss://bucket/snapshot\","
                    + kvSnapshotHandleJson
                    + ",\"log_offset\":20,"
                    + "\"index_replication_offsets\":[{\"table_id\":1,\"bucket_id\":0,\"offset\":100}]}",
            "{\"version\":1,"
                    + "\"table_id\":2,\"partition_id\":10,\"bucket_id\":0,"
                    + "\"snapshot_id\":3,"
                    + "\"snapshot_location\":\"oss://bucket/snapshot\","
                    + kvSnapshotHandleJson
                    + ",\"log_offset\":30,"
                    + "\"index_replication_offsets\":[{\"table_id\":1,\"partition_id\":10,\"bucket_id\":0,\"offset\":200}]}"
        };
    }

    @Test
    void testLegacyFormatDeserialization() {
        // Legacy format uses JSON object with string keys: "tableId:bucketId" or
        // "tableId:partitionId:bucketId"
        String legacyJsonNonPartitioned =
                "{\"version\":1,"
                        + "\"table_id\":2,\"bucket_id\":0,"
                        + "\"snapshot_id\":2,"
                        + "\"snapshot_location\":\"oss://bucket/snapshot\","
                        + "\"kv_snapshot_handle\":{"
                        + "\"shared_file_handles\":[],"
                        + "\"private_file_handles\":[],"
                        + "\"snapshot_incremental_size\":0},"
                        + "\"log_offset\":20,"
                        + "\"index_replication_offsets\":{\"1:0\":100}}";

        CompletedSnapshot snapshot =
                CompletedSnapshotJsonSerde.fromJson(
                        legacyJsonNonPartitioned.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        Map<TableBucket, Long> offsets = snapshot.getIndexReplicationOffsets();
        assertThat(offsets).isNotNull();
        assertThat(offsets).hasSize(1);
        assertThat(offsets.get(new TableBucket(1, 0))).isEqualTo(100L);

        // Legacy format with partitioned bucket key
        String legacyJsonPartitioned =
                "{\"version\":1,"
                        + "\"table_id\":2,\"partition_id\":10,\"bucket_id\":0,"
                        + "\"snapshot_id\":3,"
                        + "\"snapshot_location\":\"oss://bucket/snapshot\","
                        + "\"kv_snapshot_handle\":{"
                        + "\"shared_file_handles\":[],"
                        + "\"private_file_handles\":[],"
                        + "\"snapshot_incremental_size\":0},"
                        + "\"log_offset\":30,"
                        + "\"index_replication_offsets\":{\"1:10:0\":200}}";

        CompletedSnapshot snapshot2 =
                CompletedSnapshotJsonSerde.fromJson(
                        legacyJsonPartitioned.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        Map<TableBucket, Long> offsets2 = snapshot2.getIndexReplicationOffsets();
        assertThat(offsets2).isNotNull();
        assertThat(offsets2).hasSize(1);
        assertThat(offsets2.get(new TableBucket(1, 10L, 0))).isEqualTo(200L);
    }
}
