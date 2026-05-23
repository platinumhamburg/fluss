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

package org.apache.fluss.server.replica;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SecondaryIndexVisibility;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.replica.delay.DelayedWrite;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ReplicaManager#computeRequiredIndexOffset(TableInfo, long)}.
 *
 * <p>P2T12: verifies that the helper that drives {@code DelayedBucketStatus.requiredIndexOffset}
 * honors the {@code secondary-index.visibility} table property:
 *
 * <ul>
 *   <li>tables without secondary indexes never wait on an index-pushed-offset watermark;
 *   <li>tables with indexes default to {@link SecondaryIndexVisibility#SYNC} and require the
 *       index-pushed-offset to reach the write offset before the ack returns;
 *   <li>tables with indexes set to {@link SecondaryIndexVisibility#ASYNC} ack as soon as the WAL
 *       is replicated, regardless of index push progress.
 * </ul>
 */
class ReplicaManagerVisibilityTest {

    private static final TablePath TABLE_PATH = TablePath.of("test_db", "test_visibility");
    private static final long TABLE_ID = 9_000_001L;
    private static final long WRITE_OFFSET = 1234L;

    @Test
    void testNoIndexTableAcksWithoutIndexOffsetRequirement() {
        TableInfo tableInfo = tableInfoOf(schemaWithoutIndex(), /* visibility */ null);

        long required = ReplicaManager.computeRequiredIndexOffset(tableInfo, WRITE_OFFSET);

        assertThat(required).isEqualTo(DelayedWrite.DelayedBucketStatus.NO_INDEX_OFFSET_REQUIRED);
    }

    @Test
    void testNoIndexTableIgnoresExplicitVisibility() {
        // Even if a user (somehow) sets visibility on a table without indexes, there's nothing to
        // wait on — guard against accidental requirements on un-indexed tables.
        TableInfo syncOnNoIndex = tableInfoOf(schemaWithoutIndex(), SecondaryIndexVisibility.SYNC);
        TableInfo asyncOnNoIndex =
                tableInfoOf(schemaWithoutIndex(), SecondaryIndexVisibility.ASYNC);

        assertThat(ReplicaManager.computeRequiredIndexOffset(syncOnNoIndex, WRITE_OFFSET))
                .isEqualTo(DelayedWrite.DelayedBucketStatus.NO_INDEX_OFFSET_REQUIRED);
        assertThat(ReplicaManager.computeRequiredIndexOffset(asyncOnNoIndex, WRITE_OFFSET))
                .isEqualTo(DelayedWrite.DelayedBucketStatus.NO_INDEX_OFFSET_REQUIRED);
    }

    @Test
    void testSyncVisibilityIndexTableRequiresIndexOffset() {
        // Default visibility (no property set) is SYNC — the ack must wait for the index-pushed
        // offset to reach the write offset.
        TableInfo defaultSync = tableInfoOf(schemaWithIndex(), /* visibility */ null);
        TableInfo explicitSync = tableInfoOf(schemaWithIndex(), SecondaryIndexVisibility.SYNC);

        assertThat(ReplicaManager.computeRequiredIndexOffset(defaultSync, WRITE_OFFSET))
                .isEqualTo(WRITE_OFFSET);
        assertThat(ReplicaManager.computeRequiredIndexOffset(explicitSync, WRITE_OFFSET))
                .isEqualTo(WRITE_OFFSET);
    }

    @Test
    void testAsyncVisibilityIndexTableSkipsIndexOffsetRequirement() {
        TableInfo asyncIndexed = tableInfoOf(schemaWithIndex(), SecondaryIndexVisibility.ASYNC);

        long required = ReplicaManager.computeRequiredIndexOffset(asyncIndexed, WRITE_OFFSET);

        assertThat(required).isEqualTo(DelayedWrite.DelayedBucketStatus.NO_INDEX_OFFSET_REQUIRED);
    }

    @Test
    void testNullTableInfoDefaultsToNoIndexRequirement() {
        // Defensive path: if metadata lookup fails the caller passes null and we must NOT block
        // the write path on an index watermark we cannot verify.
        assertThat(ReplicaManager.computeRequiredIndexOffset(null, WRITE_OFFSET))
                .isEqualTo(DelayedWrite.DelayedBucketStatus.NO_INDEX_OFFSET_REQUIRED);
    }

    // ---------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------

    private static Schema schemaWithoutIndex() {
        return Schema.newBuilder()
                .column("a", DataTypes.INT())
                .column("b", DataTypes.STRING())
                .column("c", DataTypes.STRING())
                .primaryKey("a")
                .build();
    }

    private static Schema schemaWithIndex() {
        return Schema.newBuilder()
                .column("a", DataTypes.INT())
                .column("b", DataTypes.STRING())
                .column("c", DataTypes.STRING())
                .primaryKey("a")
                .index("idx_b", "b")
                .build();
    }

    private static TableInfo tableInfoOf(
            Schema schema, /* @Nullable */ SecondaryIndexVisibility visibility) {
        TableDescriptor.Builder builder =
                TableDescriptor.builder().schema(schema).distributedBy(3, "a");
        if (visibility != null) {
            builder.property(ConfigOptions.SECONDARY_INDEX_VISIBILITY, visibility);
        }
        long now = System.currentTimeMillis();
        return TableInfo.of(
                TABLE_PATH,
                TABLE_ID,
                /* schemaId */ 1,
                builder.build(),
                /* remoteLogDir */ "/tmp/fluss/remote-data",
                now,
                now);
    }
}
