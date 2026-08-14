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

package org.apache.fluss.client.table;

import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.lookup.SecondaryIndexLookuper;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IndexTableUtils;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collections;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end ITCase for {@link Table#getSecondaryIndexLookuper(String)} and its underlying {@link
 * SecondaryIndexLookuper}.
 *
 * <p>Scenarios:
 *
 * <ul>
 *   <li>{@link #testLookupReturnsNoStalePointers()} — async-visibility update from {@code idxCol=A}
 *       to {@code idxCol=B}. Lookup by {@code A} must return empty regardless of whether the DELETE
 *       for the stale {@code (A, pk)} entry has already landed in the Index Table: the recheck step
 *       filters surviving stale pointers, and the standard cleanup path eventually removes them.
 *   <li>{@link #testGetSecondaryIndexLookuperRejectsUnknownIndexName()} — exercising the public
 *       validation path on a live {@link Table}.
 * </ul>
 */
class FlussTableSecondaryIndexLookuperITCase extends ClientToServerITCaseBase {

    private static final String DB = "fluss_secondary_index_lookuper_itcase";
    private static final String INDEX_NAME = "idx_b";

    /** Bounded poll deadline for index visibility checks — same budget as P2 ITCase. */
    private static final Duration INDEX_VISIBILITY_TIMEOUT = Duration.ofSeconds(30);

    @Test
    void testLookupReturnsNoStalePointers() throws Exception {
        TablePath mainPath = TablePath.of(DB, "main_t_lookup");
        long mainTableId = createMainTableWithIndex(mainPath, IndexVisibility.ASYNC);

        try (Table mainTable = conn.getTable(mainPath);
                Table indexTable = conn.getTable(indexTablePathFor(mainPath, mainTableId))) {

            // (1) Insert (a=1, b="A"); wait until the Index Table holds an entry under b="A".
            UpsertWriter upsertWriter = mainTable.newUpsert().createWriter();
            upsertWriter.upsert(row(1, "A"));
            upsertWriter.flush();
            waitForIndexEntryPresence(indexTable, "A", /* expectPresent */ true);

            // (2) Update pk=1 to b="B"; wait until the new index entry shows up. The DELETE of
            // the old ("A", 1) entry runs asynchronously in this mode and may not have landed by
            // the time we issue the lookup below — that is exactly the race the recheck must
            // handle.
            upsertWriter.upsert(row(1, "B"));
            upsertWriter.flush();
            waitForIndexEntryPresence(indexTable, "B", /* expectPresent */ true);

            // (3) Lookup by b="A" must return empty. Two paths land us there:
            //   - The async DELETE already removed ("A", 1) -> Hop 1 returns 0 candidate rows.
            //   - The DELETE has not landed yet -> Hop 1 returns the stale ("A", 1) row, Hop 2
            //     point-gets pk=1 and finds (1, "B"), recheck sees "B" != "A" and discards.
            Lookuper lookuper = mainTable.getSecondaryIndexLookuper(INDEX_NAME);
            LookupResult lookupAResult = lookuper.lookup(row("A")).get();
            assertThat(lookupAResult.getRowList())
                    .as("stale lookup by old idxCol value must be filtered out")
                    .isEmpty();

            // (4) Lookup by b="B" must return the current row (1, "B").
            LookupResult lookupBResult = lookuper.lookup(row("B")).get();
            assertThat(lookupBResult.getRowList()).hasSize(1);
            InternalRow result = lookupBResult.getRowList().get(0);
            assertThat(result.getInt(0)).isEqualTo(1);
            assertThat(result.getString(1).toString()).isEqualTo("B");
        }
    }

    @Test
    void testGetSecondaryIndexLookuperRejectsUnknownIndexName() throws Exception {
        TablePath mainPath = TablePath.of(DB, "main_t_unknown_idx");
        createMainTableWithIndex(mainPath, /* visibility */ null);
        try (Table table = conn.getTable(mainPath)) {
            assertThatThrownBy(() -> table.getSecondaryIndexLookuper("nonexistent"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("nonexistent");
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------

    private long createMainTableWithIndex(TablePath mainPath, @Nullable IndexVisibility visibility)
            throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .primaryKey("a")
                        .index(
                                INDEX_NAME,
                                IndexType.SECONDARY,
                                Collections.singletonList("b"),
                                visibility == null ? IndexVisibility.SYNC : visibility,
                                3)
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "a").build();
        return createTable(mainPath, descriptor, /* ignoreIfExists */ true);
    }

    private static TablePath indexTablePathFor(TablePath mainPath, long mainTableId) {
        return TablePath.of(
                mainPath.getDatabaseName(),
                IndexTableUtils.indexTableName(mainTableId, INDEX_NAME));
    }

    /**
     * Polls the Index Table by a prefix lookup on the idx column {@code b} until presence matches
     * the expected value. Prefix lookup is the same shape the production {@link
     * SecondaryIndexLookuper} uses for Hop 1, so this directly mirrors what the lookuper will see
     * during the test.
     */
    private static void waitForIndexEntryPresence(
            Table indexTable, String bValue, boolean expectPresent) {
        Lookuper indexPrefix = indexTable.newLookup().lookupBy("b").createLookuper();
        String desc = "wait for index entry b=" + bValue + " presence=" + expectPresent;
        waitUntil(
                () -> {
                    LookupResult r = indexPrefix.lookup(row(bValue)).get();
                    boolean present = !r.getRowList().isEmpty();
                    return present == expectPresent;
                },
                INDEX_VISIBILITY_TIMEOUT,
                desc);
    }
}
