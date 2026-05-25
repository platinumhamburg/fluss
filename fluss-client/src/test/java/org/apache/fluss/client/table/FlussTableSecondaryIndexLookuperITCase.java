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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SecondaryIndexVisibility;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IndexTableUtils;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.time.Duration;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end ITCase for {@link FlussTable#getSecondaryIndexLookuper(String)} and its underlying
 * {@link SecondaryIndexLookuper} (Plan 4 §2.7 — two-hop lookup with stale-pointer recheck).
 *
 * <p>Scenarios:
 *
 * <ul>
 *   <li>{@link #testLookupReturnsNoStalePointers()} — async-visibility update from {@code idxCol=A}
 *       to {@code idxCol=B}. Lookup by {@code A} must return empty regardless of whether the Plan
 *       2 DELETE for the stale {@code (A, pk)} entry has already landed in the Index Table: the
 *       recheck step filters surviving stale pointers, and the standard cleanup path eventually
 *       removes them.
 *   <li>{@link #testGetSecondaryIndexLookuperRejectsUnknownIndexName()} — exercising the public
 *       validation path on a live {@link FlussTable} (the unit-level {@code
 *       FlussTable.findIndexOrThrow} is also covered by {@code
 *       FlussTableSecondaryIndexLookuperTest} but it skips the public surface because {@link
 *       org.apache.fluss.client.FlussConnection} is {@code final} and not Mockito-mockable).
 * </ul>
 *
 * <h2>Note on test location</h2>
 *
 * <p>Plan 4 Task 7's spec named {@code fluss-server/src/test/java/.../IndexPushReplicationITCase}
 * as the host file, but {@code fluss-server} cannot depend on {@code fluss-client} (it would form
 * a reactor cycle with {@code fluss-client}'s existing test-scope dep on {@code fluss-server}),
 * so the test must live in {@code fluss-client} to reach {@link FlussTable} and {@link Lookuper}.
 * The server-side scenarios that prepare the data shape this test consumes are already covered by
 * the gateway-level scenarios in {@code IndexPushReplicationITCase}.
 */
class FlussTableSecondaryIndexLookuperITCase extends ClientToServerITCaseBase {

    private static final String DB = "fluss_secondary_index_lookuper_itcase";
    private static final String INDEX_NAME = "idx_b";

    /** Bounded poll deadline for index visibility checks — same budget as P2 ITCase. */
    private static final Duration INDEX_VISIBILITY_TIMEOUT = Duration.ofSeconds(30);

    @Test
    void testLookupReturnsNoStalePointers() throws Exception {
        TablePath mainPath = TablePath.of(DB, "main_t_lookup");
        createMainTableWithIndex(mainPath, SecondaryIndexVisibility.ASYNC);

        try (Table mainTable = conn.getTable(mainPath);
                Table indexTable = conn.getTable(indexTablePathFor(mainPath))) {

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
            Lookuper lookuper =
                    ((FlussTable) mainTable).getSecondaryIndexLookuper(INDEX_NAME);
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
            FlussTable flussTable = (FlussTable) table;
            assertThatThrownBy(() -> flussTable.getSecondaryIndexLookuper("nonexistent"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("nonexistent");
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------

    private void createMainTableWithIndex(
            TablePath mainPath, @Nullable SecondaryIndexVisibility visibility) throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .primaryKey("a")
                        .index(INDEX_NAME, "b")
                        .build();
        TableDescriptor.Builder builder =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1, "a")
                        // Pin the Index Table bucket count to 1 to keep
                        // resolveIndexBucketCount aligned with the single main bucket.
                        .property(ConfigOptions.secondaryIndexBucketNumKey(INDEX_NAME), "1");
        if (visibility != null) {
            builder.property(ConfigOptions.SECONDARY_INDEX_VISIBILITY, visibility);
        }
        createTable(mainPath, builder.build(), /* ignoreIfExists */ true);
    }

    private static TablePath indexTablePathFor(TablePath mainPath) {
        return TablePath.of(
                mainPath.getDatabaseName(),
                IndexTableUtils.indexTableName(mainPath.getTableName(), INDEX_NAME));
    }

    /**
     * Polls the Index Table by a prefix lookup on the idx column {@code b} until presence matches
     * the expected value. Prefix lookup is the same shape the production {@link
     * SecondaryIndexLookuper} uses for Hop 1, so this directly mirrors what the lookuper will see
     * during the test.
     */
    private static void waitForIndexEntryPresence(
            Table indexTable, String bValue, boolean expectPresent) {
        Lookuper indexPrefix =
                indexTable.newLookup().lookupBy("b").createLookuper();
        String desc =
                "wait for index entry b=" + bValue + " presence=" + expectPresent;
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
