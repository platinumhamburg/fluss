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

package org.apache.fluss.client.lookup;

import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SecondaryIndexLookuper}. */
class SecondaryIndexLookuperTest {

    @Test
    void testHop1ForwardsLookupKeyToPrefixLookuper() throws Exception {
        InternalRow expectedKey = new GenericRow(1);
        InternalRow indexHit = new GenericRow(1);
        StubLookuper indexLookuper =
                new StubLookuper(
                        key ->
                                CompletableFuture.completedFuture(
                                        new LookupResult(
                                                Collections.singletonList(indexHit))));
        StubLookuper mainLookuper =
                new StubLookuper(
                        key ->
                                CompletableFuture.completedFuture(
                                        new LookupResult(
                                                Collections.<InternalRow>emptyList())));

        SecondaryIndexLookuper lookuper =
                new SecondaryIndexLookuper(
                        indexLookuper,
                        mainLookuper,
                        new int[] {0},
                        new InternalRow.FieldGetter[] {row -> row},
                        new InternalRow.FieldGetter[] {row -> row},
                        indexRow -> indexRow);

        LookupResult result = lookuper.lookup(expectedKey).get();

        assertThat(indexLookuper.lastLookupKey).isSameAs(expectedKey);
        // Hop 2 is invoked once with the single candidate row from Hop 1.
        assertThat(mainLookuper.lastLookupKey).isSameAs(indexHit);
        // The stubbed main lookuper returns an empty list, so the aggregated result is empty.
        assertThat(result.getRowList()).isEmpty();
    }

    @Test
    void testHop2FetchesMainRowPerCandidateAndAggregates() throws Exception {
        GenericRow lookupKey = new GenericRow(1);
        lookupKey.setField(0, "A");

        GenericRow indexRow1 = new GenericRow(2);
        indexRow1.setField(0, "A");
        indexRow1.setField(1, 1L);
        GenericRow indexRow2 = new GenericRow(2);
        indexRow2.setField(0, "A");
        indexRow2.setField(1, 2L);
        GenericRow indexRow3 = new GenericRow(2);
        indexRow3.setField(0, "A");
        indexRow3.setField(1, 3L);
        GenericRow mainRow1 = new GenericRow(2);
        mainRow1.setField(0, "A");
        mainRow1.setField(1, 1L);
        GenericRow mainRow2 = new GenericRow(2);
        mainRow2.setField(0, "A");
        mainRow2.setField(1, 2L);
        GenericRow mainRow3 = new GenericRow(2);
        mainRow3.setField(0, "A");
        mainRow3.setField(1, 3L);

        StubLookuper indexLookuper =
                new StubLookuper(
                        key ->
                                CompletableFuture.completedFuture(
                                        new LookupResult(
                                                Arrays.asList(indexRow1, indexRow2, indexRow3))));

        Map<Long, InternalRow> mainTable = new HashMap<>();
        mainTable.put(1L, mainRow1);
        mainTable.put(2L, mainRow2);
        mainTable.put(3L, mainRow3);
        StubLookuper mainLookuper =
                new StubLookuper(
                        key -> {
                            long pk = key.getLong(0);
                            InternalRow r = mainTable.get(pk);
                            return CompletableFuture.completedFuture(
                                    new LookupResult(
                                            r == null
                                                    ? Collections.<InternalRow>emptyList()
                                                    : Collections.singletonList(r)));
                        });

        SecondaryIndexLookuper lookuper =
                new SecondaryIndexLookuper(
                        indexLookuper,
                        mainLookuper,
                        new int[] {0},
                        new InternalRow.FieldGetter[] {row -> ((GenericRow) row).getField(0)},
                        new InternalRow.FieldGetter[] {row -> ((GenericRow) row).getField(0)},
                        indexRow -> {
                            GenericRow pk = new GenericRow(1);
                            pk.setField(0, indexRow.getLong(1));
                            return pk;
                        });

        LookupResult result = lookuper.lookup(lookupKey).get();

        assertThat(result.getRowList())
                .containsExactlyInAnyOrder(mainRow1, mainRow2, mainRow3);
    }

    @Test
    void testHop2SkipsDeletedRows() throws Exception {
        GenericRow lookupKey = new GenericRow(1);
        lookupKey.setField(0, "A");

        GenericRow indexRow1 = new GenericRow(2);
        indexRow1.setField(0, "A");
        indexRow1.setField(1, 1L);
        GenericRow indexRow2 = new GenericRow(2);
        indexRow2.setField(0, "A");
        indexRow2.setField(1, 2L);
        GenericRow indexRow3 = new GenericRow(2);
        indexRow3.setField(0, "A");
        indexRow3.setField(1, 3L);
        GenericRow mainRow1 = new GenericRow(2);
        mainRow1.setField(0, "A");
        mainRow1.setField(1, 1L);
        GenericRow mainRow3 = new GenericRow(2);
        mainRow3.setField(0, "A");
        mainRow3.setField(1, 3L);

        StubLookuper indexLookuper =
                new StubLookuper(
                        key ->
                                CompletableFuture.completedFuture(
                                        new LookupResult(
                                                Arrays.asList(indexRow1, indexRow2, indexRow3))));

        // pk=2 is missing -> main lookup returns empty list -> skip-deleted-rows path.
        Map<Long, InternalRow> mainTable = new HashMap<>();
        mainTable.put(1L, mainRow1);
        mainTable.put(3L, mainRow3);
        StubLookuper mainLookuper =
                new StubLookuper(
                        key -> {
                            long pk = key.getLong(0);
                            InternalRow r = mainTable.get(pk);
                            return CompletableFuture.completedFuture(
                                    new LookupResult(
                                            r == null
                                                    ? Collections.<InternalRow>emptyList()
                                                    : Collections.singletonList(r)));
                        });

        SecondaryIndexLookuper lookuper =
                new SecondaryIndexLookuper(
                        indexLookuper,
                        mainLookuper,
                        new int[] {0},
                        new InternalRow.FieldGetter[] {row -> ((GenericRow) row).getField(0)},
                        new InternalRow.FieldGetter[] {row -> ((GenericRow) row).getField(0)},
                        indexRow -> {
                            GenericRow pk = new GenericRow(1);
                            pk.setField(0, indexRow.getLong(1));
                            return pk;
                        });

        LookupResult result = lookuper.lookup(lookupKey).get();

        assertThat(result.getRowList()).containsExactlyInAnyOrder(mainRow1, mainRow3);
    }

    @Test
    void testRecheckDiscardsRowsWithMismatchedIdxCols() throws Exception {
        // Lookup key = "A". Hop 1 returns 3 index rows for "A".
        // Hop 2 returns: row(idxCol=A, pk=10), row(idxCol=B, pk=20), row(idxCol=A, pk=30).
        // After recheck: rows 10 and 30 remain (the pk=20 row's current idxCol is "B"
        // -> stale index pointer -> discard).
        GenericRow lookupKey = new GenericRow(1);
        lookupKey.setField(0, "A");

        GenericRow indexRow1 = new GenericRow(2);
        indexRow1.setField(0, "A");
        indexRow1.setField(1, 10L);
        GenericRow indexRow2 = new GenericRow(2);
        indexRow2.setField(0, "A");
        indexRow2.setField(1, 20L);
        GenericRow indexRow3 = new GenericRow(2);
        indexRow3.setField(0, "A");
        indexRow3.setField(1, 30L);

        GenericRow mainRow10 = new GenericRow(2);
        mainRow10.setField(0, "A");
        mainRow10.setField(1, 10L);
        GenericRow mainRow20 = new GenericRow(2);
        mainRow20.setField(0, "B");
        mainRow20.setField(1, 20L);
        GenericRow mainRow30 = new GenericRow(2);
        mainRow30.setField(0, "A");
        mainRow30.setField(1, 30L);

        StubLookuper indexLookuper =
                new StubLookuper(
                        key ->
                                CompletableFuture.completedFuture(
                                        new LookupResult(
                                                Arrays.asList(indexRow1, indexRow2, indexRow3))));

        Map<Long, InternalRow> mainTable = new HashMap<>();
        mainTable.put(10L, mainRow10);
        mainTable.put(20L, mainRow20);
        mainTable.put(30L, mainRow30);
        StubLookuper mainLookuper =
                new StubLookuper(
                        key -> {
                            long pk = key.getLong(0);
                            InternalRow r = mainTable.get(pk);
                            return CompletableFuture.completedFuture(
                                    new LookupResult(
                                            r == null
                                                    ? Collections.<InternalRow>emptyList()
                                                    : Collections.singletonList(r)));
                        });

        SecondaryIndexLookuper lookuper =
                new SecondaryIndexLookuper(
                        indexLookuper,
                        mainLookuper,
                        new int[] {0},
                        // lookup-key getter: idxCol value lives at position 0 of the lookup key.
                        new InternalRow.FieldGetter[] {row -> ((GenericRow) row).getField(0)},
                        // main-row getter: idxCol value lives at position 0 of the main row.
                        new InternalRow.FieldGetter[] {row -> ((GenericRow) row).getField(0)},
                        indexRow -> {
                            GenericRow pk = new GenericRow(1);
                            pk.setField(0, indexRow.getLong(1));
                            return pk;
                        });

        LookupResult result = lookuper.lookup(lookupKey).get();

        assertThat(result.getRowList()).containsExactlyInAnyOrder(mainRow10, mainRow30);
    }

    @Test
    void testRecheckPassesWhenAllIdxColsMatch() throws Exception {
        // All 3 main rows have idxCol=A and the lookup key is "A" -> all 3 returned.
        GenericRow lookupKey = new GenericRow(1);
        lookupKey.setField(0, "A");

        GenericRow indexRow1 = new GenericRow(2);
        indexRow1.setField(0, "A");
        indexRow1.setField(1, 1L);
        GenericRow indexRow2 = new GenericRow(2);
        indexRow2.setField(0, "A");
        indexRow2.setField(1, 2L);
        GenericRow indexRow3 = new GenericRow(2);
        indexRow3.setField(0, "A");
        indexRow3.setField(1, 3L);

        GenericRow mainRow1 = new GenericRow(2);
        mainRow1.setField(0, "A");
        mainRow1.setField(1, 1L);
        GenericRow mainRow2 = new GenericRow(2);
        mainRow2.setField(0, "A");
        mainRow2.setField(1, 2L);
        GenericRow mainRow3 = new GenericRow(2);
        mainRow3.setField(0, "A");
        mainRow3.setField(1, 3L);

        StubLookuper indexLookuper =
                new StubLookuper(
                        key ->
                                CompletableFuture.completedFuture(
                                        new LookupResult(
                                                Arrays.asList(indexRow1, indexRow2, indexRow3))));

        Map<Long, InternalRow> mainTable = new HashMap<>();
        mainTable.put(1L, mainRow1);
        mainTable.put(2L, mainRow2);
        mainTable.put(3L, mainRow3);
        StubLookuper mainLookuper =
                new StubLookuper(
                        key -> {
                            long pk = key.getLong(0);
                            InternalRow r = mainTable.get(pk);
                            return CompletableFuture.completedFuture(
                                    new LookupResult(
                                            r == null
                                                    ? Collections.<InternalRow>emptyList()
                                                    : Collections.singletonList(r)));
                        });

        SecondaryIndexLookuper lookuper =
                new SecondaryIndexLookuper(
                        indexLookuper,
                        mainLookuper,
                        new int[] {0},
                        new InternalRow.FieldGetter[] {row -> ((GenericRow) row).getField(0)},
                        new InternalRow.FieldGetter[] {row -> ((GenericRow) row).getField(0)},
                        indexRow -> {
                            GenericRow pk = new GenericRow(1);
                            pk.setField(0, indexRow.getLong(1));
                            return pk;
                        });

        LookupResult result = lookuper.lookup(lookupKey).get();

        assertThat(result.getRowList())
                .containsExactlyInAnyOrder(mainRow1, mainRow2, mainRow3);
    }

    @Test
    void testRecheckHandlesNullableIdxColCorrectly() throws Exception {
        // Lookup key idxCol is null. Hop 2 returns two rows:
        //   - mainRow1: idxCol=null -> matches via Objects.equals -> keep.
        //   - mainRow2: idxCol="A"  -> mismatch -> discard.
        GenericRow lookupKey = new GenericRow(1);
        lookupKey.setField(0, null);

        GenericRow indexRow1 = new GenericRow(2);
        indexRow1.setField(0, null);
        indexRow1.setField(1, 1L);
        GenericRow indexRow2 = new GenericRow(2);
        indexRow2.setField(0, null);
        indexRow2.setField(1, 2L);

        GenericRow mainRow1 = new GenericRow(2);
        mainRow1.setField(0, null);
        mainRow1.setField(1, 1L);
        GenericRow mainRow2 = new GenericRow(2);
        mainRow2.setField(0, "A");
        mainRow2.setField(1, 2L);

        StubLookuper indexLookuper =
                new StubLookuper(
                        key ->
                                CompletableFuture.completedFuture(
                                        new LookupResult(
                                                Arrays.asList(indexRow1, indexRow2))));

        Map<Long, InternalRow> mainTable = new HashMap<>();
        mainTable.put(1L, mainRow1);
        mainTable.put(2L, mainRow2);
        StubLookuper mainLookuper =
                new StubLookuper(
                        key -> {
                            long pk = key.getLong(0);
                            InternalRow r = mainTable.get(pk);
                            return CompletableFuture.completedFuture(
                                    new LookupResult(
                                            r == null
                                                    ? Collections.<InternalRow>emptyList()
                                                    : Collections.singletonList(r)));
                        });

        SecondaryIndexLookuper lookuper =
                new SecondaryIndexLookuper(
                        indexLookuper,
                        mainLookuper,
                        new int[] {0},
                        new InternalRow.FieldGetter[] {row -> ((GenericRow) row).getField(0)},
                        new InternalRow.FieldGetter[] {row -> ((GenericRow) row).getField(0)},
                        indexRow -> {
                            GenericRow pk = new GenericRow(1);
                            pk.setField(0, indexRow.getLong(1));
                            return pk;
                        });

        LookupResult result = lookuper.lookup(lookupKey).get();

        assertThat(result.getRowList()).containsExactly(mainRow1);
    }

    private static final class StubLookuper implements Lookuper {
        final Function<InternalRow, CompletableFuture<LookupResult>> onLookup;
        volatile InternalRow lastLookupKey;

        StubLookuper(Function<InternalRow, CompletableFuture<LookupResult>> onLookup) {
            this.onLookup = onLookup;
        }

        @Override
        public CompletableFuture<LookupResult> lookup(InternalRow key) {
            this.lastLookupKey = key;
            return onLookup.apply(key);
        }
    }
}
