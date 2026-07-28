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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
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
                                        new LookupResult(Collections.singletonList(indexHit))));
        StubLookuper mainLookuper =
                new StubLookuper(
                        key ->
                                CompletableFuture.completedFuture(
                                        new LookupResult(Collections.<InternalRow>emptyList())));

        SecondaryIndexLookuper lookuper =
                new SecondaryIndexLookuper(
                        indexLookuper,
                        mainLookuper,
                        new int[] {0},
                        new InternalRow.FieldGetter[] {row -> row},
                        new InternalRow.FieldGetter[] {row -> row},
                        indexRow -> (GenericRow) indexRow);

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

        assertThat(result.getRowList()).containsExactlyInAnyOrder(mainRow1, mainRow2, mainRow3);
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

        assertThat(result.getRowList()).containsExactlyInAnyOrder(mainRow1, mainRow2, mainRow3);
    }

    @Test
    void testRecheckComparesByteArrayIndexColumnsByContent() throws Exception {
        byte[] lookupBytes = new byte[] {1, 2, 3};
        byte[] mainBytes = new byte[] {1, 2, 3};

        GenericRow lookupKey = new GenericRow(1);
        lookupKey.setField(0, lookupBytes);
        GenericRow indexRow = new GenericRow(2);
        indexRow.setField(0, lookupBytes);
        indexRow.setField(1, 10L);
        GenericRow mainRow = new GenericRow(2);
        mainRow.setField(0, mainBytes);
        mainRow.setField(1, 10L);

        StubLookuper indexLookuper =
                new StubLookuper(
                        key ->
                                CompletableFuture.completedFuture(
                                        new LookupResult(Collections.singletonList(indexRow))));
        StubLookuper mainLookuper =
                new StubLookuper(
                        key ->
                                CompletableFuture.completedFuture(
                                        new LookupResult(Collections.singletonList(mainRow))));

        SecondaryIndexLookuper lookuper =
                new SecondaryIndexLookuper(
                        indexLookuper,
                        mainLookuper,
                        new int[] {0},
                        new InternalRow.FieldGetter[] {row -> ((GenericRow) row).getField(0)},
                        new InternalRow.FieldGetter[] {row -> ((GenericRow) row).getField(0)},
                        index -> {
                            GenericRow pk = new GenericRow(1);
                            pk.setField(0, index.getLong(1));
                            return pk;
                        });

        LookupResult result = lookuper.lookup(lookupKey).get();

        assertThat(result.getRowList()).containsExactly(mainRow);
    }

    @Test
    void testRecheckUsesLookupKeySnapshotAcrossAsyncBoundary() throws Exception {
        byte[] lookupBytes = new byte[] {1, 2, 3};
        byte[] mainBytes = new byte[] {1, 2, 3};

        GenericRow lookupKey = new GenericRow(1);
        lookupKey.setField(0, lookupBytes);
        GenericRow indexRow = new GenericRow(2);
        indexRow.setField(0, lookupBytes);
        indexRow.setField(1, 10L);
        GenericRow mainRow = new GenericRow(2);
        mainRow.setField(0, mainBytes);
        mainRow.setField(1, 10L);

        CompletableFuture<LookupResult> hop1Future = new CompletableFuture<>();
        StubLookuper indexLookuper = new StubLookuper(key -> hop1Future);
        StubLookuper mainLookuper =
                new StubLookuper(
                        key ->
                                CompletableFuture.completedFuture(
                                        new LookupResult(Collections.singletonList(mainRow))));

        SecondaryIndexLookuper lookuper =
                new SecondaryIndexLookuper(
                        indexLookuper,
                        mainLookuper,
                        new int[] {0},
                        new InternalRow.FieldGetter[] {row -> ((GenericRow) row).getField(0)},
                        new InternalRow.FieldGetter[] {row -> ((GenericRow) row).getField(0)},
                        index -> {
                            GenericRow pk = new GenericRow(1);
                            pk.setField(0, index.getLong(1));
                            return pk;
                        });

        CompletableFuture<LookupResult> resultFuture = lookuper.lookup(lookupKey);
        lookupBytes[0] = 9;
        lookupKey.setField(0, lookupBytes);
        hop1Future.complete(new LookupResult(Collections.singletonList(indexRow)));

        assertThat(resultFuture.get().getRowList()).containsExactly(mainRow);
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
                                        new LookupResult(Arrays.asList(indexRow1, indexRow2))));

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

    @Test
    void testDeduplicatesNonAdjacentPhysicalRowsByLogicalPrimaryKey() throws Exception {
        GenericRow lookupKey = GenericRow.of("matched");

        GenericRow oldPartitionRow = GenericRow.of("matched", 1, "2024", 10L);
        GenericRow otherRow = GenericRow.of("matched", 2, "2024", 20L);
        GenericRow recreatedPartitionRow = GenericRow.of("matched", 1, "2024", 30L);
        GenericRow currentMainRow = GenericRow.of(1, "matched", "2024");
        GenericRow otherMainRow = GenericRow.of(2, "matched", "2024");

        StubLookuper indexLookuper =
                new StubLookuper(
                        key ->
                                CompletableFuture.completedFuture(
                                        new LookupResult(
                                                Arrays.asList(
                                                        oldPartitionRow,
                                                        otherRow,
                                                        recreatedPartitionRow))));
        List<InternalRow> mainLookupKeys = new ArrayList<>();
        StubLookuper mainLookuper =
                new StubLookuper(
                        key -> {
                            mainLookupKeys.add(key);
                            InternalRow mainRow =
                                    key.getInt(0) == 1 ? currentMainRow : otherMainRow;
                            return CompletableFuture.completedFuture(
                                    new LookupResult(Collections.singletonList(mainRow)));
                        });

        SecondaryIndexLookuper lookuper =
                new SecondaryIndexLookuper(
                        indexLookuper,
                        mainLookuper,
                        new int[] {1},
                        new InternalRow.FieldGetter[] {row -> ((GenericRow) row).getField(0)},
                        new InternalRow.FieldGetter[] {row -> ((GenericRow) row).getField(1)},
                        SecondaryIndexLookuperTest::extractPartitionedBasePrimaryKey);

        LookupResult result = lookuper.lookup(lookupKey).get();

        assertThat(mainLookupKeys)
                .containsExactly(GenericRow.of(1, "2024"), GenericRow.of(2, "2024"));
        assertThat(result.getRowList()).containsExactly(currentMainRow, otherMainRow);
    }

    @Test
    void testDeduplicatesBinaryPrimaryKeysByContent() throws Exception {
        GenericRow lookupKey = GenericRow.of("matched");
        GenericRow oldPartitionRow = GenericRow.of("matched", new byte[] {1, 2, 3}, 10L);
        GenericRow recreatedPartitionRow = GenericRow.of("matched", new byte[] {1, 2, 3}, 20L);
        GenericRow currentMainRow = GenericRow.of(new byte[] {1, 2, 3}, "matched");

        StubLookuper indexLookuper =
                new StubLookuper(
                        key ->
                                CompletableFuture.completedFuture(
                                        new LookupResult(
                                                Arrays.asList(
                                                        oldPartitionRow, recreatedPartitionRow))));
        AtomicInteger mainLookupCount = new AtomicInteger();
        StubLookuper mainLookuper =
                new StubLookuper(
                        key -> {
                            mainLookupCount.incrementAndGet();
                            return CompletableFuture.completedFuture(
                                    new LookupResult(Collections.singletonList(currentMainRow)));
                        });

        SecondaryIndexLookuper lookuper =
                new SecondaryIndexLookuper(
                        indexLookuper,
                        mainLookuper,
                        new int[] {1},
                        new InternalRow.FieldGetter[] {row -> ((GenericRow) row).getField(0)},
                        new InternalRow.FieldGetter[] {row -> ((GenericRow) row).getField(1)},
                        indexRow -> GenericRow.of(((GenericRow) indexRow).getField(1)));

        LookupResult result = lookuper.lookup(lookupKey).get();

        assertThat(mainLookupCount).hasValue(1);
        assertThat(result.getRowList()).containsExactly(currentMainRow);
    }

    @Test
    void testConcurrentLookupsKeepDeduplicationStatePerCall() throws Exception {
        CompletableFuture<LookupResult> firstHop1 = new CompletableFuture<>();
        CompletableFuture<LookupResult> secondHop1 = new CompletableFuture<>();
        AtomicInteger hop1Invocation = new AtomicInteger();
        StubLookuper indexLookuper =
                new StubLookuper(
                        key -> hop1Invocation.getAndIncrement() == 0 ? firstHop1 : secondHop1);

        GenericRow currentMainRow = GenericRow.of(1, "matched", "2024");
        AtomicInteger mainLookupCount = new AtomicInteger();
        StubLookuper mainLookuper =
                new StubLookuper(
                        key -> {
                            mainLookupCount.incrementAndGet();
                            return CompletableFuture.completedFuture(
                                    new LookupResult(Collections.singletonList(currentMainRow)));
                        });
        SecondaryIndexLookuper lookuper =
                new SecondaryIndexLookuper(
                        indexLookuper,
                        mainLookuper,
                        new int[] {1},
                        new InternalRow.FieldGetter[] {row -> ((GenericRow) row).getField(0)},
                        new InternalRow.FieldGetter[] {row -> ((GenericRow) row).getField(1)},
                        SecondaryIndexLookuperTest::extractPartitionedBasePrimaryKey);

        CompletableFuture<LookupResult> firstResult = lookuper.lookup(GenericRow.of("matched"));
        CompletableFuture<LookupResult> secondResult = lookuper.lookup(GenericRow.of("matched"));
        List<InternalRow> duplicatePhysicalRows =
                Arrays.asList(
                        GenericRow.of("matched", 1, "2024", 10L),
                        GenericRow.of("matched", 1, "2024", 20L));

        secondHop1.complete(new LookupResult(duplicatePhysicalRows));
        firstHop1.complete(new LookupResult(duplicatePhysicalRows));

        assertThat(firstResult.get().getRowList()).containsExactly(currentMainRow);
        assertThat(secondResult.get().getRowList()).containsExactly(currentMainRow);
        assertThat(mainLookupCount).hasValue(2);
    }

    @Test
    void testWarnsWhenHop1CandidateCountReachesLowSelectivityThreshold() throws Exception {
        int candidateCount = 1024;
        List<InternalRow> indexRows = new ArrayList<>(candidateCount);
        for (int i = 0; i < candidateCount; i++) {
            GenericRow indexRow = new GenericRow(1);
            indexRow.setField(0, i);
            indexRows.add(indexRow);
        }

        StubLookuper indexLookuper =
                new StubLookuper(
                        key -> CompletableFuture.completedFuture(new LookupResult(indexRows)));
        AtomicInteger mainLookupCount = new AtomicInteger();
        StubLookuper mainLookuper =
                new StubLookuper(
                        key -> {
                            mainLookupCount.incrementAndGet();
                            return CompletableFuture.completedFuture(
                                    new LookupResult(Collections.<InternalRow>emptyList()));
                        });

        SecondaryIndexLookuper lookuper =
                new SecondaryIndexLookuper(
                        indexLookuper,
                        mainLookuper,
                        new int[] {0},
                        new InternalRow.FieldGetter[] {row -> ((GenericRow) row).getField(0)},
                        new InternalRow.FieldGetter[] {row -> ((GenericRow) row).getField(0)},
                        indexRow -> (GenericRow) indexRow);

        List<LogEvent> events = new ArrayList<>();
        AbstractAppender appender =
                new AbstractAppender(
                        "secondary-index-lookuper-test", null, null, false, Property.EMPTY_ARRAY) {
                    @Override
                    public void append(LogEvent event) {
                        events.add(event.toImmutable());
                    }
                };
        appender.start();
        LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
        Configuration configuration = loggerContext.getConfiguration();
        String loggerName = SecondaryIndexLookuper.class.getName();
        LoggerConfig loggerConfig = new LoggerConfig(loggerName, Level.WARN, false);
        loggerConfig.addAppender(appender, Level.WARN, null);
        configuration.addLogger(loggerName, loggerConfig);
        loggerContext.updateLoggers();
        try {
            LookupResult result = lookuper.lookup(new GenericRow(1)).get();

            assertThat(result.getRowList()).isEmpty();
            assertThat(mainLookupCount).hasValue(candidateCount);
            assertThat(events)
                    .anySatisfy(
                            event -> {
                                assertThat(event.getLevel()).isEqualTo(Level.WARN);
                                assertThat(event.getMessage().getFormattedMessage())
                                        .contains("low-selectivity")
                                        .contains(String.valueOf(candidateCount));
                            });
        } finally {
            configuration.removeLogger(loggerName);
            loggerContext.updateLoggers();
            appender.stop();
        }
    }

    private static GenericRow extractPartitionedBasePrimaryKey(InternalRow indexRow) {
        GenericRow genericIndexRow = (GenericRow) indexRow;
        return GenericRow.of(genericIndexRow.getField(1), genericIndexRow.getField(2));
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
