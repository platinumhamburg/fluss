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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.row.InternalRow;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Two-hop {@link Lookuper} for a Global Secondary Index.
 *
 * <p>Hop 1: prefix-scan the Index Table by the provided {@code lookupKey} to obtain candidate
 * Index Table rows (each carrying the base table's primary key in trailing positions).
 *
 * <p>Hop 2: for every candidate row, extract the basePK via {@code basePkExtractorFromIndexRow}
 * and point-get the main table. A main lookup returning an empty result list means the row was
 * deleted (stale Index Table pointer) and is skipped from the aggregated output.
 *
 * <p>The recheck step that re-validates surviving main rows against the original {@code lookupKey}
 * is intentionally NOT performed here — it is added in P4T3.
 */
@Internal
@NotThreadSafe
public final class SecondaryIndexLookuper implements Lookuper {

    private final Lookuper indexTablePrefixLookuper;
    private final Lookuper mainTablePointLookuper;
    private final int[] idxColumnIndicesInMainRow;
    private final InternalRow.FieldGetter[] idxColumnGettersInMainRow;
    private final Function<InternalRow, InternalRow> basePkExtractorFromIndexRow;

    public SecondaryIndexLookuper(
            Lookuper indexTablePrefixLookuper,
            Lookuper mainTablePointLookuper,
            int[] idxColumnIndicesInMainRow,
            InternalRow.FieldGetter[] idxColumnGettersInMainRow,
            Function<InternalRow, InternalRow> basePkExtractorFromIndexRow) {
        this.indexTablePrefixLookuper =
                checkNotNull(indexTablePrefixLookuper, "indexTablePrefixLookuper");
        this.mainTablePointLookuper =
                checkNotNull(mainTablePointLookuper, "mainTablePointLookuper");
        this.idxColumnIndicesInMainRow =
                checkNotNull(idxColumnIndicesInMainRow, "idxColumnIndicesInMainRow").clone();
        this.idxColumnGettersInMainRow =
                checkNotNull(idxColumnGettersInMainRow, "idxColumnGettersInMainRow").clone();
        this.basePkExtractorFromIndexRow =
                checkNotNull(basePkExtractorFromIndexRow, "basePkExtractorFromIndexRow");
    }

    @Override
    public CompletableFuture<LookupResult> lookup(InternalRow lookupKey) {
        // Hop 1: prefix scan the Index Table for candidate (idxCols, basePK) rows.
        // Hop 2: fan out one point-get per candidate basePK against the main table and aggregate.
        // Recheck (P4T3) is intentionally not performed here.
        return indexTablePrefixLookuper.lookup(lookupKey).thenCompose(this::doHop2);
    }

    private CompletableFuture<LookupResult> doHop2(LookupResult hop1Result) {
        List<InternalRow> candidateIndexRows = hop1Result.getRowList();
        if (candidateIndexRows.isEmpty()) {
            return CompletableFuture.completedFuture(
                    new LookupResult(Collections.<InternalRow>emptyList()));
        }
        List<CompletableFuture<LookupResult>> mainFutures =
                new ArrayList<>(candidateIndexRows.size());
        for (InternalRow indexRow : candidateIndexRows) {
            InternalRow basePk = basePkExtractorFromIndexRow.apply(indexRow);
            mainFutures.add(mainTablePointLookuper.lookup(basePk));
        }
        return CompletableFuture.allOf(mainFutures.toArray(new CompletableFuture[0]))
                .thenApply(
                        v -> {
                            List<InternalRow> aggregated = new ArrayList<>();
                            for (CompletableFuture<LookupResult> f : mainFutures) {
                                LookupResult r = f.join();
                                // Empty list signals a deleted / missing main row -> skip.
                                aggregated.addAll(r.getRowList());
                            }
                            return new LookupResult(aggregated);
                        });
    }
}
