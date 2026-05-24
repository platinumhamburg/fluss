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

import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Two-hop {@link Lookuper} for a Global Secondary Index.
 *
 * <p>Hop 1: prefix-scan the Index Table by the provided {@code lookupKey} to obtain candidate
 * {@code basePK}s. Hop 2 (point-get against the main table) and the recheck step that filters
 * stale Index Table pointers are added in subsequent tasks (P4T2 and P4T3 respectively).
 *
 * <p>For P4T1 only the Hop 1 forwarding is implemented; the constructor still accepts the
 * dependencies for Hop 2 / recheck so that callers can be wired up early.
 */
@Internal
@NotThreadSafe
public final class SecondaryIndexLookuper implements Lookuper {

    private final Lookuper indexTablePrefixLookuper;
    private final Lookuper mainTablePointLookuper;
    private final int[] idxColumnIndicesInMainRow;
    private final InternalRow.FieldGetter[] idxColumnGettersInMainRow;

    public SecondaryIndexLookuper(
            Lookuper indexTablePrefixLookuper,
            Lookuper mainTablePointLookuper,
            int[] idxColumnIndicesInMainRow,
            InternalRow.FieldGetter[] idxColumnGettersInMainRow) {
        this.indexTablePrefixLookuper =
                checkNotNull(indexTablePrefixLookuper, "indexTablePrefixLookuper");
        this.mainTablePointLookuper =
                checkNotNull(mainTablePointLookuper, "mainTablePointLookuper");
        this.idxColumnIndicesInMainRow =
                checkNotNull(idxColumnIndicesInMainRow, "idxColumnIndicesInMainRow").clone();
        this.idxColumnGettersInMainRow =
                checkNotNull(idxColumnGettersInMainRow, "idxColumnGettersInMainRow").clone();
    }

    @Override
    public CompletableFuture<LookupResult> lookup(InternalRow lookupKey) {
        // Hop 1: prefix scan the Index Table. Hop 2 + recheck added in subsequent tasks.
        return indexTablePrefixLookuper.lookup(lookupKey);
    }
}
