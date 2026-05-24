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

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.FlussConnection;
import org.apache.fluss.client.lookup.Lookup;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.lookup.SecondaryIndexLookuper;
import org.apache.fluss.client.lookup.TableLookup;
import org.apache.fluss.client.metadata.ClientSchemaGetter;
import org.apache.fluss.client.table.scanner.Scan;
import org.apache.fluss.client.table.scanner.TableScan;
import org.apache.fluss.client.table.writer.Append;
import org.apache.fluss.client.table.writer.TableAppend;
import org.apache.fluss.client.table.writer.TableUpsert;
import org.apache.fluss.client.table.writer.Upsert;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.IndexTableUtils;

import java.util.List;
import java.util.function.Function;

import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * The base impl of {@link Table}.
 *
 * @since 0.1
 */
@PublicEvolving
public class FlussTable implements Table {

    private final FlussConnection conn;
    private final TablePath tablePath;
    private final TableInfo tableInfo;
    private final boolean hasPrimaryKey;
    private final SchemaGetter schemaGetter;

    public FlussTable(FlussConnection conn, TablePath tablePath, TableInfo tableInfo) {
        this.conn = conn;
        this.tablePath = tablePath;
        this.tableInfo = tableInfo;
        this.hasPrimaryKey = tableInfo.hasPrimaryKey();
        this.schemaGetter =
                new ClientSchemaGetter(tablePath, tableInfo.getSchemaInfo(), conn.getAdmin());
    }

    @Override
    public TableInfo getTableInfo() {
        return tableInfo;
    }

    @Override
    public Scan newScan() {
        return new TableScan(conn, tableInfo, schemaGetter);
    }

    @Override
    public Lookup newLookup() {
        return new TableLookup(
                tableInfo, schemaGetter, conn.getMetadataUpdater(), conn.getOrCreateLookupClient());
    }

    @Override
    public Append newAppend() {
        checkState(
                !hasPrimaryKey,
                "Table %s is not a Log Table and doesn't support AppendWriter.",
                tablePath);
        return new TableAppend(tablePath, tableInfo, conn.getOrCreateWriterClient());
    }

    @Override
    public Upsert newUpsert() {
        checkState(
                hasPrimaryKey,
                "Table %s is not a Primary Key Table and doesn't support UpsertWriter.",
                tablePath);
        return new TableUpsert(tablePath, tableInfo, conn.getOrCreateWriterClient());
    }

    /**
     * Returns a {@link Lookuper} for the named secondary index that performs a two-hop lookup with
     * stale-pointer recheck (FIP V2 §2.7):
     *
     * <ul>
     *   <li>Hop 1: prefix-scan the Index Table by the user-provided index-column key to retrieve
     *       candidate {@code (idxCols, basePK)} rows.
     *   <li>Hop 2: point-get the main table for each candidate {@code basePK}. Rows that have been
     *       deleted upstream are dropped (empty main lookup result).
     *   <li>Recheck: re-evaluate the index columns of every surviving main row against the original
     *       lookup key; rows that no longer match are discarded as stale index pointers.
     * </ul>
     *
     * <p>The returned lookuper is {@link javax.annotation.concurrent.NotThreadSafe}; callers should
     * obtain a separate instance per concurrent caller (mirroring the contract of {@link
     * Lookup#createLookuper()}).
     *
     * @param indexName the name of the secondary index as declared via {@code Schema.Builder.index}
     * @return a {@link SecondaryIndexLookuper} wired against this table and its Index Table
     * @throws IllegalArgumentException if no secondary index with the given name is declared on
     *     this table.
     */
    public Lookuper getSecondaryIndexLookuper(String indexName) {
        Schema mainSchema = tableInfo.getSchema();
        Schema.Index index = findIndexOrThrow(mainSchema, tablePath, indexName);
        List<String> idxCols = index.getColumnNames();

        // Resolve the Index Table handle via the connection.
        TablePath indexTablePath =
                TablePath.of(
                        tablePath.getDatabaseName(),
                        IndexTableUtils.indexTableName(tablePath.getTableName(), indexName));
        Table indexTable = conn.getTable(indexTablePath);

        // Hop 1: PrefixKeyLookuper on the Index Table by the index columns.
        Lookuper indexTablePrefixLookuper =
                indexTable.newLookup().lookupBy(idxCols).createLookuper();

        // Hop 2: PrimaryKeyLookuper on this (main) table.
        Lookuper mainTablePointLookuper = this.newLookup().createLookuper();

        // Recheck wiring: index-column positions / getters in the lookup-key row vs the main row.
        RowType mainRowType = mainSchema.getRowType();
        int n = idxCols.size();
        int[] idxColumnIndicesInMainRow = new int[n];
        InternalRow.FieldGetter[] idxColumnGettersInLookupKey = new InternalRow.FieldGetter[n];
        InternalRow.FieldGetter[] idxColumnGettersInMainRow = new InternalRow.FieldGetter[n];
        for (int i = 0; i < n; i++) {
            int posInMain = mainRowType.getFieldIndex(idxCols.get(i));
            idxColumnIndicesInMainRow[i] = posInMain;
            DataType t = mainRowType.getTypeAt(posInMain);
            // The lookup-key row carries idx columns in declaration order at positions 0..n-1.
            idxColumnGettersInLookupKey[i] = InternalRow.createFieldGetter(t, i);
            // The main row carries each idx column at its position in the main schema.
            idxColumnGettersInMainRow[i] = InternalRow.createFieldGetter(t, posInMain);
        }

        // basePK extractor: copy base-PK columns out of the Index Table row into a fresh
        // GenericRow whose layout matches the main table's primary-key columns (and therefore the
        // shape expected by the PrimaryKeyLookuper above). Index Table column ordering can dedup
        // overlap between idx columns and PK columns (see deriveIndexTableDescriptor), so we look
        // up each base-PK position by name in the Index Table's RowType.
        RowType indexRowType = indexTable.getTableInfo().getSchema().getRowType();
        List<String> basePk = mainSchema.getPrimaryKeyColumnNames();
        int pkSize = basePk.size();
        final InternalRow.FieldGetter[] basePkGettersInIndexRow =
                new InternalRow.FieldGetter[pkSize];
        for (int i = 0; i < pkSize; i++) {
            int posInIndex = indexRowType.getFieldIndex(basePk.get(i));
            basePkGettersInIndexRow[i] =
                    InternalRow.createFieldGetter(indexRowType.getTypeAt(posInIndex), posInIndex);
        }
        Function<InternalRow, InternalRow> basePkExtractor =
                indexRow -> {
                    GenericRow pk = new GenericRow(pkSize);
                    for (int i = 0; i < pkSize; i++) {
                        pk.setField(i, basePkGettersInIndexRow[i].getFieldOrNull(indexRow));
                    }
                    return pk;
                };

        return new SecondaryIndexLookuper(
                indexTablePrefixLookuper,
                mainTablePointLookuper,
                idxColumnIndicesInMainRow,
                idxColumnGettersInLookupKey,
                idxColumnGettersInMainRow,
                basePkExtractor);
    }

    /**
     * Looks up an {@link Schema.Index} by name in the given main-table schema. Extracted from
     * {@link #getSecondaryIndexLookuper(String)} for unit-test coverage of the validation step
     * without standing up a {@link FlussConnection}.
     */
    @VisibleForTesting
    static Schema.Index findIndexOrThrow(Schema schema, TablePath tablePath, String indexName) {
        for (Schema.Index i : schema.getIndexes()) {
            if (i.getIndexName().equals(indexName)) {
                return i;
            }
        }
        throw new IllegalArgumentException(
                "Unknown secondary index '" + indexName + "' on table " + tablePath);
    }

    @Override
    public void close() throws Exception {
        // do nothing
        schemaGetter.release();
    }
}
