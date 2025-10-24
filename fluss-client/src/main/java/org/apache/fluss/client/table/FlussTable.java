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
import org.apache.fluss.client.lookup.TableLookup;
import org.apache.fluss.client.table.scanner.Scan;
import org.apache.fluss.client.table.scanner.TableScan;
import org.apache.fluss.client.table.writer.Append;
import org.apache.fluss.client.table.writer.TableAppend;
import org.apache.fluss.client.table.writer.TableUpsert;
import org.apache.fluss.client.table.writer.Upsert;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.IndexTableUtils;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

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
    private final Configuration conf;

    public FlussTable(
            FlussConnection conn, TablePath tablePath, TableInfo tableInfo, Configuration conf) {
        this.conn = conn;
        this.tablePath = tablePath;
        this.tableInfo = tableInfo;
        this.hasPrimaryKey = tableInfo.hasPrimaryKey();
        this.conf = conf;
    }

    /**
     * Check if this table is an index table based on the table name. Index table name format: "__"
     * + mainTableName + "__" + "index" + "__" + indexName.
     *
     * @return true if this is an index table, false otherwise
     */
    @VisibleForTesting
    boolean isIndexTable() {
        return IndexTableUtils.isIndexTable(tablePath.getTableName());
    }

    @Override
    public TableInfo getTableInfo() {
        return tableInfo;
    }

    @Override
    public Scan newScan() {
        return new TableScan(conn, tableInfo);
    }

    @Override
    public Lookup newLookup() {
        return new TableLookup(
                tableInfo, conn.getMetadataUpdater(), conn.getOrCreateLookupClient(), conf);
    }

    @Override
    public Append newAppend() {
        checkState(
                !hasPrimaryKey,
                "Table %s is not a Log Table and doesn't support AppendWriter.",
                tablePath);
        checkState(
                !isIndexTable(),
                "Index table %s doesn't support AppendWriter. Index tables can only be updated by internal roles.",
                tablePath);
        return new TableAppend(tablePath, tableInfo, conn.getOrCreateWriterClient());
    }

    @Override
    public Upsert newUpsert() {
        checkState(
                hasPrimaryKey,
                "Table %s is not a Primary Key Table and doesn't support UpsertWriter.",
                tablePath);
        checkState(
                !isIndexTable(),
                "Index table %s doesn't support UpsertWriter. Index tables can only be updated by internal roles.",
                tablePath);
        return new TableUpsert(tablePath, tableInfo, conn.getOrCreateWriterClient());
    }

    @Override
    public void close() throws Exception {
        // do nothing
    }
}
