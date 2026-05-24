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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;

import java.util.ArrayList;
import java.util.List;

/**
 * Derives the {@link TableDescriptor} for every Index Table declared on a main table's {@link
 * Schema#getIndexes()}.
 *
 * <p>Used by {@code CoordinatorService.createTable} to auto-create Index Tables synchronously after
 * the main table is registered, so that callers issuing a single {@code CREATE TABLE ... INDEX}
 * statement do not have to manage Index Table lifecycle by hand.
 */
public final class IndexTableAutoDerive {

    private IndexTableAutoDerive() {}

    /**
     * Builds an Index Table {@link TableDescriptor} for every {@link Schema.Index} declared on the
     * main table. The result preserves the order of {@code mainDescriptor.getSchema().getIndexes()}
     * so callers can correlate the descriptors back to the originating index by parallel
     * iteration.
     *
     * <p>Returns an empty list when the main table declares no indexes.
     */
    public static List<TableDescriptor> deriveIndexTables(
            TableDescriptor mainDescriptor, long mainTableId, TablePath mainTablePath) {
        List<Schema.Index> indexes = mainDescriptor.getSchema().getIndexes();
        if (indexes.isEmpty()) {
            return new ArrayList<>(0);
        }
        String mainTableName =
                mainTablePath.getDatabaseName() + "." + mainTablePath.getTableName();
        List<TableDescriptor> result = new ArrayList<>(indexes.size());
        for (Schema.Index index : indexes) {
            result.add(
                    TableDescriptor.deriveIndexTableDescriptor(
                            mainDescriptor,
                            mainTableId,
                            mainTableName,
                            index.getIndexName()));
        }
        return result;
    }
}
