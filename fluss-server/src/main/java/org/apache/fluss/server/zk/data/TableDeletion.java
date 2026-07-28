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

package org.apache.fluss.server.zk.data;

import org.apache.fluss.metadata.TablePath;

import java.util.Objects;

/** The persisted identity of a table selected for deletion. */
public final class TableDeletion {

    private final TablePath tablePath;
    private final long tableId;

    public TableDeletion(TablePath tablePath, long tableId) {
        this.tablePath = tablePath;
        this.tableId = tableId;
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    public long getTableId() {
        return tableId;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof TableDeletion)) {
            return false;
        }
        TableDeletion that = (TableDeletion) object;
        return tableId == that.tableId && Objects.equals(tablePath, that.tablePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tablePath, tableId);
    }

    @Override
    public String toString() {
        return "TableDeletion{" + "tablePath=" + tablePath + ", tableId=" + tableId + '}';
    }
}
