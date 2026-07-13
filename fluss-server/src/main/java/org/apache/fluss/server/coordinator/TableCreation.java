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

import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.data.TableAssignment;

import javax.annotation.Nullable;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** A table whose identifier has been allocated and whose metadata is ready to persist. */
final class TableCreation {

    private final TablePath tablePath;
    private final long tableId;
    private final TableDescriptor tableDescriptor;
    private final @Nullable TableAssignment tableAssignment;

    TableCreation(
            TablePath tablePath,
            long tableId,
            TableDescriptor tableDescriptor,
            @Nullable TableAssignment tableAssignment) {
        this.tablePath = checkNotNull(tablePath);
        this.tableId = tableId;
        this.tableDescriptor = checkNotNull(tableDescriptor);
        this.tableAssignment = tableAssignment;
    }

    TablePath getTablePath() {
        return tablePath;
    }

    long getTableId() {
        return tableId;
    }

    TableDescriptor getTableDescriptor() {
        return tableDescriptor;
    }

    @Nullable
    TableAssignment getTableAssignment() {
        return tableAssignment;
    }
}
