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

package org.apache.fluss.server.log.state;

import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;

import java.util.HashMap;
import java.util.Map;

/** BucketStateManager. */
public class BucketStateManager {

    private final TableInfo tableInfo;

    public BucketStateManager(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    private Map<String, byte[]> currentState(StateDef def) {
        return new HashMap<>();
    }

    public byte[] getState(StateDef stateDef, String key, boolean readCommitted) {
        return currentState(stateDef).get(key);
    }

    public void apply(Iterable<LogRecordBatch> batches, long firstOffset) {
        LogRecordReadContext readContext =
                LogRecordReadContext.createReadContext(tableInfo, false, null);
    }

    public void commit(long offset) {}
}
