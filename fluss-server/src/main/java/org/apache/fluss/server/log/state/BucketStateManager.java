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
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.utils.CloseableIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** BucketStateManager. */
public class BucketStateManager {

    private final TableInfo tableInfo;

    private static final List<StateDef> HEADER_STATE_DEF_LIST = new ArrayList<>(4);

    private static final List<StateDef> RECORD_STATE_DEF_LIST = new ArrayList<>(4);

    static {
        for (StateDef def : StateDef.values()) {
            if (def.headerOnly()) {
                HEADER_STATE_DEF_LIST.add(def);
            } else {
                RECORD_STATE_DEF_LIST.add(def);
            }
        }
    }

    public BucketStateManager(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    private Map<String, String> currentState(StateDef def) {
        return new HashMap<>();
    }

    public Object getState(StateDef stateDef, String key) {
        return currentState(stateDef).get(key);
    }

    public void apply(Iterable<LogRecordBatch> batches, long firstOffset) {
        LogRecordReadContext readContext =
                LogRecordReadContext.createReadContext(tableInfo, false, null);
        long currentOffset = firstOffset;
        for (LogRecordBatch batch : batches) {
            for (StateDef stateDef : HEADER_STATE_DEF_LIST) {
                ((BatchStateTransition) (stateDef.transition()))
                        .apply(currentState(stateDef), batch);
            }

            try (CloseableIterator<LogRecord> recordIterator = batch.records(readContext)) {
                while (recordIterator.hasNext()) {
                    LogRecord record = recordIterator.next();
                    for (StateDef stateDef : RECORD_STATE_DEF_LIST) {
                        ((RecordStateTransition) (stateDef.transition()))
                                .apply(currentState(stateDef), record);
                    }
                    currentOffset++;
                }
            }
        }
    }

    public void commit(long offset) {}
}
