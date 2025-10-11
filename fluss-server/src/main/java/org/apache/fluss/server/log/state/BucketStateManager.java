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

import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.server.utils.FatalErrorHandler;

import java.io.IOException;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** TODO StateCheckpointManager. */
public class BucketStateManager {

    private RawStateStore rawStateStore;

    private long lastAppliedOffset;

    private long lastCheckpointOffset;

    private FatalErrorHandler fatalErrorHandler;

    public void checkPoint() {}

    public void apply(LogRecordBatch batch) {
        StateDefs[] stateDefs = StateDefs.values();
        for (StateDefs stateDef : stateDefs) {
            try {
                checkNotNull(stateDef.getConverter());
                checkNotNull(stateDef.getKeySerde());
                checkNotNull(stateDef.getValueSerde());

                Map<Object, Object> stateMap = stateDef.getConverter().convert(batch);
                for (Map.Entry<Object, Object> entry : stateMap.entrySet()) {
                    // Encode composite key: version + state_id + separator + state_key_bytes
                    byte[] compositeKey =
                            CompositeStateKeyEncoder.encode(
                                    stateDef, entry.getKey(), stateDef.getKeySerde());

                    // Encode value
                    byte[] valueBytes = stateDef.getValueSerde().serialize(entry.getValue());

                    // Apply merge policy
                    switch (stateDef.getStateMergePolicy()) {
                        case LAST_VALUE:
                            rawStateStore.put(compositeKey, valueBytes);
                            break;
                        case FIRST_VALUE:
                            // Only put if key doesn't exist
                            byte[] existingValue = rawStateStore.get(compositeKey);
                            if (existingValue == null) {
                                rawStateStore.put(compositeKey, valueBytes);
                            }
                            break;
                        default:
                            throw new UnsupportedOperationException(
                                    "Unsupported merge policy: " + stateDef.getStateMergePolicy());
                    }
                }
            } catch (IOException e) {
                fatalErrorHandler.onFatalError(e);
            }
        }
    }
}
