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

/** State definitions enumeration containing all supported state types. */
public enum StateDefs {
    LAST_APPLIED_LOG_INDEX(0, "lastAppliedLogIndex", StateMergePolicy.LAST_VALUE);

    private final short id;
    private final String name;
    private final StateMergePolicy stateMergePolicy;
    private StateConverter converter;
    private StateSerde<Object> keySerde;
    private StateSerde<Object> valueSerde;

    StateDefs(int id, String name, StateMergePolicy stateMergePolicy) {
        this.id = (short) id;
        this.name = name;
        this.stateMergePolicy = stateMergePolicy;
    }

    public short getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public StateConverter getConverter() {
        return converter;
    }

    public StateMergePolicy getStateMergePolicy() {
        return stateMergePolicy;
    }

    public StateSerde<Object> getKeySerde() {
        return keySerde;
    }

    public StateSerde<Object> getValueSerde() {
        return valueSerde;
    }

    /**
     * Sets the state converter for this state definition.
     *
     * @param converter the state converter
     */
    public void setConverter(StateConverter converter) {
        this.converter = converter;
    }

    /**
     * Sets the key serializer for this state definition.
     *
     * @param keySerde the key serializer
     */
    public void setKeySerde(StateSerde<Object> keySerde) {
        this.keySerde = keySerde;
    }

    /**
     * Sets the value serializer for this state definition.
     *
     * @param valueSerde the value serializer
     */
    public void setValueSerde(StateSerde<Object> valueSerde) {
        this.valueSerde = valueSerde;
    }
}
