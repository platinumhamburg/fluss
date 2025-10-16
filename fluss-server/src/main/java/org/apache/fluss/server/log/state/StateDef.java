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

/** A definition of a state. */
public enum StateDef {
    INDEX_REPLICATE_OFFSET("index_apply_state");

    private static final String STATE_KAY_SEPARATOR = "__";

    private String namespace;

    private String stateKeyPrefix;

    StateDef(String namespace) {
        this.namespace = namespace;
        this.stateKeyPrefix = namespace + STATE_KAY_SEPARATOR;
    }

    public String namespace() {
        return namespace;
    }

    public String stateKey(String key) {
        return stateKeyPrefix + key;
    }

    public String stateKeyPrefix() {
        return stateKeyPrefix;
    }
}
