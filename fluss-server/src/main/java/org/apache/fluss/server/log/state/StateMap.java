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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/** A map that stores the state of a log record. */
public class StateMap implements Map<String, String> {

    private final StateDef stateDef;

    private final TreeMap<String, String> underlyingMap;

    public StateMap(StateDef stateDef, TreeMap<String, String> underlyingMap) {
        this.stateDef = stateDef;
        this.underlyingMap = underlyingMap;
    }

    @Override
    public int size() {
        int count = 0;
        String prefix = stateDef.stateKeyPrefix();
        for (String key : underlyingMap.keySet()) {
            if (key.startsWith(prefix)) {
                count++;
            }
        }
        return count;
    }

    @Override
    public boolean isEmpty() {
        String prefix = stateDef.stateKeyPrefix();
        for (String key : underlyingMap.keySet()) {
            if (key.startsWith(prefix)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean containsKey(Object key) {
        return underlyingMap.containsKey(stateDef.stateKey((String) key));
    }

    @Override
    public boolean containsValue(Object value) {
        String prefix = stateDef.stateKeyPrefix();

        for (Map.Entry<String, String> entry : underlyingMap.entrySet()) {
            if (entry.getKey().startsWith(prefix) && Objects.equals(entry.getValue(), value)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String get(Object key) {
        return underlyingMap.get(stateDef.stateKey((String) key));
    }

    @Override
    public String put(String key, String value) {
        return underlyingMap.put(stateDef.stateKey(key), value);
    }

    @Override
    public String remove(Object key) {
        return underlyingMap.remove(stateDef.stateKey((String) key));
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
        for (Entry<? extends String, ? extends String> entry : m.entrySet()) {
            underlyingMap.put(stateDef.stateKey(entry.getKey()), entry.getValue());
        }
    }

    @Override
    public void clear() {
        underlyingMap
                .entrySet()
                .removeIf(entry -> entry.getKey().startsWith(stateDef.stateKeyPrefix()));
    }

    @Override
    public Set<String> keySet() {
        Set<String> result = new HashSet<>();
        String prefix = stateDef.stateKeyPrefix();
        int prefixLength = prefix.length();

        for (String key : underlyingMap.keySet()) {
            if (key.startsWith(prefix)) {
                result.add(key.substring(prefixLength));
            }
        }
        return result;
    }

    @Override
    public Collection<String> values() {
        List<String> result = new ArrayList<>();
        String prefix = stateDef.stateKeyPrefix();

        for (Map.Entry<String, String> entry : underlyingMap.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                result.add(entry.getValue());
            }
        }
        return result;
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        Set<Entry<String, String>> result = new HashSet<>();
        String prefix = stateDef.stateKeyPrefix();
        int prefixLength = prefix.length();

        for (Map.Entry<String, String> entry : underlyingMap.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                String originalKey = entry.getKey().substring(prefixLength);
                result.add(new AbstractMap.SimpleEntry<>(originalKey, entry.getValue()));
            }
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Map)) {
            return false;
        }

        Map<?, ?> other = (Map<?, ?>) o;
        if (size() != other.size()) {
            return false;
        }

        try {
            for (Entry<String, String> entry : entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                if (!Objects.equals(value, other.get(key))) {
                    return false;
                }
            }
        } catch (ClassCastException | NullPointerException unused) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int h = 0;
        for (Entry<String, String> entry : entrySet()) {
            h += Objects.hashCode(entry.getKey()) ^ Objects.hashCode(entry.getValue());
        }
        return h;
    }
}
