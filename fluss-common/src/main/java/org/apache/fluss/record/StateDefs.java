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

package org.apache.fluss.record;

import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * Defines predefined bucket state definitions and their associated serializers.
 *
 * <p>Each state definition has:
 *
 * <ul>
 *   <li>An ID for dictionary-based representation in checkpoint files
 *   <li>A description for documenting the actual purpose of the state
 *   <li>A key serializer for converting between key Java types and String representation
 *   <li>A value serializer for converting between value Java types and String representation
 * </ul>
 *
 * <p>Each state definition can only store keys and values of the same type.
 */
public enum StateDefs {
    /**
     * Stores the data bucket log offset corresponding to the last offset of an index
     * LogRecordBatch.
     *
     * <p>Key Type: TableBucket
     *
     * <p>Value Type: Long
     */
    DATA_BUCKET_OFFSET_OF_INDEX(
            1,
            "Tracks the data bucket log offset corresponding to the last offset of index LogRecordBatch",
            TableBucket.class,
            new TableBucketSerializer(),
            Long.class,
            new LongSerializer());

    private final int id;
    private final String description;
    private final Class<?> keyType;
    private final StateSerializer<?> keySerializer;
    private final Class<?> valueType;
    private final StateSerializer<?> valueSerializer;

    private static final Map<Integer, StateDefs> ID_TO_DEF = new HashMap<>();

    static {
        for (StateDefs def : StateDefs.values()) {
            ID_TO_DEF.put(def.id, def);
        }
    }

    <K, V> StateDefs(
            int id,
            String description,
            Class<K> keyType,
            StateSerializer<K> keySerializer,
            Class<V> valueType,
            StateSerializer<V> valueSerializer) {
        this.id = id;
        this.description = description;
        this.keyType = keyType;
        this.keySerializer = keySerializer;
        this.valueType = valueType;
        this.valueSerializer = valueSerializer;
    }

    /** Returns the unique ID for this state definition. */
    public int getId() {
        return id;
    }

    /** Returns the description for this state definition. */
    public String getDescription() {
        return description;
    }

    /** Returns the key type for this state definition. */
    @SuppressWarnings("unchecked")
    public <K> Class<K> getKeyType() {
        return (Class<K>) keyType;
    }

    /** Returns the key serializer for this state definition. */
    @SuppressWarnings("unchecked")
    public <K> StateSerializer<K> getKeySerializer() {
        return (StateSerializer<K>) keySerializer;
    }

    /** Returns the value type for this state definition. */
    @SuppressWarnings("unchecked")
    public <V> Class<V> getValueType() {
        return (Class<V>) valueType;
    }

    /** Returns the value serializer for this state definition. */
    @SuppressWarnings("unchecked")
    public <V> StateSerializer<V> getValueSerializer() {
        return (StateSerializer<V>) valueSerializer;
    }

    /** Looks up a state definition by its ID. */
    @Nullable
    public static StateDefs fromId(int id) {
        return ID_TO_DEF.get(id);
    }

    /**
     * Serializer interface for converting between Java types and String representation.
     *
     * @param <T> the Java type to serialize/deserialize
     */
    public interface StateSerializer<T> {
        /**
         * Serializes a value to its String representation.
         *
         * @param value the value to serialize
         * @return the String representation
         */
        String serialize(T value);

        /**
         * Deserializes a String representation back to the Java type.
         *
         * @param str the String representation
         * @return the deserialized value
         */
        T deserialize(String str);
    }

    /** Serializer for Long values. */
    private static class LongSerializer implements StateSerializer<Long> {
        @Override
        public String serialize(Long value) {
            return value.toString();
        }

        @Override
        public Long deserialize(String str) {
            return Long.parseLong(str);
        }
    }

    /**
     * Serializer for TableBucket values.
     *
     * <p>Format: "tableId:bucket" or "tableId:partitionId:bucket"
     *
     * <p>This format is compact and efficient for serialization while maintaining readability.
     */
    private static class TableBucketSerializer implements StateSerializer<TableBucket> {
        @Override
        public String serialize(TableBucket value) {
            if (value.getPartitionId() == null) {
                return value.getTableId() + ":" + value.getBucket();
            } else {
                return value.getTableId() + ":" + value.getPartitionId() + ":" + value.getBucket();
            }
        }

        @Override
        public TableBucket deserialize(String str) {
            String[] parts = str.split(":");
            if (parts.length == 2) {
                long tableId = Long.parseLong(parts[0]);
                int bucket = Integer.parseInt(parts[1]);
                return new TableBucket(tableId, bucket);
            } else if (parts.length == 3) {
                long tableId = Long.parseLong(parts[0]);
                long partitionId = Long.parseLong(parts[1]);
                int bucket = Integer.parseInt(parts[2]);
                return new TableBucket(tableId, partitionId, bucket);
            } else {
                throw new IllegalArgumentException("Invalid TableBucket serialized format: " + str);
            }
        }
    }
}
