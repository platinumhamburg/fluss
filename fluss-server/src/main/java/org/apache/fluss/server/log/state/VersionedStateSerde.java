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

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Versioned serializer and deserializer for state values with format:
 * [version(2bytes)][value_bytes].
 *
 * @param <T> the type of objects to serialize/deserialize
 */
public interface VersionedStateSerde<T> extends StateSerde<T> {

    /**
     * Serializes an object to versioned byte array using the current version.
     *
     * @param data the object to serialize
     * @return versioned serialized byte array
     * @throws IOException if serialization fails
     */
    @Override
    default byte[] serialize(T data) throws IOException {
        return serialize(StateSerdeVersions.CURRENT_STATE_VALUE_VERSION, data);
    }

    /**
     * Serializes an object to versioned byte array with the specified version.
     *
     * @param version the version to use for serialization
     * @param data the object to serialize
     * @return versioned serialized byte array
     * @throws IOException if serialization fails
     */
    default byte[] serialize(short version, T data) throws IOException {
        if (!StateSerdeVersions.isSupportedStateValueVersion(version)) {
            throw new IllegalArgumentException("Unsupported state value version: " + version);
        }

        byte[] valueBytes = serializeValue(version, data);

        // Create versioned format: [version(2bytes)][value_bytes]
        ByteBuffer buffer = ByteBuffer.allocate(2 + valueBytes.length);
        buffer.putShort(version);
        buffer.put(valueBytes);

        return buffer.array();
    }

    /**
     * Deserializes versioned byte array to object, automatically detecting version.
     *
     * @param bytes the versioned byte array to deserialize
     * @return deserialized object
     * @throws IOException if deserialization fails
     */
    @Override
    default T deserialize(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length < 2) {
            throw new IllegalArgumentException("Invalid versioned value: too short");
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        short version = buffer.getShort();

        if (!StateSerdeVersions.isSupportedStateValueVersion(version)) {
            throw new IllegalArgumentException("Unsupported state value version: " + version);
        }

        // Extract value bytes (skip version)
        byte[] valueBytes = new byte[bytes.length - 2];
        buffer.get(valueBytes);

        return deserializeValue(version, valueBytes);
    }

    /**
     * Serializes the value without version prefix.
     *
     * @param version the version being used
     * @param data the object to serialize
     * @return serialized value bytes (without version)
     * @throws IOException if serialization fails
     */
    byte[] serializeValue(short version, T data) throws IOException;

    /**
     * Deserializes the value bytes based on the version.
     *
     * @param version the version used for serialization
     * @param valueBytes the value bytes to deserialize (without version)
     * @return deserialized object
     * @throws IOException if deserialization fails
     */
    T deserializeValue(short version, byte[] valueBytes) throws IOException;

    /**
     * Gets the version from a versioned byte array.
     *
     * @param bytes the versioned byte array
     * @return the version
     * @throws IllegalArgumentException if the byte array is invalid
     */
    static short getVersion(byte[] bytes) {
        if (bytes == null || bytes.length < 2) {
            throw new IllegalArgumentException("Invalid versioned value: too short");
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return buffer.getShort();
    }

    /**
     * Extracts the value bytes from a versioned byte array.
     *
     * @param bytes the versioned byte array
     * @return the value bytes (without version)
     * @throws IllegalArgumentException if the byte array is invalid
     */
    static byte[] extractValueBytes(byte[] bytes) {
        if (bytes == null || bytes.length < 2) {
            throw new IllegalArgumentException("Invalid versioned value: too short");
        }

        byte[] valueBytes = new byte[bytes.length - 2];
        System.arraycopy(bytes, 2, valueBytes, 0, valueBytes.length);
        return valueBytes;
    }
}
