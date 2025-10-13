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
 * Versioned encoder for composite state keys with format:
 * [version(2bytes)][state_id(2bytes)][separator(1byte)][state_key_bytes].
 */
public class CompositeStateKeyEncoder {

    private static final byte SEPARATOR = (byte) 0xFF;

    /**
     * Encodes a composite state key with the current version format.
     *
     * @param stateDef the state definition containing the ID
     * @param stateKey the original state key object
     * @param keySerde the serializer for the state key
     * @param <K> the type of the state key
     * @return encoded composite key bytes
     * @throws IOException if key serialization fails
     */
    public static <K> byte[] encode(StateDefs stateDef, K stateKey, StateSerde<K> keySerde)
            throws IOException {
        return encode(
                StateSerdeVersions.CURRENT_COMPOSITE_KEY_VERSION, stateDef, stateKey, keySerde);
    }

    /**
     * Encodes a composite state key with the specified version format.
     *
     * @param version the encoding version to use
     * @param stateDef the state definition containing the ID
     * @param stateKey the original state key object
     * @param keySerde the serializer for the state key
     * @param <K> the type of the state key
     * @return encoded composite key bytes
     * @throws IOException if key serialization fails
     */
    public static <K> byte[] encode(
            short version, StateDefs stateDef, K stateKey, StateSerde<K> keySerde)
            throws IOException {
        if (!StateSerdeVersions.isSupportedCompositeKeyVersion(version)) {
            throw new IllegalArgumentException("Unsupported composite key version: " + version);
        }

        byte[] keyBytes = keySerde.serialize(stateKey);
        short stateId = stateDef.getId();

        switch (version) {
            case StateSerdeVersions.COMPOSITE_KEY_VERSION_1:
                return encodeV1(version, stateId, keyBytes);
            default:
                throw new IllegalArgumentException("Unsupported composite key version: " + version);
        }
    }

    /**
     * Encodes composite key using version 1 format:
     * [version(2bytes)][state_id(2bytes)][separator(1byte)][state_key_bytes].
     *
     * @param version the version short
     * @param stateId the state ID
     * @param keyBytes the serialized state key bytes
     * @return encoded composite key bytes
     */
    private static byte[] encodeV1(short version, short stateId, byte[] keyBytes) {
        // Allocate buffer: 2 bytes version + 2 bytes state ID + 1 byte separator + key bytes
        ByteBuffer buffer = ByteBuffer.allocate(2 + 2 + 1 + keyBytes.length);
        buffer.putShort(version);
        buffer.putShort(stateId);
        buffer.put(SEPARATOR);
        buffer.put(keyBytes);

        return buffer.array();
    }

    /**
     * Gets the separator byte used in composite keys.
     *
     * @return the separator byte
     */
    public static byte getSeparator() {
        return SEPARATOR;
    }

    /**
     * Gets the minimum length for a valid composite key.
     *
     * @return the minimum length (version + state_id + separator + at least 1 byte key)
     */
    public static int getMinCompositeKeyLength() {
        return 2 + 2 + 1 + 1; // version + state_id + separator + key
    }
}
