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
 * Versioned decoder for composite state keys with format:
 * [version(2bytes)][state_id(2bytes)][separator(1byte)][state_key_bytes].
 */
public class CompositeStateKeyDecoder {

    private static final byte SEPARATOR = (byte) 0xFF;
    private static final int MIN_COMPOSITE_KEY_LENGTH =
            6; // 2 bytes version + 2 bytes state ID + 1 byte separator + 1 byte key

    /**
     * Represents a decoded composite key containing version, state ID and state key.
     *
     * @param <K> the type of the state key
     */
    public static class DecodedCompositeKey<K> {
        private final short version;
        private final short stateId;
        private final K stateKey;

        public DecodedCompositeKey(short version, short stateId, K stateKey) {
            this.version = version;
            this.stateId = stateId;
            this.stateKey = stateKey;
        }

        public short getVersion() {
            return version;
        }

        public short getStateId() {
            return stateId;
        }

        public K getStateKey() {
            return stateKey;
        }

        /**
         * Gets the StateDefs for this state ID.
         *
         * @return the corresponding StateDefs, or null if not found
         */
        public StateDefs getStateDef() {
            for (StateDefs stateDef : StateDefs.values()) {
                if (stateDef.getId() == stateId) {
                    return stateDef;
                }
            }
            return null;
        }
    }

    /**
     * Validates if the composite key has the correct format.
     *
     * @param compositeKey the composite key bytes to validate
     * @return true if the key format is valid, false otherwise
     */
    public static boolean isValidCompositeKey(byte[] compositeKey) {
        if (compositeKey == null || compositeKey.length < MIN_COMPOSITE_KEY_LENGTH) {
            return false;
        }

        // Check if separator is at the correct position (after version and state_id)
        return compositeKey[4] == SEPARATOR;
    }

    /**
     * Decodes the version from a composite key.
     *
     * @param compositeKey the composite key bytes
     * @return the version
     * @throws IllegalArgumentException if the composite key format is invalid
     */
    public static short decodeVersion(byte[] compositeKey) {
        validateCompositeKey(compositeKey);

        ByteBuffer buffer = ByteBuffer.wrap(compositeKey);
        return buffer.getShort();
    }

    /**
     * Decodes the state ID from a composite key.
     *
     * @param compositeKey the composite key bytes
     * @return the state ID
     * @throws IllegalArgumentException if the composite key format is invalid
     */
    public static short decodeStateId(byte[] compositeKey) {
        validateCompositeKey(compositeKey);

        ByteBuffer buffer = ByteBuffer.wrap(compositeKey);
        buffer.getShort(); // skip version
        return buffer.getShort();
    }

    /**
     * Decodes the original state key from a composite key.
     *
     * @param compositeKey the composite key bytes
     * @param keySerde the deserializer for the state key
     * @param <K> the type of the state key
     * @return the decoded state key
     * @throws IOException if key deserialization fails
     * @throws IllegalArgumentException if the composite key format is invalid
     */
    public static <K> K decodeStateKey(byte[] compositeKey, StateSerde<K> keySerde)
            throws IOException {
        validateCompositeKey(compositeKey);

        // Skip version (2 bytes) + state ID (2 bytes) + separator (1 byte)
        int keyStart = 5;
        byte[] keyBytes = new byte[compositeKey.length - keyStart];
        System.arraycopy(compositeKey, keyStart, keyBytes, 0, keyBytes.length);

        return keySerde.deserialize(keyBytes);
    }

    /**
     * Decodes version, state ID and state key from a composite key.
     *
     * @param compositeKey the composite key bytes
     * @param keySerde the deserializer for the state key
     * @param <K> the type of the state key
     * @return a DecodedCompositeKey containing version, state ID and state key
     * @throws IOException if key deserialization fails
     * @throws IllegalArgumentException if the composite key format is invalid
     */
    public static <K> DecodedCompositeKey<K> decode(byte[] compositeKey, StateSerde<K> keySerde)
            throws IOException {
        validateCompositeKey(compositeKey);

        short version = decodeVersion(compositeKey);
        short stateId = decodeStateId(compositeKey);
        K stateKey = decodeStateKey(compositeKey, keySerde);

        return new DecodedCompositeKey<>(version, stateId, stateKey);
    }

    /**
     * Decodes the state key using the StateDefs to automatically determine the correct serde.
     *
     * @param compositeKey the composite key bytes
     * @return a DecodedCompositeKey containing version, state ID and state key
     * @throws IOException if key deserialization fails or serde is not configured
     * @throws IllegalArgumentException if the composite key format is invalid or StateDefs not
     *     found
     */
    public static DecodedCompositeKey<Object> decodeWithStateDef(byte[] compositeKey)
            throws IOException {
        short version = decodeVersion(compositeKey);
        short stateId = decodeStateId(compositeKey);

        // Validate version compatibility
        if (!StateSerdeVersions.isSupportedCompositeKeyVersion(version)) {
            throw new IllegalArgumentException("Unsupported composite key version: " + version);
        }

        // Find the corresponding StateDefs
        StateDefs stateDef = null;
        for (StateDefs def : StateDefs.values()) {
            if (def.getId() == stateId) {
                stateDef = def;
                break;
            }
        }

        if (stateDef == null) {
            throw new IllegalArgumentException("Unknown state ID: " + stateId);
        }

        if (stateDef.getKeySerde() == null) {
            throw new IOException("Key serde not configured for state: " + stateDef.getName());
        }

        Object stateKey = decodeStateKey(compositeKey, stateDef.getKeySerde());
        return new DecodedCompositeKey<>(version, stateId, stateKey);
    }

    /**
     * Extracts the raw state key bytes from a composite key without deserialization.
     *
     * @param compositeKey the composite key bytes
     * @return the raw state key bytes
     * @throws IllegalArgumentException if the composite key format is invalid
     */
    public static byte[] extractStateKeyBytes(byte[] compositeKey) {
        validateCompositeKey(compositeKey);

        // Skip version (2 bytes) + state ID (2 bytes) + separator (1 byte)
        int keyStart = 5;
        byte[] keyBytes = new byte[compositeKey.length - keyStart];
        System.arraycopy(compositeKey, keyStart, keyBytes, 0, keyBytes.length);

        return keyBytes;
    }

    /**
     * Decodes composite key based on its version, supporting backward compatibility.
     *
     * @param compositeKey the composite key bytes
     * @param keySerde the deserializer for the state key
     * @param <K> the type of the state key
     * @return a DecodedCompositeKey containing version, state ID and state key
     * @throws IOException if key deserialization fails
     * @throws IllegalArgumentException if the composite key format is invalid or version
     *     unsupported
     */
    public static <K> DecodedCompositeKey<K> decodeVersioned(
            byte[] compositeKey, StateSerde<K> keySerde) throws IOException {
        short version = decodeVersion(compositeKey);

        if (!StateSerdeVersions.isSupportedCompositeKeyVersion(version)) {
            throw new IllegalArgumentException("Unsupported composite key version: " + version);
        }

        switch (version) {
            case StateSerdeVersions.COMPOSITE_KEY_VERSION_1:
                return decode(compositeKey, keySerde);
            default:
                throw new IllegalArgumentException("Unsupported composite key version: " + version);
        }
    }

    /**
     * Validates the composite key format.
     *
     * @param compositeKey the composite key to validate
     * @throws IllegalArgumentException if the format is invalid
     */
    private static void validateCompositeKey(byte[] compositeKey) {
        if (!isValidCompositeKey(compositeKey)) {
            throw new IllegalArgumentException(
                    "Invalid composite key format or length: "
                            + (compositeKey == null ? "null" : compositeKey.length));
        }
    }
}
