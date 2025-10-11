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

/** Version constants for state key and value encoding/decoding. */
public class StateSerdeVersions {

    /** Current version for composite state key encoding. */
    public static final short COMPOSITE_KEY_VERSION_1 = 1;

    /** Current version for state value encoding. */
    public static final short STATE_VALUE_VERSION_1 = 1;

    /** Default versions to use for new encodings. */
    public static final short CURRENT_COMPOSITE_KEY_VERSION = COMPOSITE_KEY_VERSION_1;

    public static final short CURRENT_STATE_VALUE_VERSION = STATE_VALUE_VERSION_1;

    /** Minimum supported version for backward compatibility. */
    public static final short MIN_SUPPORTED_COMPOSITE_KEY_VERSION = 1;

    public static final short MIN_SUPPORTED_STATE_VALUE_VERSION = 1;

    /**
     * Checks if the given composite key version is supported.
     *
     * @param version the version to check
     * @return true if supported, false otherwise
     */
    public static boolean isSupportedCompositeKeyVersion(short version) {
        return version >= MIN_SUPPORTED_COMPOSITE_KEY_VERSION
                && version <= CURRENT_COMPOSITE_KEY_VERSION;
    }

    /**
     * Checks if the given state value version is supported.
     *
     * @param version the version to check
     * @return true if supported, false otherwise
     */
    public static boolean isSupportedStateValueVersion(short version) {
        return version >= MIN_SUPPORTED_STATE_VALUE_VERSION
                && version <= CURRENT_STATE_VALUE_VERSION;
    }
}
