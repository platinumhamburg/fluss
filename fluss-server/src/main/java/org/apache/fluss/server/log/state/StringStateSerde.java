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
import java.nio.charset.StandardCharsets;

/** Versioned string serializer/deserializer for state keys and values. */
public class StringStateSerde implements VersionedStateSerde<String> {

    @Override
    public byte[] serializeValue(short version, String data) throws IOException {
        if (data == null) {
            return new byte[0];
        }

        switch (version) {
            case StateSerdeVersions.STATE_VALUE_VERSION_1:
                return data.getBytes(StandardCharsets.UTF_8);
            default:
                throw new IllegalArgumentException("Unsupported string serde version: " + version);
        }
    }

    @Override
    public String deserializeValue(short version, byte[] valueBytes) throws IOException {
        if (valueBytes == null || valueBytes.length == 0) {
            return null;
        }

        switch (version) {
            case StateSerdeVersions.STATE_VALUE_VERSION_1:
                return new String(valueBytes, StandardCharsets.UTF_8);
            default:
                throw new IllegalArgumentException("Unsupported string serde version: " + version);
        }
    }
}
