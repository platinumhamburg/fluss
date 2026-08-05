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

package org.apache.fluss.row;

import org.apache.fluss.annotation.Internal;

import java.util.Optional;

import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_1;
import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_2;
import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_3;

/** Utility for resolving the key encoding version from table KV format metadata. */
@Internal
public final class KeyFormatVersion {

    private KeyFormatVersion() {}

    public static int resolve(Optional<Integer> kvFormatVersion) {
        return resolve(kvFormatVersion.orElse(KV_FORMAT_VERSION_1));
    }

    public static int resolve(int kvFormatVersion) {
        return kvFormatVersion == KV_FORMAT_VERSION_3 ? KV_FORMAT_VERSION_2 : kvFormatVersion;
    }
}
