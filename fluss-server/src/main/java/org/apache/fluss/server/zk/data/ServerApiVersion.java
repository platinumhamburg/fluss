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

package org.apache.fluss.server.zk.data;

import org.apache.fluss.annotation.Internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** An immutable API version range advertised by a registered Fluss server. */
@Internal
public final class ServerApiVersion implements Comparable<ServerApiVersion> {

    private final short apiKey;
    private final short minVersion;
    private final short maxVersion;

    /** Creates an API range for the given API key. */
    public ServerApiVersion(short apiKey, short minVersion, short maxVersion) {
        checkArgument(minVersion >= 0, "Minimum API version must be non-negative: %s.", minVersion);
        checkArgument(
                maxVersion >= minVersion,
                "Maximum API version %s must be at least minimum API version %s.",
                maxVersion,
                minVersion);
        this.apiKey = apiKey;
        this.minVersion = minVersion;
        this.maxVersion = maxVersion;
    }

    /** Returns the API key. */
    public short getApiKey() {
        return apiKey;
    }

    /** Returns the lowest supported version. */
    public short getMinVersion() {
        return minVersion;
    }

    /** Returns the highest supported version. */
    public short getMaxVersion() {
        return maxVersion;
    }

    @Override
    public int compareTo(ServerApiVersion other) {
        return Short.compare(apiKey, other.apiKey);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ServerApiVersion that = (ServerApiVersion) o;
        return apiKey == that.apiKey
                && minVersion == that.minVersion
                && maxVersion == that.maxVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(apiKey, minVersion, maxVersion);
    }

    @Override
    public String toString() {
        return "ServerApiVersion{"
                + "apiKey="
                + apiKey
                + ", minVersion="
                + minVersion
                + ", maxVersion="
                + maxVersion
                + '}';
    }

    static List<ServerApiVersion> copyAndValidate(List<ServerApiVersion> apiVersions) {
        checkNotNull(apiVersions, "API versions must not be null.");
        List<ServerApiVersion> copy = new ArrayList<>(apiVersions.size());
        ServerApiVersion previous = null;
        for (ServerApiVersion apiVersion : apiVersions) {
            checkNotNull(apiVersion, "API version must not be null.");
            checkArgument(
                    previous == null || previous.apiKey < apiVersion.apiKey,
                    "API versions must be strictly ascending by API key: %s then %s.",
                    previous == null ? "none" : previous.apiKey,
                    apiVersion.apiKey);
            copy.add(apiVersion);
            previous = apiVersion;
        }
        return Collections.unmodifiableList(copy);
    }
}
