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

package org.apache.fluss.server.index;

import org.apache.fluss.annotation.Internal;

import java.util.Objects;

/**
 * Parameters controlling how index data is fetched from the data bucket leader, including byte
 * limits, minimum offset advancement, and wait timeout.
 */
@Internal
public final class FetchIndexParams {

    /** Default max wait time in milliseconds for fetch requests. */
    public static final long DEFAULT_MAX_WAIT_MS = 500L;

    private final int maxBytes;
    private final int minAdvanceOffset;
    private final long maxWaitMs;

    public FetchIndexParams(int maxBytes, int minAdvanceOffset, long maxWaitMs) {
        this.maxBytes = maxBytes;
        this.minAdvanceOffset = minAdvanceOffset;
        this.maxWaitMs = maxWaitMs;
    }

    public int maxBytes() {
        return maxBytes;
    }

    public int minAdvanceOffset() {
        return minAdvanceOffset;
    }

    public long maxWaitMs() {
        return maxWaitMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FetchIndexParams that = (FetchIndexParams) o;
        return maxBytes == that.maxBytes
                && minAdvanceOffset == that.minAdvanceOffset
                && maxWaitMs == that.maxWaitMs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxBytes, minAdvanceOffset, maxWaitMs);
    }

    @Override
    public String toString() {
        return "FetchIndexParams("
                + "maxBytes="
                + maxBytes
                + ", minAdvanceOffset="
                + minAdvanceOffset
                + ", maxWaitMs="
                + maxWaitMs
                + ')';
    }
}
