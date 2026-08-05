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

/** Immutable opaque 128-bit writer identity. */
public final class WriterKey {
    private final long high;
    private final long low;

    public WriterKey(long high, long low) {
        this.high = high;
        this.low = low;
    }

    public long high() {
        return high;
    }

    public long low() {
        return low;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof WriterKey)) {
            return false;
        }
        WriterKey that = (WriterKey) other;
        return high == that.high && low == that.low;
    }

    @Override
    public int hashCode() {
        return 31 * Long.hashCode(high) + Long.hashCode(low);
    }
}
