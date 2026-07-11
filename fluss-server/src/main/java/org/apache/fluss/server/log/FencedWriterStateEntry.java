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

package org.apache.fluss.server.log;

import org.apache.fluss.record.WriterKey;

import java.util.Objects;

/** Latest accepted fence for an opaque V1 writer key. */
public final class FencedWriterStateEntry {
    private final WriterKey writerKey;
    private final long lastSequence;
    private final long dominatingTargetWalOffset;
    private final long lastTimestamp;

    public FencedWriterStateEntry(
            WriterKey writerKey,
            long lastSequence,
            long dominatingTargetWalOffset,
            long lastTimestamp) {
        if (lastSequence < 0L) {
            throw new IllegalArgumentException("lastSequence must be non-negative");
        }
        this.writerKey = Objects.requireNonNull(writerKey, "writerKey");
        this.lastSequence = lastSequence;
        this.dominatingTargetWalOffset = dominatingTargetWalOffset;
        this.lastTimestamp = lastTimestamp;
    }

    public WriterKey writerKey() {
        return writerKey;
    }

    public long lastSequence() {
        return lastSequence;
    }

    public long dominatingTargetWalOffset() {
        return dominatingTargetWalOffset;
    }

    public long lastTimestamp() {
        return lastTimestamp;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof FencedWriterStateEntry)) {
            return false;
        }
        FencedWriterStateEntry that = (FencedWriterStateEntry) other;
        return lastSequence == that.lastSequence
                && dominatingTargetWalOffset == that.dominatingTargetWalOffset
                && lastTimestamp == that.lastTimestamp
                && writerKey.equals(that.writerKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(writerKey, lastSequence, dominatingTargetWalOffset, lastTimestamp);
    }

    @Override
    public String toString() {
        return "FencedWriterStateEntry{"
                + "writerKey="
                + writerKey
                + ", lastSequence="
                + lastSequence
                + ", dominatingTargetWalOffset="
                + dominatingTargetWalOffset
                + ", lastTimestamp="
                + lastTimestamp
                + '}';
    }
}
