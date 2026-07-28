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

/** Latest cumulative progress accepted for a writer key. */
public final class WriterProgressStateEntry {
    private final WriterKey writerKey;
    private final long lastProgress;
    private final long progressWalOffset;
    private final long lastTimestamp;

    public WriterProgressStateEntry(
            WriterKey writerKey, long lastProgress, long progressWalOffset, long lastTimestamp) {
        if (lastProgress < 0L) {
            throw new IllegalArgumentException("lastProgress must be non-negative");
        }
        this.writerKey = Objects.requireNonNull(writerKey, "writerKey");
        this.lastProgress = lastProgress;
        this.progressWalOffset = progressWalOffset;
        this.lastTimestamp = lastTimestamp;
    }

    public WriterKey writerKey() {
        return writerKey;
    }

    public long lastProgress() {
        return lastProgress;
    }

    public long progressWalOffset() {
        return progressWalOffset;
    }

    public long lastTimestamp() {
        return lastTimestamp;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof WriterProgressStateEntry)) {
            return false;
        }
        WriterProgressStateEntry that = (WriterProgressStateEntry) other;
        return lastProgress == that.lastProgress
                && progressWalOffset == that.progressWalOffset
                && lastTimestamp == that.lastTimestamp
                && writerKey.equals(that.writerKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(writerKey, lastProgress, progressWalOffset, lastTimestamp);
    }

    @Override
    public String toString() {
        return "WriterProgressStateEntry{"
                + "writerKey="
                + writerKey
                + ", lastProgress="
                + lastProgress
                + ", progressWalOffset="
                + progressWalOffset
                + ", lastTimestamp="
                + lastTimestamp
                + '}';
    }
}
