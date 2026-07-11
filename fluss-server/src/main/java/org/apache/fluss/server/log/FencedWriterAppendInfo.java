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

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.WriterKey;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

/** Prepared, one-shot V1 WriterState update published after its target WAL append succeeds. */
public final class FencedWriterAppendInfo {
    private final WriterKey writerKey;
    private final TableBucket tableBucket;
    @Nullable private final FencedWriterStateEntry currentEntry;
    @Nullable private FencedWriterStateEntry updatedEntry;
    private boolean published;

    FencedWriterAppendInfo(
            WriterKey writerKey,
            TableBucket tableBucket,
            @Nullable FencedWriterStateEntry currentEntry) {
        this.writerKey = Objects.requireNonNull(writerKey, "writerKey");
        this.tableBucket = Objects.requireNonNull(tableBucket, "tableBucket");
        this.currentEntry = currentEntry;
    }

    public WriterKey writerKey() {
        return writerKey;
    }

    public void append(long sequence, long targetWalOffset, long timestamp) {
        if (updatedEntry != null) {
            throw new IllegalStateException("A fenced writer update accepts exactly one append");
        }
        if (sequence < 0L) {
            throw new IllegalArgumentException("sequence must be non-negative");
        }
        if (currentEntry != null && sequence <= currentEntry.lastSequence()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Stale fenced sequence %s for writer %s; current sequence is %s",
                            sequence, writerKey, currentEntry.lastSequence()));
        }
        updatedEntry = new FencedWriterStateEntry(writerKey, sequence, targetWalOffset, timestamp);
    }

    public Optional<FencedWriterStateEntry> currentEntry() {
        return Optional.ofNullable(currentEntry);
    }

    public FencedWriterStateEntry updatedEntry() {
        if (updatedEntry == null) {
            throw new IllegalStateException("No fenced writer update has been appended");
        }
        return updatedEntry;
    }

    TableBucket tableBucket() {
        return tableBucket;
    }

    FencedWriterStateEntry takeUpdatedEntryForPublish() {
        if (published) {
            throw new IllegalStateException("Fenced writer update has already been published");
        }
        FencedWriterStateEntry entry = updatedEntry();
        published = true;
        return entry;
    }
}
