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

/** Prepared WriterState progress update published after its target WAL append succeeds. */
public final class WriterProgressAppendInfo {
    private final WriterKey writerKey;
    private final TableBucket tableBucket;
    @Nullable private final WriterProgressStateEntry currentEntry;
    @Nullable private WriterProgressStateEntry updatedEntry;
    private boolean published;

    WriterProgressAppendInfo(
            WriterKey writerKey,
            TableBucket tableBucket,
            @Nullable WriterProgressStateEntry currentEntry) {
        this.writerKey = Objects.requireNonNull(writerKey, "writerKey");
        this.tableBucket = Objects.requireNonNull(tableBucket, "tableBucket");
        this.currentEntry = currentEntry;
    }

    public WriterKey writerKey() {
        return writerKey;
    }

    public void append(long progress, long targetWalOffset, long timestamp) {
        if (updatedEntry != null) {
            throw new IllegalStateException("A writer progress update accepts exactly one append");
        }
        if (progress < 0L) {
            throw new IllegalArgumentException("writer progress must be non-negative");
        }
        if (currentEntry != null && progress <= currentEntry.lastProgress()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Stale progress %s for writer %s; current progress is %s",
                            progress, writerKey, currentEntry.lastProgress()));
        }
        updatedEntry =
                new WriterProgressStateEntry(writerKey, progress, targetWalOffset, timestamp);
    }

    public Optional<WriterProgressStateEntry> currentEntry() {
        return Optional.ofNullable(currentEntry);
    }

    public WriterProgressStateEntry updatedEntry() {
        if (updatedEntry == null) {
            throw new IllegalStateException("No writer progress update has been appended");
        }
        return updatedEntry;
    }

    TableBucket tableBucket() {
        return tableBucket;
    }

    WriterProgressStateEntry takeUpdatedEntryForPublish() {
        if (published) {
            throw new IllegalStateException("Writer progress update has already been published");
        }
        WriterProgressStateEntry entry = updatedEntry();
        published = true;
        return entry;
    }
}
