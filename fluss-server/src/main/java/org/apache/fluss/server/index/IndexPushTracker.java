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

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Per-data-bucket-leader tracker that gates {@code indexPushedOffset} advancement on acks from
 * index bucket leaders. Strictly monotonic — {@link #getIndexPushedOffset()} never decreases.
 * Thread-safe.
 *
 * <p>Lifecycle: {@link #register(long, Set)} announces a new source-WAL offset and the set of
 * Index Table buckets that must acknowledge its push. Each {@link #ack(long, Target)} removes one
 * target; once the set becomes empty, the offset is eligible to advance. The published value is
 * the largest contiguous offset (no gaps) whose target set is empty.
 */
@Internal
public final class IndexPushTracker {

    /** Identifier for an outstanding push: which Index Table bucket. */
    public static final class Target {
        private final long indexTableId;
        private final int indexBucket;

        public Target(long indexTableId, int indexBucket) {
            checkArgument(
                    indexBucket >= 0, "indexBucket must be non-negative, got %s", indexBucket);
            this.indexTableId = indexTableId;
            this.indexBucket = indexBucket;
        }

        public long getIndexTableId() {
            return indexTableId;
        }

        public int getIndexBucket() {
            return indexBucket;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Target)) {
                return false;
            }
            Target target = (Target) o;
            return indexTableId == target.indexTableId && indexBucket == target.indexBucket;
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexTableId, indexBucket);
        }

        @Override
        public String toString() {
            return "Target{" + indexTableId + ":" + indexBucket + "}";
        }
    }

    private final Object lock = new Object();
    private final TreeMap<Long, Set<Target>> pendingByOffset = new TreeMap<>();
    private long indexPushedOffset = -1L;

    public void register(long sourceOffset, Set<Target> targets) {
        checkNotNull(targets, "targets");
        synchronized (lock) {
            pendingByOffset.put(sourceOffset, new HashSet<>(targets));
        }
    }

    public long ack(long sourceOffset, Target target) {
        checkNotNull(target, "target");
        synchronized (lock) {
            Set<Target> remaining = pendingByOffset.get(sourceOffset);
            if (remaining != null) {
                remaining.remove(target);
            }
            return advanceLocked();
        }
    }

    public long getIndexPushedOffset() {
        synchronized (lock) {
            return indexPushedOffset;
        }
    }

    private long advanceLocked() {
        while (!pendingByOffset.isEmpty()) {
            Long head = pendingByOffset.firstKey();
            Set<Target> remaining = pendingByOffset.get(head);
            if (remaining.isEmpty()) {
                indexPushedOffset = head;
                pendingByOffset.remove(head);
            } else {
                break;
            }
        }
        return indexPushedOffset;
    }
}
