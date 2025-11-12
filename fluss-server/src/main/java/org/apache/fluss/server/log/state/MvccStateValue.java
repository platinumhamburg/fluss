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

import org.apache.fluss.record.ChangeType;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.fluss.utils.concurrent.LockUtils.inReadLock;
import static org.apache.fluss.utils.concurrent.LockUtils.inWriteLock;

/**
 * This class represents the state of a bucket.
 *
 * <p>It is used to store the state of a bucket in memory.
 *
 * <p>It is thread-safe.
 */
public class MvccStateValue {

    private InnerValue imageValue;
    private long imageOffset = -1;

    private final TreeMap<Long, InnerValue> deltaValues;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public MvccStateValue() {
        imageValue = null;
        deltaValues = new TreeMap<>();
    }

    public MvccStateValue(Object value) {
        imageValue = new InnerValue(ChangeType.INSERT, value);
        deltaValues = new TreeMap<>();
    }

    public @Nullable Object getValue(boolean readCommitted) {
        return inReadLock(
                lock,
                () -> {
                    if (readCommitted || deltaValues.isEmpty()) {
                        return null == imageValue ? null : imageValue.value;
                    }
                    Map.Entry<Long, InnerValue> lastChange = deltaValues.lastEntry();
                    switch (lastChange.getValue().changeType) {
                        case INSERT:
                        case UPDATE_AFTER:
                            return lastChange.getValue().value;
                        case DELETE:
                            return null;
                        default:
                            throw new IllegalArgumentException(
                                    "Unexpected change type " + lastChange.getValue().changeType);
                    }
                });
    }

    /**
     * Get value with its log offset.
     *
     * @param readCommitted whether to read committed value or uncommitted value
     * @return ValueWithOffset containing the value and its log offset, or null if no value exists
     */
    public @Nullable ValueWithOffset getValueWithOffset(boolean readCommitted) {
        return inReadLock(
                lock,
                () -> {
                    if (readCommitted || deltaValues.isEmpty()) {
                        if (null == imageValue) {
                            return null;
                        }
                        // Check if the committed value is a DELETE
                        if (imageValue.changeType == ChangeType.DELETE) {
                            return null;
                        }
                        return new ValueWithOffset(imageValue.value, imageOffset);
                    }
                    Map.Entry<Long, InnerValue> lastChange = deltaValues.lastEntry();
                    switch (lastChange.getValue().changeType) {
                        case INSERT:
                        case UPDATE_AFTER:
                            return new ValueWithOffset(
                                    lastChange.getValue().value, lastChange.getKey());
                        case DELETE:
                            return null;
                        default:
                            throw new IllegalArgumentException(
                                    "Unexpected change type " + lastChange.getValue().changeType);
                    }
                });
    }

    public void apply(long offset, ChangeType changeType, Object value) {
        inWriteLock(
                lock,
                () -> {
                    deltaValues.put(offset, new InnerValue(changeType, value));
                });
    }

    public void commitTo(long offset) {
        inWriteLock(
                lock,
                () -> {
                    Map.Entry<Long, InnerValue> floorEntry = deltaValues.floorEntry(offset);
                    if (floorEntry != null) {
                        imageValue = floorEntry.getValue();
                        imageOffset = floorEntry.getKey();
                        deltaValues.headMap(offset, true).clear();
                    }
                });
    }

    public boolean isValid() {
        return inReadLock(lock, () -> imageValue != null || !deltaValues.isEmpty());
    }

    /** Wrapper class for value with its log offset. */
    public static class ValueWithOffset {
        private final Object value;
        private final long offset;

        public ValueWithOffset(Object value, long offset) {
            this.value = value;
            this.offset = offset;
        }

        public Object getValue() {
            return value;
        }

        public long getOffset() {
            return offset;
        }
    }

    private static class InnerValue {

        public InnerValue(ChangeType changeType, Object value) {
            this.changeType = changeType;
            this.value = value;
        }

        private final ChangeType changeType;
        private final Object value;
    }
}
