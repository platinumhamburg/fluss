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

package org.apache.fluss.flink.sink.writer.undo;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.ByteArrayWrapper;

import java.util.HashSet;
import java.util.Set;

/**
 * Encapsulates the recovery state for a single bucket.
 *
 * <p>This class tracks:
 *
 * <ul>
 *   <li>The bucket being recovered
 *   <li>The checkpoint offset (start point for reading changelog)
 *   <li>The target offset (end point for reading changelog)
 *   <li>Processed primary keys for deduplication (streaming execution)
 *   <li>Progress tracking during changelog scanning
 * </ul>
 */
public class BucketRecoveryContext {

    private final TableBucket bucket;
    private final long checkpointOffset;
    private long targetOffset;

    private final Set<ByteArrayWrapper> processedKeys;
    private long lastProcessedOffset;
    private int totalRecordsProcessed;

    public BucketRecoveryContext(TableBucket bucket, long checkpointOffset) {
        this.bucket = bucket;
        this.checkpointOffset = checkpointOffset;
        this.targetOffset = -1;
        this.processedKeys = new HashSet<>();
        this.lastProcessedOffset = checkpointOffset;
        this.totalRecordsProcessed = 0;
    }

    public TableBucket getBucket() {
        return bucket;
    }

    public long getCheckpointOffset() {
        return checkpointOffset;
    }

    public long getTargetOffset() {
        return targetOffset;
    }

    public void setTargetOffset(long targetOffset) {
        this.targetOffset = targetOffset;
    }

    public Set<ByteArrayWrapper> getProcessedKeys() {
        return processedKeys;
    }

    /**
     * Checks if this bucket needs recovery.
     *
     * @return true if checkpoint offset is less than target offset
     */
    public boolean needsRecovery() {
        return checkpointOffset < targetOffset;
    }

    /**
     * Checks if changelog scanning is complete for this bucket.
     *
     * <p>Complete means either:
     *
     * <ul>
     *   <li>No recovery is needed (checkpointOffset >= targetOffset), or
     *   <li>We have processed records up to targetOffset - 1 (since offsets are 0-indexed and
     *       target is exclusive)
     * </ul>
     *
     * @return true if changelog scanning is complete
     */
    public boolean isComplete() {
        // If no recovery is needed, we're already complete
        if (!needsRecovery()) {
            return true;
        }
        // If no records have been processed yet, we're not complete
        if (totalRecordsProcessed == 0) {
            return false;
        }
        // We're complete when we've processed up to targetOffset - 1
        // (targetOffset is exclusive, so we need to read records from checkpointOffset to
        // targetOffset - 1)
        return lastProcessedOffset >= targetOffset - 1;
    }

    /**
     * Records that a changelog record has been processed.
     *
     * @param offset the offset of the processed record
     */
    public void recordProcessed(long offset) {
        lastProcessedOffset = offset;
        totalRecordsProcessed++;
    }

    public int getTotalRecordsProcessed() {
        return totalRecordsProcessed;
    }

    public long getLastProcessedOffset() {
        return lastProcessedOffset;
    }

    @Override
    public String toString() {
        return "BucketRecoveryContext{"
                + "bucket="
                + bucket
                + ", checkpointOffset="
                + checkpointOffset
                + ", targetOffset="
                + targetOffset
                + ", processedKeys="
                + processedKeys.size()
                + ", complete="
                + isComplete()
                + '}';
    }
}
