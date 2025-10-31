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

package org.apache.fluss.server.log.remote;

import org.apache.fluss.remote.RemoteLogSegment;

import javax.annotation.Nullable;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * This represents all the required data and indexed for a specific log segment that needs to be
 * stored in the tiered storage. is passed with {@link
 * RemoteLogStorage#copyLogSegmentFiles(RemoteLogSegment, LogSegmentFiles)} while copying a specific
 * log segment to the tiered storage.
 */
public class LogSegmentFiles {

    private final Path logSegment;
    private final Path offsetIndex;
    private final Path timeIndex;
    private final @Nullable Path writerIdIndex;
    private final @Nullable Path bucketStateSnapshot;
    // TODO add leader epoch index after introduce leader epoch.

    public LogSegmentFiles(
            Path logSegment,
            Path offsetIndex,
            Path timeIndex,
            @Nullable Path writerIdIndex,
            @Nullable Path bucketStateSnapshot) {
        this.logSegment = checkNotNull(logSegment, "logSegment can not be null");
        this.offsetIndex = checkNotNull(offsetIndex, "offsetIndex can not be null");
        this.timeIndex = checkNotNull(timeIndex, "timeIndex can not be null");
        this.writerIdIndex = writerIdIndex;
        this.bucketStateSnapshot = bucketStateSnapshot;
    }

    public Path logSegment() {
        return logSegment;
    }

    public Path offsetIndex() {
        return offsetIndex;
    }

    public Path timeIndex() {
        return timeIndex;
    }

    public @Nullable Path writerIdIndex() {
        return writerIdIndex;
    }

    public @Nullable Path bucketStateSnapshot() {
        return bucketStateSnapshot;
    }

    public List<Path> getAllPaths() {
        List<Path> paths = new ArrayList<>();
        paths.add(logSegment);
        paths.add(offsetIndex);
        paths.add(timeIndex);
        if (writerIdIndex != null) {
            paths.add(writerIdIndex);
        }
        if (bucketStateSnapshot != null) {
            paths.add(bucketStateSnapshot);
        }
        return paths;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogSegmentFiles that = (LogSegmentFiles) o;
        return Objects.equals(logSegment, that.logSegment)
                && Objects.equals(offsetIndex, that.offsetIndex)
                && Objects.equals(timeIndex, that.timeIndex)
                && Objects.equals(writerIdIndex, that.writerIdIndex)
                && Objects.equals(bucketStateSnapshot, that.bucketStateSnapshot);
    }

    @Override
    public int hashCode() {
        return Objects.hash(logSegment, offsetIndex, timeIndex, writerIdIndex, bucketStateSnapshot);
    }

    @Override
    public String toString() {
        return "LogSegmentData{"
                + "logSegment="
                + logSegment
                + ", offsetIndex="
                + offsetIndex
                + ", timeIndex="
                + timeIndex
                + ", writerIdIndex="
                + writerIdIndex
                + ", bucketStateSnapshot="
                + bucketStateSnapshot
                + '}';
    }
}
