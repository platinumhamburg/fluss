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

package org.apache.fluss.server.entity;

import org.apache.fluss.annotation.Internal;

import java.util.Objects;

/** The structure of index fetch request information. */
@Internal
public final class FetchIndexReqInfo {
    private final long fetchOffset;
    private final long indexCommitOffset;

    public FetchIndexReqInfo(long fetchOffset, long indexCommitOffset) {
        this.fetchOffset = fetchOffset;
        this.indexCommitOffset = indexCommitOffset;
    }

    public long getFetchOffset() {
        return fetchOffset;
    }

    public long getIndexCommitOffset() {
        return indexCommitOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FetchIndexReqInfo that = (FetchIndexReqInfo) o;
        return fetchOffset == that.fetchOffset && indexCommitOffset == that.indexCommitOffset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fetchOffset, indexCommitOffset);
    }

    @Override
    public String toString() {
        return "FetchIndexReqInfo{"
                + "fetchOffset="
                + fetchOffset
                + ", indexCommitOffset="
                + indexCommitOffset
                + '}';
    }
}
