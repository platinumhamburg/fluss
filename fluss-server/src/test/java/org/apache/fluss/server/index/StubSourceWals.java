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

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.log.FetchDataInfo;
import org.apache.fluss.server.log.FetchIsolation;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Test stubs for {@link IndexReplicator.SourceWal}. Every stub carries a distinct source {@link
 * TableBucket}, matching the production invariant that each replicator has a real source identity.
 */
final class StubSourceWals {

    private static final AtomicLong NEXT_SOURCE_TABLE_ID = new AtomicLong(7_000_000L);

    private StubSourceWals() {}

    /** A WAL stub with a unique source bucket that fails on any read access. */
    static IndexReplicator.SourceWal unreadable() {
        TableBucket source = new TableBucket(NEXT_SOURCE_TABLE_ID.getAndIncrement(), 0);
        return new IndexReplicator.SourceWal() {
            @Override
            public TableBucket tableBucket() {
                return source;
            }

            @Override
            public long highWatermark() {
                throw new IllegalStateException("stub WAL must not be read");
            }

            @Override
            public long logStartOffset() {
                throw new IllegalStateException("stub WAL must not be read");
            }

            @Override
            public FetchDataInfo read(
                    long offset, int maxBytes, FetchIsolation isolation, boolean minOneMessage) {
                throw new IllegalStateException("stub WAL must not be read");
            }
        };
    }
}
