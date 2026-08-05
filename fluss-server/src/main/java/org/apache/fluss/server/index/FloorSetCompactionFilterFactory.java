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
import org.apache.fluss.metadata.PartitionTombstone;
import org.apache.fluss.row.encode.KvValueLayout;

import org.rocksdb.AbstractCompactionFilter;
import org.rocksdb.AbstractCompactionFilterFactory;
import org.rocksdb.FloorSetCompactionFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.function.Supplier;

import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_3;

/**
 * Factory that creates per-compaction {@link FloorSetCompactionFilter} instances.
 *
 * <p>Attached to a RocksDB ColumnFamily for Index Tables that use KV format version 3, this factory
 * is called by RocksDB at the start of each compaction job. It snapshots the current tombstone
 * state via the supplied {@link Supplier} and passes tag offset, floor, and explicit set to the
 * native filter via JNI.
 *
 * <p>The tag is written at a fixed offset in the v3 value format ({@code
 * [schemaId(2)][tag(8)][BinaryRow]}), so the filter only needs the tag offset — no schema metadata
 * is required.
 *
 * <p>Thread-safety: RocksDB may call {@link #createCompactionFilter(Context)} from any compaction
 * thread. The supplier must be thread-safe.
 */
@Internal
public final class FloorSetCompactionFilterFactory
        extends AbstractCompactionFilterFactory<FloorSetCompactionFilter> {

    private static final Logger LOG =
            LoggerFactory.getLogger(FloorSetCompactionFilterFactory.class);

    private final int tagOffset;
    private final Supplier<PartitionTombstone> tombstoneSupplier;

    /**
     * Creates a factory that obtains the latest tombstone state on each compaction.
     *
     * @param tombstoneSupplier supplies the current {@link PartitionTombstone}; must be thread-safe
     */
    public FloorSetCompactionFilterFactory(Supplier<PartitionTombstone> tombstoneSupplier) {
        super();
        this.tagOffset = KvValueLayout.forKvFormatVersion(KV_FORMAT_VERSION_3).valueTagOffset();
        this.tombstoneSupplier = tombstoneSupplier;
    }

    @Override
    public FloorSetCompactionFilter createCompactionFilter(
            AbstractCompactionFilter.Context context) {
        PartitionTombstone tombstone = tombstoneSupplier.get();
        long floor = tombstone.getFloor();
        long[] explicitEntries = toLongArray(tombstone.getExplicitSet());
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Creating FloorSetCompactionFilter: floor={}, explicitCount={}",
                    floor,
                    explicitEntries.length);
        }
        return new FloorSetCompactionFilter(tagOffset, floor, explicitEntries);
    }

    @Override
    public String name() {
        return "FloorSetCompactionFilterFactory";
    }

    private static long[] toLongArray(Set<Long> set) {
        long[] arr = new long[set.size()];
        int i = 0;
        for (long v : set) {
            arr[i++] = v;
        }
        return arr;
    }
}
