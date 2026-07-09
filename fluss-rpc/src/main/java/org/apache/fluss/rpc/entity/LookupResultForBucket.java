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

package org.apache.fluss.rpc.entity;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.rpc.messages.LookupRequest;
import org.apache.fluss.rpc.protocol.ApiError;

import javax.annotation.Nullable;

import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Result of {@link LookupRequest} for each table bucket. */
public class LookupResultForBucket extends ResultForBucket {

    private final List<byte[]> values;
    @Nullable private final KvValueLayout kvValueLayout;
    @Nullable private final List<KvValueLayout> kvValueLayouts;

    /** Identifies the original partition for historical lookup; null for normal lookup. */
    private final @Nullable String originalPartitionName;

    public LookupResultForBucket(
            TableBucket tableBucket, List<byte[]> values, KvValueLayout kvValueLayout) {
        this(tableBucket, values, kvValueLayout, null, null, ApiError.NONE);
    }

    public LookupResultForBucket(TableBucket tableBucket, ApiError error) {
        this(tableBucket, null, null, null, null, error);
    }

    /** Creates a successful historical lookup result. */
    public LookupResultForBucket(
            TableBucket tableBucket,
            List<byte[]> values,
            KvValueLayout kvValueLayout,
            String originalPartitionName) {
        this(tableBucket, values, kvValueLayout, null, originalPartitionName, ApiError.NONE);
    }

    /** Creates a successful historical lookup result whose values use different layouts. */
    public LookupResultForBucket(
            TableBucket tableBucket,
            List<byte[]> values,
            List<KvValueLayout> kvValueLayouts,
            String originalPartitionName) {
        this(tableBucket, values, null, kvValueLayouts, originalPartitionName, ApiError.NONE);
    }

    /** Creates a failed historical lookup result. */
    public LookupResultForBucket(
            TableBucket tableBucket, String originalPartitionName, ApiError error) {
        this(tableBucket, null, null, null, originalPartitionName, error);
    }

    private LookupResultForBucket(
            TableBucket tableBucket,
            List<byte[]> values,
            @Nullable KvValueLayout kvValueLayout,
            @Nullable List<KvValueLayout> kvValueLayouts,
            @Nullable String originalPartitionName,
            ApiError error) {
        super(tableBucket, error);
        checkArgument(
                kvValueLayouts == null
                        || (values != null && kvValueLayouts.size() == values.size()),
                "A layout must be provided for every lookup value.");
        this.values = values;
        this.kvValueLayout = kvValueLayout;
        this.kvValueLayouts = kvValueLayouts;
        this.originalPartitionName = originalPartitionName;
    }

    public List<byte[]> lookupValues() {
        return values;
    }

    /** Returns the original partition name for historical lookup, or null for normal lookup. */
    public @Nullable String originalPartitionName() {
        return originalPartitionName;
    }

    /** Returns the shared value layout, or null for a failed lookup or mixed-layout values. */
    @Nullable
    public KvValueLayout getKvValueLayout() {
        return kvValueLayout;
    }

    /** Returns the physical layout of the value at the given index. */
    public KvValueLayout getKvValueLayout(int valueIndex) {
        return kvValueLayouts == null ? kvValueLayout : kvValueLayouts.get(valueIndex);
    }
}
