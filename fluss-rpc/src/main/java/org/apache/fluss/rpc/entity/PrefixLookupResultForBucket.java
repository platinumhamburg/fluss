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
import org.apache.fluss.rpc.messages.PrefixLookupRequest;
import org.apache.fluss.rpc.protocol.ApiError;

import javax.annotation.Nullable;

import java.util.List;

/** The Result of {@link PrefixLookupRequest} for each table bucket. */
public class PrefixLookupResultForBucket extends ResultForBucket {

    private final List<List<byte[]>> values;
    @Nullable private final KvValueLayout kvValueLayout;

    public PrefixLookupResultForBucket(
            TableBucket tableBucket, List<List<byte[]>> values, KvValueLayout kvValueLayout) {
        this(tableBucket, values, kvValueLayout, ApiError.NONE);
    }

    public PrefixLookupResultForBucket(TableBucket tableBucket, ApiError error) {
        this(tableBucket, null, null, error);
    }

    private PrefixLookupResultForBucket(
            TableBucket tableBucket,
            List<List<byte[]>> values,
            @Nullable KvValueLayout kvValueLayout,
            ApiError error) {
        super(tableBucket, error);
        this.values = values;
        this.kvValueLayout = kvValueLayout;
    }

    public List<List<byte[]>> prefixLookupValues() {
        return values;
    }

    /** Returns the storage layout for a successful prefix lookup result. */
    @Nullable
    public KvValueLayout getKvValueLayout() {
        return kvValueLayout;
    }
}
