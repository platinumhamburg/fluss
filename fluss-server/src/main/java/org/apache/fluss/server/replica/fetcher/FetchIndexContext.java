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

package org.apache.fluss.server.replica.fetcher;

import org.apache.fluss.metadata.DataIndexTableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.FetchIndexRequest;

import java.util.Map;
import java.util.Set;

/** FetchIndexContext to fetch index from data bucket leader. */
public class FetchIndexContext {
    private final Map<Long, TablePath> tableIdToTablePath;
    private final FetchIndexRequest fetchIndexRequest;
    private Set<DataIndexTableBucket> requestDataIndexBuckets;

    public FetchIndexContext(
            Map<Long, TablePath> tableIdToTablePath,
            FetchIndexRequest fetchIndexRequest,
            Set<DataIndexTableBucket> requestDataIndexBuckets) {
        this.tableIdToTablePath = tableIdToTablePath;
        this.fetchIndexRequest = fetchIndexRequest;
        this.requestDataIndexBuckets = requestDataIndexBuckets;
    }

    public FetchIndexRequest getFetchIndexRequest() {
        return fetchIndexRequest;
    }

    public TablePath getTablePath(long tableId) {
        return tableIdToTablePath.get(tableId);
    }

    public Set<DataIndexTableBucket> getRequestDataIndexBuckets() {
        return requestDataIndexBuckets;
    }
}
