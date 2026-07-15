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

package org.apache.fluss.flink.action.orphan.job;

import org.apache.fluss.flink.action.orphan.audit.ScopeIdentity;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class CleanTaskScopeTest {

    @Test
    void bucketTaskRetainsTablePartitionAndBucketIdentity() {
        ScopeIdentity scope =
                ScopeIdentity.table("db", "orders", 7L).withPartitionAndBucket(11L, 3);

        CleanTask task =
                new BucketCleanTask(
                        scope,
                        "/log/tablet",
                        null,
                        Collections.emptySet(),
                        Collections.emptySet(),
                        Collections.emptySet(),
                        100L,
                        true,
                        false);

        assertThat(task.scope()).isEqualTo(scope);
    }

    @Test
    void orphanDirectoryTaskRetainsOwningScope() {
        ScopeIdentity scope = ScopeIdentity.orphanTable("db", "old_orders-5", 5L);

        CleanTask task =
                new OrphanDirCleanTask(scope, "/remote/db/old_orders-5", 100L, true, false);

        assertThat(task.scope()).isEqualTo(scope);
    }
}
