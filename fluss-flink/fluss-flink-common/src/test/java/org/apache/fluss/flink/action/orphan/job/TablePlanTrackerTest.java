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
import org.apache.fluss.flink.action.orphan.audit.SkipReasonCode;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

class TablePlanTrackerTest {

    @Test
    void retainsTablesWithNoTasksAndStableSkipReasons() {
        ScopeIdentity orders = ScopeIdentity.table("db", "orders", 7L);
        TablePlanTracker tracker = new TablePlanTracker();

        tracker.ensure(orders);
        tracker.task(orders.withPartitionAndBucket(11L, 3));
        tracker.metadataFailure(orders);
        tracker.skip(orders, SkipReasonCode.NO_REMOTE_MANIFEST);

        Collection<TablePlanStats> snapshots = tracker.snapshots();

        assertThat(snapshots).hasSize(1);
        TablePlanStats plan = snapshots.iterator().next();
        assertThat(plan.scope()).isEqualTo(orders);
        assertThat(plan.tasksPlanned()).isEqualTo(1L);
        assertThat(plan.metadataFailures()).isEqualTo(1L);
        assertThat(plan.skipped()).containsEntry(SkipReasonCode.NO_REMOTE_MANIFEST, 1L);
    }

    @Test
    void reportInputSurvivesFlinkGenericSerialization() {
        TablePlanStats plan =
                new TablePlanStats(
                        ScopeIdentity.table("db", "orders", 7L),
                        1L,
                        1L,
                        java.util.Collections.singletonMap(SkipReasonCode.RPC_ERROR, 1L));
        TypeSerializer<CleanupReportInput> serializer =
                TypeInformation.of(new TypeHint<CleanupReportInput>() {})
                        .createSerializer(new SerializerConfigImpl());

        CleanupReportInput copy = serializer.copy(CleanupReportInput.plan(plan));

        assertThat(copy.plan()).isNotNull();
        assertThat(copy.plan().skipped()).containsEntry(SkipReasonCode.RPC_ERROR, 1L);
    }
}
