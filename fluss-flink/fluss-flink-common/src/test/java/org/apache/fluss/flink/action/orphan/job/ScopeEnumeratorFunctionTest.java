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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.action.orphan.audit.AuditLogger;
import org.apache.fluss.flink.action.orphan.audit.ScopeIdentity;
import org.apache.fluss.flink.action.orphan.config.OrphanCleanConfig;
import org.apache.fluss.flink.adapter.MultipleParameterToolAdapter;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ScopeEnumeratorFunctionTest {

    @Test
    void mapsActionFileSystemConfigsIntoClientNamespace() {
        OrphanCleanConfig config =
                configWith(
                        "fs.oss.endpoint=test-endpoint",
                        "fs.oss.region=test-region",
                        "client.security.protocol=SASL");

        Configuration clientConfig = ScopeEnumeratorFunction.createFlussClientConfiguration(config);

        assertThat(clientConfig.toMap())
                .containsEntry("client.fs.oss.endpoint", "test-endpoint")
                .containsEntry("client.fs.oss.region", "test-region")
                .containsEntry("client.security.protocol", "SASL")
                .doesNotContainKeys("fs.oss.endpoint", "fs.oss.region");
    }

    @Test
    void rejectsConflictingActionAndClientFileSystemConfigs() {
        OrphanCleanConfig config =
                configWith(
                        "fs.oss.endpoint=action-endpoint",
                        "client.fs.oss.endpoint=client-endpoint");

        assertThatThrownBy(() -> ScopeEnumeratorFunction.createFlussClientConfiguration(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fs.oss.endpoint")
                .hasMessageContaining("client.fs.oss.endpoint");
    }

    @Test
    void serialPlanningMatchesPr4TaskOrderAndFinalPlanStats() throws Exception {
        Map<String, List<String>> tablesByDatabase = new LinkedHashMap<String, List<String>>();
        tablesByDatabase.put("db1", Arrays.asList("db1.table1", "db1.table2"));
        tablesByDatabase.put("db2", Arrays.asList("db2.table3"));

        PlanningResult reference = planSerialReference(tablesByDatabase);
        PlanningResult actual = new PlanningResult();
        ScopeEnumeratorFunction.planTargetsAndOrphans(
                1,
                new ArrayList<String>(tablesByDatabase.keySet()),
                tablesByDatabase::get,
                tables -> {
                    for (String table : tables) {
                        replayTarget(table, actual);
                    }
                },
                table -> emitOrphan("orphan-partition:" + table, actual),
                database -> emitOrphan("orphan-table:" + database, actual));

        assertThat(actual.taskSequence).containsExactlyElementsOf(reference.taskSequence);
        assertThat(actual.planStats).usingRecursiveComparison().isEqualTo(reference.planStats);
    }

    private static PlanningResult planSerialReference(Map<String, List<String>> tablesByDatabase) {
        PlanningResult reference = new PlanningResult();
        for (Map.Entry<String, List<String>> database : tablesByDatabase.entrySet()) {
            for (String table : database.getValue()) {
                replayTarget(table, reference);
                emitOrphan("orphan-partition:" + table, reference);
            }
            emitOrphan("orphan-table:" + database.getKey(), reference);
        }
        return reference;
    }

    private static void replayTarget(String table, PlanningResult result) {
        int buckets = table.endsWith("table1") ? 2 : 1;
        ScopeTargetStats targetStats =
                new ScopeTargetStats(ScopeIdentity.unresolvedTable("test", table), buckets, false);
        ScopeTargetEnumeration.Result.Builder builder =
                ScopeTargetEnumeration.Result.builder(targetStats).discoveredBuckets(buckets);
        for (int bucket = 0; bucket < buckets; bucket++) {
            targetStats.logResolvedBucket();
            builder.task(task("target:" + table + ":" + bucket));
        }
        targetStats.complete(0L);
        builder.build()
                .replay(
                        new AuditLogger(),
                        result.planStats,
                        task -> result.taskSequence.add(((OrphanDirCleanTask) task).dirPath()));
    }

    private static void emitOrphan(String label, PlanningResult result) {
        result.taskSequence.add(label);
        result.planStats.orphanDirTask();
    }

    private static OrphanDirCleanTask task(String label) {
        return new OrphanDirCleanTask(ScopeIdentity.global(), label, 0L, true, false);
    }

    private static OrphanCleanConfig configWith(String... configs) {
        String[] args = new String[4 + configs.length * 2];
        args[0] = "--bootstrap-server";
        args[1] = "h:9123";
        args[2] = "--all-databases";
        args[3] = "--dry-run";
        for (int i = 0; i < configs.length; i++) {
            args[4 + i * 2] = "--conf";
            args[5 + i * 2] = configs[i];
        }
        return OrphanCleanConfig.fromParams(MultipleParameterToolAdapter.fromArgs(args));
    }

    private static final class PlanningResult {
        private final List<String> taskSequence = new ArrayList<String>();
        private final ScopePlanStats planStats = new ScopePlanStats();
    }
}
