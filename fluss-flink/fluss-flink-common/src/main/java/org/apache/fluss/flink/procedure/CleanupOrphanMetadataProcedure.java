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

package org.apache.fluss.flink.procedure;

import org.apache.fluss.client.admin.CleanupOrphanMetadataResult;

import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

/** Procedure to cleanup orphan metadata left behind by incomplete table deletions. */
public class CleanupOrphanMetadataProcedure extends ProcedureBase {

    @ProcedureHint(argument = {})
    public String[] call(ProcedureContext context) throws Exception {
        CleanupOrphanMetadataResult result = admin.cleanupOrphanMetadata().get();
        return new String[] {
            String.format(
                    "Cleaned %d orphan table(s) and %d orphan partition(s). "
                            + "Table IDs: %s, Partition IDs: %s",
                    result.getOrphanTableCount(),
                    result.getOrphanPartitionCount(),
                    result.getCleanedTableIds(),
                    result.getCleanedPartitionIds())
        };
    }
}
