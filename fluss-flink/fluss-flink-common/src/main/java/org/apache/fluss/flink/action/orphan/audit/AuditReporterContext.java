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

package org.apache.fluss.flink.action.orphan.audit;

import org.apache.fluss.annotation.PublicUnstable;

import javax.annotation.Nullable;

import java.util.UUID;

/** Runtime identity supplied when an orphan cleanup audit reporter is opened. */
@PublicUnstable
public final class AuditReporterContext {

    private final String runId;
    private final boolean dryRun;
    private final AuditStage stage;
    private final @Nullable String operatorName;
    private final @Nullable Integer subtaskIndex;
    private final @Nullable Integer attemptNumber;
    private final ClassLoader userCodeClassLoader;

    public AuditReporterContext(
            String runId,
            boolean dryRun,
            AuditStage stage,
            @Nullable String operatorName,
            @Nullable Integer subtaskIndex,
            @Nullable Integer attemptNumber,
            ClassLoader userCodeClassLoader) {
        this.runId = validateRunId(runId);
        if (stage == null) {
            throw new IllegalArgumentException("stage");
        }
        if (userCodeClassLoader == null) {
            throw new IllegalArgumentException("userCodeClassLoader");
        }
        this.dryRun = dryRun;
        this.stage = stage;
        this.operatorName = operatorName;
        this.subtaskIndex = subtaskIndex;
        this.attemptNumber = attemptNumber;
        this.userCodeClassLoader = userCodeClassLoader;
    }

    public String getRunId() {
        return runId;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public AuditStage getStage() {
        return stage;
    }

    @Nullable
    public String getOperatorName() {
        return operatorName;
    }

    @Nullable
    public Integer getSubtaskIndex() {
        return subtaskIndex;
    }

    @Nullable
    public Integer getAttemptNumber() {
        return attemptNumber;
    }

    public ClassLoader getUserCodeClassLoader() {
        return userCodeClassLoader;
    }

    private static String validateRunId(String runId) {
        if (runId == null || runId.isEmpty()) {
            throw new IllegalArgumentException("runId");
        }
        try {
            UUID.fromString(runId);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("runId");
        }
        return runId;
    }
}
