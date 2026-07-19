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

import org.apache.fluss.annotation.Internal;

import javax.annotation.Nullable;

import java.util.Objects;

/** Identity of one bounded normal-decision diagnostic group. */
@Internal
final class DiagnosticSampleKey {

    private final String scopeKey;
    private final String objectType;
    private final String reasonCode;
    private final @Nullable Integer subtaskIndex;
    private final @Nullable Integer attemptNumber;

    DiagnosticSampleKey(
            ScopeIdentity scope,
            String objectType,
            String reasonCode,
            @Nullable Integer subtaskIndex,
            @Nullable Integer attemptNumber) {
        this.scopeKey = scopeKey(scope);
        this.objectType = objectType;
        this.reasonCode = reasonCode;
        this.subtaskIndex = subtaskIndex;
        this.attemptNumber = attemptNumber;
    }

    private static String scopeKey(ScopeIdentity scope) {
        if (scope.tableId() != null) {
            return "table:" + scope.tableId();
        }
        return scope.kind()
                + ":"
                + scope.database()
                + ":"
                + scope.table()
                + ":"
                + String.valueOf(scope.partitionId());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof DiagnosticSampleKey)) {
            return false;
        }
        DiagnosticSampleKey that = (DiagnosticSampleKey) obj;
        return scopeKey.equals(that.scopeKey)
                && objectType.equals(that.objectType)
                && reasonCode.equals(that.reasonCode)
                && Objects.equals(subtaskIndex, that.subtaskIndex)
                && Objects.equals(attemptNumber, that.attemptNumber);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scopeKey, objectType, reasonCode, subtaskIndex, attemptNumber);
    }
}
