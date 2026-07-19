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

package org.apache.fluss.flink.action.orphan.rule;

import org.apache.fluss.annotation.Internal;

import java.util.Objects;
import java.util.Optional;

/** Immutable diagnostic explanation of a file-rule decision. */
@Internal
public final class RuleEvaluation {

    private final Decision decision;
    private final String reasonCode;
    private final String referenceType;
    private final String referenceMatchKind;
    private final String referenceKey;

    private RuleEvaluation(
            Decision decision,
            String reasonCode,
            String referenceType,
            String referenceMatchKind,
            String referenceKey) {
        this.decision = Objects.requireNonNull(decision, "decision");
        this.reasonCode = Objects.requireNonNull(reasonCode, "reasonCode");
        this.referenceType = referenceType;
        this.referenceMatchKind = referenceMatchKind;
        this.referenceKey = referenceKey;
    }

    public static RuleEvaluation decision(Decision decision, String reasonCode) {
        return new RuleEvaluation(decision, reasonCode, null, null, null);
    }

    public static RuleEvaluation active(
            String reasonCode, String referenceType, String matchKind, String referenceKey) {
        return new RuleEvaluation(
                Decision.KEEP_ACTIVE,
                reasonCode,
                Objects.requireNonNull(referenceType, "referenceType"),
                Objects.requireNonNull(matchKind, "matchKind"),
                Objects.requireNonNull(referenceKey, "referenceKey"));
    }

    public Decision decision() {
        return decision;
    }

    public String reasonCode() {
        return reasonCode;
    }

    public Optional<String> referenceType() {
        return Optional.ofNullable(referenceType);
    }

    public Optional<String> referenceMatchKind() {
        return Optional.ofNullable(referenceMatchKind);
    }

    public Optional<String> referenceKey() {
        return Optional.ofNullable(referenceKey);
    }
}
