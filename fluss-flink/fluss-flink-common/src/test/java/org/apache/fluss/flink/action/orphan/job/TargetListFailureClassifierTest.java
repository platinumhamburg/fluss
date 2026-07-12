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

import org.apache.fluss.flink.action.orphan.RpcErrorClassifier;
import org.apache.fluss.flink.action.orphan.audit.SkipCategory;
import org.apache.fluss.flink.action.orphan.audit.SkipReasonCode;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TargetListFailureClassifierTest {

    @Test
    void missingPartitionIsExpectedStalePartitionSkip() {
        SkipReasonCode reason =
                TargetListFailureClassifier.reason(42L, RpcErrorClassifier.Category.NOT_FOUND);

        assertThat(reason).isEqualTo(SkipReasonCode.STALE_PARTITION);
        assertThat(reason.category()).isEqualTo(SkipCategory.EXPECTED_SKIP);
        assertThat(reason.retryable()).isFalse();
        assertThat(reason.actionRequired()).isFalse();
        assertThat(TargetListFailureClassifier.isMetadataFailure(reason)).isFalse();
    }

    @Test
    void transientTargetFailureRemainsDegradedMetadataFailure() {
        SkipReasonCode reason =
                TargetListFailureClassifier.reason(42L, RpcErrorClassifier.Category.TRANSIENT);

        assertThat(reason).isEqualTo(SkipReasonCode.METADATA_READ_FAILED);
        assertThat(TargetListFailureClassifier.isMetadataFailure(reason)).isTrue();
    }
}
