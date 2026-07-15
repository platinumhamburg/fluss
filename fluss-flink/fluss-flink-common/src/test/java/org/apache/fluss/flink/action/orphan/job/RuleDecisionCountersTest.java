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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RuleDecisionCountersTest {

    @Test
    void partitionsEveryScannedFileIntoOneTerminalDecision() {
        RuleDecisionCounters counters =
                RuleDecisionCounters.scanned(10L)
                        .add(RuleDecisionCounters.scanned(20L))
                        .add(RuleDecisionCounters.scanned(30L))
                        .add(RuleDecisionCounters.scanned(40L))
                        .add(RuleDecisionCounters.scanned(50L))
                        .add(RuleDecisionCounters.keepActive(10L))
                        .add(RuleDecisionCounters.newerThanCutoff(20L))
                        .add(RuleDecisionCounters.unknownFileType(30L))
                        .add(RuleDecisionCounters.candidate(40L))
                        .add(RuleDecisionCounters.mtimeUnavailable(50L));

        assertThat(counters.scannedFiles()).isEqualTo(5L);
        assertThat(counters.keepActiveFiles()).isEqualTo(1L);
        assertThat(counters.newerThanCutoffFiles()).isEqualTo(1L);
        assertThat(counters.unknownFileTypeFiles()).isEqualTo(1L);
        assertThat(counters.candidateFiles()).isEqualTo(1L);
        assertThat(counters.mtimeUnavailableFiles()).isEqualTo(1L);
        assertThat(counters.isConsistent()).isTrue();
    }

    @Test
    void detectsMissingTerminalDecision() {
        assertThat(RuleDecisionCounters.scanned(10L).isConsistent()).isFalse();
    }
}
