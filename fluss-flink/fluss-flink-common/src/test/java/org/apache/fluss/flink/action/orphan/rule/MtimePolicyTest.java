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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MtimePolicyTest {

    private static final long CUTOFF = 1_000L;

    @Test
    void distinguishesUnavailableMtimeFromFreshMtime() {
        assertThat(MtimePolicy.isUnavailable(Long.MAX_VALUE)).isTrue();
        assertThat(MtimePolicy.isUnavailable(CUTOFF)).isFalse();
        assertThat(MtimePolicy.evaluateInactiveFile(Long.MAX_VALUE, CUTOFF))
                .isEqualTo(Decision.MTIME_UNAVAILABLE);
        assertThat(MtimePolicy.failClosed(Decision.DEFER, Long.MAX_VALUE))
                .isEqualTo(Decision.MTIME_UNAVAILABLE);
        assertThat(MtimePolicy.failClosed(Decision.DELETE, Long.MAX_VALUE))
                .isEqualTo(Decision.MTIME_UNAVAILABLE);
        assertThat(MtimePolicy.failClosed(Decision.KEEP_ACTIVE, Long.MAX_VALUE))
                .isEqualTo(Decision.KEEP_ACTIVE);
    }

    @Test
    void preservesStrictCutoffBoundary() {
        assertThat(MtimePolicy.evaluateInactiveFile(CUTOFF - 1L, CUTOFF))
                .isEqualTo(Decision.DELETE);
        assertThat(MtimePolicy.evaluateInactiveFile(CUTOFF, CUTOFF)).isEqualTo(Decision.DEFER);
        assertThat(MtimePolicy.evaluateInactiveFile(CUTOFF + 1L, CUTOFF)).isEqualTo(Decision.DEFER);
        assertThat(MtimePolicy.isOlderThanCutoff(Long.MAX_VALUE, CUTOFF)).isFalse();
    }
}
