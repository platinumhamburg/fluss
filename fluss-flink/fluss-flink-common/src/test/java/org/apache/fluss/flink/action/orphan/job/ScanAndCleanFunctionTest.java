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

import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ScanAndCleanFunctionTest {

    @Test
    void forwardsScopeSummaryWithoutOpeningFilesystemState() throws Exception {
        ScopePlanStats plan = new ScopePlanStats();
        ScopeSummaryTask marker = ScopeSummaryTask.from(plan);
        CleanupStats expected = marker.stats();
        ScanAndCleanFunction function = new ScanAndCleanFunction(100L, Collections.emptyMap());
        List<CleanupStats> output = new ArrayList<>();

        function.processElement(marker, null, new ListCollector(output));

        assertThat(output).containsExactly(expected);
    }

    private static final class ListCollector implements Collector<CleanupStats> {
        private final List<CleanupStats> output;

        private ListCollector(List<CleanupStats> output) {
            this.output = output;
        }

        @Override
        public void collect(CleanupStats record) {
            output.add(record);
        }

        @Override
        public void close() {}
    }
}
