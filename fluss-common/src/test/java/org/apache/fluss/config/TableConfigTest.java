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

package org.apache.fluss.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TableConfig}. */
class TableConfigTest {

    @Test
    void testGetFieldListaggDelimiterWithDefaultValue() {
        Configuration conf = new Configuration();
        TableConfig tableConfig = new TableConfig(conf);

        // Test default delimiter (comma)
        assertThat(tableConfig.getFieldListaggDelimiter("tags")).isEqualTo(",");
        assertThat(tableConfig.getFieldListaggDelimiter("values")).isEqualTo(",");
    }

    @Test
    void testGetFieldListaggDelimiterWithSpecialCharacters() {
        Configuration conf = new Configuration();
        conf.setString("table.merge-engine.aggregate.field1.listagg-delimiter", "||");
        conf.setString("table.merge-engine.aggregate.field2.listagg-delimiter", " - ");
        conf.setString("table.merge-engine.aggregate.field3.listagg-delimiter", "\t");

        TableConfig tableConfig = new TableConfig(conf);

        // Test special character delimiters
        assertThat(tableConfig.getFieldListaggDelimiter("field1")).isEqualTo("||");
        assertThat(tableConfig.getFieldListaggDelimiter("field2")).isEqualTo(" - ");
        assertThat(tableConfig.getFieldListaggDelimiter("field3")).isEqualTo("\t");
    }

    @Test
    void testAggregationRemoveRecordOnDelete() {
        Configuration conf = new Configuration();
        TableConfig tableConfig = new TableConfig(conf);

        // Test default value (false)
        assertThat(tableConfig.getAggregationRemoveRecordOnDelete()).isFalse();

        // Test configured value
        conf.set(ConfigOptions.TABLE_AGG_REMOVE_RECORD_ON_DELETE, true);
        TableConfig tableConfig2 = new TableConfig(conf);
        assertThat(tableConfig2.getAggregationRemoveRecordOnDelete()).isTrue();
    }
}
