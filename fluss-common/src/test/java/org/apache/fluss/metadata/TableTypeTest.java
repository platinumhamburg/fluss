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

package org.apache.fluss.metadata;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TableTypeTest {

    @Test
    void should_define_data_table_and_index_table_variants() {
        assertThat(TableType.valueOf("DATA_TABLE")).isEqualTo(TableType.DATA_TABLE);
        assertThat(TableType.valueOf("INDEX_TABLE")).isEqualTo(TableType.INDEX_TABLE);
        assertThat(TableType.values()).hasSize(2);
    }
}
