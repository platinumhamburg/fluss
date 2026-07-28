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

package org.apache.fluss.utils;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class IndexTableUtilsTest {

    @Test
    void testPartitionIdSystemColumnNameIsUnderscored() {
        assertThat(IndexTableUtils.PARTITION_ID_SYSTEM_COLUMN).isEqualTo("__partition_id");
    }

    @Test
    void testReservedIndexSystemColumnSetContainsIndexTableSystemColumns() {
        assertThat(IndexTableUtils.RESERVED_INDEX_SYSTEM_COLUMNS).containsExactly("__partition_id");
    }

    @Test
    void testIndexTableNameSeparatorIsDoubleUnderscore() {
        assertThat(IndexTableUtils.INDEX_TABLE_NAME_SEPARATOR).isEqualTo("__");
    }

    @Test
    void testIndexTableNameComposes() {
        assertThat(IndexTableUtils.indexTableName("orders", "idx_user"))
                .isEqualTo("orders__idx_user");
    }

    @Test
    void testMainTableNameFromIndexTableNameUsesLastSeparator() {
        assertThat(IndexTableUtils.mainTableNameFromIndexTableName("orders__idx_user"))
                .isEqualTo(Optional.of("orders"));
        assertThat(IndexTableUtils.mainTableNameFromIndexTableName("tenant__orders__idx_user"))
                .isEqualTo(Optional.of("tenant__orders"));
        assertThat(IndexTableUtils.mainTableNameFromIndexTableName("orders")).isEmpty();
        assertThat(IndexTableUtils.mainTableNameFromIndexTableName("__idx_user")).isEmpty();
        assertThat(IndexTableUtils.mainTableNameFromIndexTableName("orders__")).isEmpty();
    }
}
