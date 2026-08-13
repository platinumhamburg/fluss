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
    void testIndexTableNamePrefixIsOperationallyDistinct() {
        assertThat(IndexTableUtils.INDEX_TABLE_NAME_PREFIX).isEqualTo("__fluss_index_");
    }

    @Test
    void testIndexTableNameComposes() {
        assertThat(IndexTableUtils.indexTableName(42L, "idx_user"))
                .isEqualTo("__fluss_index_42__idx_user");
    }

    @Test
    void testIndexTableNameDoesNotDependOnMainTableName() {
        assertThat(IndexTableUtils.indexTableName(42L, "idx_user"))
                .doesNotContain("orders")
                .startsWith("__fluss_index_");
    }
}
