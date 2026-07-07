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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TableInfo#isIndexTable()} and main-table back-link accessors. */
class TableInfoIndexTableTest {

    @Test
    void testIndexTableInfoReportsBackLinkToMainTable() {
        Schema indexSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("u", DataTypes.BIGINT())
                        .primaryKey("u", "id")
                        .build();
        TableDescriptor idxD =
                TableDescriptor.builder()
                        .schema(indexSchema)
                        .distributedBy(4, "u")
                        .property(ConfigOptions.TABLE_TYPE, TableType.INDEX_TABLE)
                        .property(ConfigOptions.TABLE_INDEX_META_MAIN_TABLE_ID, 7L)
                        .build();

        long now = System.currentTimeMillis();
        TableInfo info =
                TableInfo.of(
                        TablePath.of("db", "t$idx_u"),
                        /* tableId= */ 8L,
                        /* schemaId= */ 1,
                        idxD,
                        /* remoteDataDir= */ null,
                        now,
                        now);

        assertThat(info.isIndexTable()).isTrue();
        assertThat(info.getMainTableId()).isEqualTo(OptionalLong.of(7L));
    }

    @Test
    void testDataTableInfoIsNotIndexTableAndHasNoBackLink() {
        Schema schema =
                Schema.newBuilder().column("id", DataTypes.BIGINT()).primaryKey("id").build();
        TableDescriptor d = TableDescriptor.builder().schema(schema).distributedBy(3, "id").build();
        long now = System.currentTimeMillis();
        TableInfo info = TableInfo.of(TablePath.of("db", "t"), 1L, 1, d, null, now, now);

        assertThat(info.isIndexTable()).isFalse();
        assertThat(info.getMainTableId()).isEmpty();
    }
}
