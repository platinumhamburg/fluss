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

package org.apache.fluss.client.table;

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link FlussTable#getSecondaryIndexLookuper(String)} validation behaviour.
 *
 * <p>Note: the full factory wiring (Index Table lookup + main table point-get + recheck) requires
 * a live FlussConnection (admin client + RPC) and is exercised end-to-end by P4T7's
 * {@code testLookupReturnsNoStalePointers} ITCase. Constructing a FlussTable in unit tests is
 * impractical because {@code FlussConnection} is {@code final} and the project does not enable
 * {@code mock-maker-inline}. Therefore this unit test scope is limited to the lookup-by-name
 * validation step extracted as {@link FlussTable#findIndexOrThrow(Schema, TablePath, String)}.
 */
class FlussTableSecondaryIndexLookuperTest {

    @Test
    void testFindIndexOrThrowRejectsUnknownIndexName() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("u", DataTypes.BIGINT())
                        .primaryKey("id")
                        .index("idx_user", "u")
                        .build();

        assertThatThrownBy(
                        () ->
                                FlussTable.findIndexOrThrow(
                                        schema, TablePath.of("db", "t"), "nonexistent"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nonexistent");
    }

    @Test
    void testFindIndexOrThrowReturnsRequestedIndex() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("u", DataTypes.BIGINT())
                        .column("v", DataTypes.STRING())
                        .primaryKey("id")
                        .index("idx_user", "u")
                        .index("idx_value", "v")
                        .build();

        Schema.Index found =
                FlussTable.findIndexOrThrow(schema, TablePath.of("db", "t"), "idx_value");

        assertThat(found.getIndexName()).isEqualTo("idx_value");
        assertThat(found.getColumnNames()).containsExactly("v");
    }
}
