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

import org.apache.fluss.exception.InvalidDatabaseException;
import org.apache.fluss.exception.InvalidTableException;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link org.apache.fluss.metadata.TablePath}. */
class TablePathTest {

    // -----------------------------------------------------------------------------------------
    // Index Table Tests
    // -----------------------------------------------------------------------------------------

    @Test
    void testForIndexTable() {
        TablePath mainTablePath = TablePath.of("test_db", "test_table");
        String indexName = "test_index";

        TablePath indexTablePath = TablePath.forIndexTable(mainTablePath, indexName);

        assertThat(indexTablePath.getDatabaseName()).isEqualTo("test_db");
        assertThat(indexTablePath.getTableName()).isEqualTo("__test_table__index__test_index");
    }

    @Test
    void testForIndexTableWithComplexNames() {
        TablePath mainTablePath = TablePath.of("my_database", "user_orders");
        String indexName = "user_email_idx";

        TablePath indexTablePath = TablePath.forIndexTable(mainTablePath, indexName);

        assertThat(indexTablePath.getDatabaseName()).isEqualTo("my_database");
        assertThat(indexTablePath.getTableName()).isEqualTo("__user_orders__index__user_email_idx");
    }

    @Test
    void testIsIndexTableName() {
        // Valid index table names
        assertThat(TablePath.of("db", "__orders__index__user_id_idx").isIndexTableName()).isTrue();
        assertThat(TablePath.of("db", "__test_table__index__idx").isIndexTableName()).isTrue();
        assertThat(TablePath.of("db", "__a__index__b").isIndexTableName()).isTrue();

        // Invalid index table names
        assertThat(TablePath.of("db", "orders").isIndexTableName()).isFalse();
        assertThat(TablePath.of("db", "__orders").isIndexTableName()).isFalse();
        assertThat(TablePath.of("db", "orders__index__idx").isIndexTableName()).isFalse();
        assertThat(TablePath.of("db", "__orders__idx").isIndexTableName()).isFalse();
    }

    @Test
    void testForIndexTableAndIsIndexTableNameConsistency() {
        // Verify that forIndexTable creates paths that isIndexTableName recognizes
        TablePath mainTablePath = TablePath.of("db", "main_table");
        TablePath indexTablePath = TablePath.forIndexTable(mainTablePath, "my_index");

        assertThat(indexTablePath.isIndexTableName()).isTrue();
        assertThat(mainTablePath.isIndexTableName()).isFalse();
    }

    // -----------------------------------------------------------------------------------------
    // Validation Tests
    // -----------------------------------------------------------------------------------------

    @Test
    void testValidate() {
        // assert valid name
        TablePath path = TablePath.of("db_2-abc3", "table-1_abc_2");
        path.validate();
        assertThat(path.isValid()).isTrue();
        assertThat(path.toString()).isEqualTo("db_2-abc3.table-1_abc_2");

        // assert invalid name prefix
        TablePath invalidPath = TablePath.of("db_2", "__table-1");
        assertThatThrownBy(invalidPath::validate)
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining(
                        "Table name __table-1 is invalid: '__' is not allowed as prefix");

        // check max length
        String longName = StringUtils.repeat("a", 200);
        assertThat(TablePath.of(longName, longName).isValid()).isTrue();

        // assert invalid names
        assertInvalidName("*abc", "'*abc' contains one or more characters other than");
        assertInvalidName("table.abc", "'table.abc' contains one or more characters other than");
        assertInvalidName(null, "null string is not allowed");
        assertInvalidName("", "the empty string is not allowed");
        assertInvalidName(" ", "' ' contains one or more characters other than");
        assertInvalidName(".", "'.' is not allowed");
        assertInvalidName("..", "'..' is not allowed");
        assertInvalidName("..", "'..' is not allowed");
        String invalidLongName = StringUtils.repeat("a", 201);
        assertInvalidName(
                invalidLongName,
                "the length of '"
                        + invalidLongName
                        + "' is longer than the max allowed length 200");
    }

    private static void assertInvalidName(String name, String expectedMessage) {
        TablePath invalidTable = TablePath.of("db", name);
        assertThat(invalidTable.isValid()).isFalse();
        assertThatThrownBy(invalidTable::validate)
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("Table name " + name + " is invalid: " + expectedMessage);

        TablePath invalidDb = TablePath.of(name, "table");
        assertThat(invalidDb.isValid()).isFalse();
        assertThatThrownBy(invalidDb::validate)
                .isInstanceOf(InvalidDatabaseException.class)
                .hasMessageContaining("Database name " + name + " is invalid: " + expectedMessage);
    }
}
