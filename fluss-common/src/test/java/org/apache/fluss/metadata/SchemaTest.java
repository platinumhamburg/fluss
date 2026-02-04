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

import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link Schema}. */
class SchemaTest {

    @Test
    void testSchemaWithoutIndex() {
        final Schema schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .column("f1", DataTypes.BIGINT())
                        .column("f2", DataTypes.STRING())
                        .primaryKey("f0", "f2")
                        .build();

        assertThat(schema.getColumns()).hasSize(3);
        assertThat(schema.getPrimaryKey()).isPresent();
        assertThat(schema.getIndexes()).isEmpty();
    }

    @Test
    void testSchemaWithSingleIndex() {
        final Schema schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .column("f1", DataTypes.BIGINT())
                        .column("f2", DataTypes.STRING())
                        .primaryKey("f0", "f2")
                        .index("idx1", "f1")
                        .build();

        assertThat(schema.getColumns()).hasSize(3);
        assertThat(schema.getPrimaryKey()).isPresent();
        assertThat(schema.getIndexes()).hasSize(1);

        Schema.Index index = schema.getIndexes().get(0);
        assertThat(index.getIndexName()).isEqualTo("idx1");
        assertThat(index.getColumnNames()).containsExactly("f1");
    }

    @Test
    void testSchemaWithMultipleIndexes() {
        final Schema schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .column("f1", DataTypes.BIGINT())
                        .column("f2", DataTypes.STRING())
                        .column("f3", DataTypes.INT())
                        .primaryKey("f0", "f2")
                        .index("idx1", "f1")
                        .index("idx2", Arrays.asList("f1", "f3"))
                        .build();

        assertThat(schema.getColumns()).hasSize(4);
        assertThat(schema.getPrimaryKey()).isPresent();
        assertThat(schema.getIndexes()).hasSize(2);

        Schema.Index index1 = schema.getIndexes().get(0);
        assertThat(index1.getIndexName()).isEqualTo("idx1");
        assertThat(index1.getColumnNames()).containsExactly("f1");

        Schema.Index index2 = schema.getIndexes().get(1);
        assertThat(index2.getIndexName()).isEqualTo("idx2");
        assertThat(index2.getColumnNames()).containsExactly("f1", "f3");
    }

    @Test
    void testIndexWithDuplicateColumns() {
        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING())
                                        .column("f1", DataTypes.BIGINT())
                                        .primaryKey("f0")
                                        .index("idx1", Arrays.asList("f1", "f1"))
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Index idx1 must not contain duplicate columns");
    }

    @Test
    void testDuplicateIndexNames() {
        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING())
                                        .column("f1", DataTypes.BIGINT())
                                        .column("f2", DataTypes.STRING())
                                        .primaryKey("f0")
                                        .index("idx1", "f1")
                                        .index("idx1", "f2")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Duplicate index names found");
    }

    @Test
    void testIndexWithNonExistentColumn() {
        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING())
                                        .column("f1", DataTypes.BIGINT())
                                        .primaryKey("f0")
                                        .index("idx1", "nonExistentColumn")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Index idx1 references non-existent columns");
    }

    @Test
    void testIndexNameValidation() {
        // Valid index names
        Schema.newBuilder()
                .column("f0", DataTypes.STRING())
                .column("f1", DataTypes.BIGINT())
                .primaryKey("f0")
                .index("valid_index_123", "f1")
                .build();

        // Invalid index names
        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING())
                                        .column("f1", DataTypes.BIGINT())
                                        .primaryKey("f0")
                                        .index("invalid-index", "f1")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("can only contain letters, digits, and underscores");

        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING())
                                        .column("f1", DataTypes.BIGINT())
                                        .primaryKey("f0")
                                        .index("invalid index", "f1")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("can only contain letters, digits, and underscores");

        // Index names with double underscores should be invalid
        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING())
                                        .column("f1", DataTypes.BIGINT())
                                        .primaryKey("f0")
                                        .index("invalid__index", "f1")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot contain double underscores '__'");

        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING())
                                        .column("f1", DataTypes.BIGINT())
                                        .primaryKey("f0")
                                        .index("__invalid_index", "f1")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot contain double underscores '__'");
    }

    @Test
    void testSchemaEqualsAndHashCode() {
        final Schema schema1 =
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .column("f1", DataTypes.BIGINT())
                        .primaryKey("f0")
                        .index("idx1", "f1")
                        .build();

        final Schema schema2 =
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .column("f1", DataTypes.BIGINT())
                        .primaryKey("f0")
                        .index("idx1", "f1")
                        .build();

        final Schema schema3 =
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .column("f1", DataTypes.BIGINT())
                        .primaryKey("f0")
                        .index("idx2", "f1")
                        .build();

        assertThat(schema1).isEqualTo(schema2);
        assertThat(schema1.hashCode()).isEqualTo(schema2.hashCode());
        assertThat(schema1).isNotEqualTo(schema3);
    }

    @Test
    void testFromSchema() {
        final Schema originalSchema =
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .column("f1", DataTypes.BIGINT())
                        .primaryKey("f0")
                        .index("idx1", "f1")
                        .build();

        final Schema copiedSchema = Schema.newBuilder().fromSchema(originalSchema).build();

        assertThat(copiedSchema).isEqualTo(originalSchema);
        assertThat(copiedSchema.getIndexes()).hasSize(1);
        assertThat(copiedSchema.getIndexes().get(0).getIndexName()).isEqualTo("idx1");
    }

    @Test
    void testEmptyIndexName() {
        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING())
                                        .column("f1", DataTypes.BIGINT())
                                        .primaryKey("f0")
                                        .index("", "f1")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Index name must not be empty");
    }

    @Test
    void testEmptyIndexColumns() {
        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING())
                                        .column("f1", DataTypes.BIGINT())
                                        .primaryKey("f0")
                                        .index("idx1", new String[0])
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Index constraint must be defined for at least a single column");
    }

    /**
     * Test that a single nullable index column is automatically converted to NOT NULL.
     *
     * <p>Validates: Requirements 1.1, 1.2
     */
    @Test
    void testIndexColumnForcedNotNull() {
        // Create schema with a nullable index column
        final Schema schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .column("f1", DataTypes.BIGINT()) // nullable by default
                        .column("f2", DataTypes.STRING())
                        .primaryKey("f0")
                        .index("idx1", "f1")
                        .build();

        // Verify the index column f1 is now NOT NULL
        Schema.Column indexColumn =
                schema.getColumns().stream()
                        .filter(col -> col.getName().equals("f1"))
                        .findFirst()
                        .orElseThrow();

        assertThat(indexColumn.getDataType().isNullable()).isFalse();

        // Verify non-index column f2 remains nullable
        Schema.Column nonIndexColumn =
                schema.getColumns().stream()
                        .filter(col -> col.getName().equals("f2"))
                        .findFirst()
                        .orElseThrow();

        assertThat(nonIndexColumn.getDataType().isNullable()).isTrue();
    }

    /**
     * Test that all columns in a multi-column index are automatically converted to NOT NULL.
     *
     * <p>Validates: Requirements 1.1, 1.2
     */
    @Test
    void testMultiColumnIndexForcedNotNull() {
        // Create schema with a multi-column index where all columns are nullable
        final Schema schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .column("f1", DataTypes.BIGINT()) // nullable by default
                        .column("f2", DataTypes.STRING()) // nullable by default
                        .column("f3", DataTypes.INT()) // nullable by default
                        .primaryKey("f0")
                        .index("idx1", Arrays.asList("f1", "f2"))
                        .build();

        // Verify index column f1 is now NOT NULL
        Schema.Column indexColumn1 =
                schema.getColumns().stream()
                        .filter(col -> col.getName().equals("f1"))
                        .findFirst()
                        .orElseThrow();

        assertThat(indexColumn1.getDataType().isNullable()).isFalse();

        // Verify index column f2 is now NOT NULL
        Schema.Column indexColumn2 =
                schema.getColumns().stream()
                        .filter(col -> col.getName().equals("f2"))
                        .findFirst()
                        .orElseThrow();

        assertThat(indexColumn2.getDataType().isNullable()).isFalse();

        // Verify non-index column f3 remains nullable
        Schema.Column nonIndexColumn =
                schema.getColumns().stream()
                        .filter(col -> col.getName().equals("f3"))
                        .findFirst()
                        .orElseThrow();

        assertThat(nonIndexColumn.getDataType().isNullable()).isTrue();
    }

    /**
     * Test that index columns that are already NOT NULL remain unchanged.
     *
     * <p>Validates: Requirement 1.3
     */
    @Test
    void testIndexColumnAlreadyNotNull() {
        // Create schema with an index column that is already NOT NULL
        final Schema schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .column("f1", DataTypes.BIGINT().copy(false)) // explicitly NOT NULL
                        .column("f2", DataTypes.STRING())
                        .primaryKey("f0")
                        .index("idx1", "f1")
                        .build();

        // Verify the index column f1 is still NOT NULL
        Schema.Column indexColumn =
                schema.getColumns().stream()
                        .filter(col -> col.getName().equals("f1"))
                        .findFirst()
                        .orElseThrow();

        assertThat(indexColumn.getDataType().isNullable()).isFalse();

        // Verify the data type is preserved correctly (BIGINT NOT NULL)
        assertThat(indexColumn.getDataType().toString()).isEqualTo("BIGINT NOT NULL");
    }
}
