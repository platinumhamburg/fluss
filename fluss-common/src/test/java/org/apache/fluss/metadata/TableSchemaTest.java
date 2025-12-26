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

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link org.apache.fluss.metadata.Schema}. */
class TableSchemaTest {

    @Test
    void testAutoIncrementColumnSchema() {
        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING())
                                        .column("f1", DataTypes.BIGINT())
                                        .column("f3", DataTypes.STRING())
                                        .primaryKey("f0")
                                        .primaryKey("f0")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Multiple primary keys are not supported.");

        assertThat(
                        Schema.newBuilder()
                                .column("f0", DataTypes.STRING())
                                .column("f1", DataTypes.BIGINT())
                                .column("f3", DataTypes.STRING())
                                .enableAutoIncrement("f1")
                                .primaryKey("f0")
                                .build()
                                .getAutoIncrementColumnNames())
                .isEqualTo(Collections.singletonList("f1"));
        assertThat(
                        Schema.newBuilder()
                                .column("f0", DataTypes.STRING())
                                .column("f1", DataTypes.BIGINT())
                                .column("f3", DataTypes.STRING())
                                .primaryKey("f0")
                                .build()
                                .getAutoIncrementColumnNames())
                .isEmpty();

        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING())
                                        .column("f1", DataTypes.BIGINT())
                                        .column("f3", DataTypes.STRING())
                                        .enableAutoIncrement("f0")
                                        .primaryKey("f0")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Auto increment column can not be used as the primary key.");

        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING())
                                        .column("f1", DataTypes.BIGINT())
                                        .column("f3", DataTypes.STRING())
                                        .enableAutoIncrement("f1")
                                        .enableAutoIncrement("f1")
                                        .primaryKey("f0")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Multiple auto increment columns are not supported yet.");
        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING())
                                        .column("f1", DataTypes.BIGINT())
                                        .column("f3", DataTypes.STRING())
                                        .enableAutoIncrement("f3")
                                        .primaryKey("f0")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("The data type of auto increment column must be INT or BIGINT.");
        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING())
                                        .column("f1", DataTypes.BIGINT())
                                        .column("f3", DataTypes.STRING())
                                        .enableAutoIncrement("f4")
                                        .primaryKey("f0")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Auto increment column f4 does not exist in table columns [f0, f1, f3].");
        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING())
                                        .column("f1", DataTypes.BIGINT())
                                        .column("f3", DataTypes.STRING())
                                        .enableAutoIncrement("f1")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Auto increment column can only be used in primary-key table.");
    }

    @Test
    void testSchemaBuilderColumnWithAggFunction() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("sum_val", DataTypes.BIGINT(), AggFunction.SUM)
                        .column("max_val", DataTypes.INT(), AggFunction.MAX)
                        .column("min_val", DataTypes.INT(), AggFunction.MIN)
                        .column("last_val", DataTypes.STRING(), AggFunction.LAST_VALUE)
                        .column(
                                "last_non_null",
                                DataTypes.STRING(),
                                AggFunction.LAST_VALUE_IGNORE_NULLS)
                        .column("first_val", DataTypes.STRING(), AggFunction.FIRST_VALUE)
                        .column(
                                "first_non_null",
                                DataTypes.STRING(),
                                AggFunction.FIRST_VALUE_IGNORE_NULLS)
                        .column("listagg_val", DataTypes.STRING(), AggFunction.LISTAGG)
                        .column("string_agg_val", DataTypes.STRING(), AggFunction.STRING_AGG)
                        .column("bool_and_val", DataTypes.BOOLEAN(), AggFunction.BOOL_AND)
                        .column("bool_or_val", DataTypes.BOOLEAN(), AggFunction.BOOL_OR)
                        .primaryKey("id")
                        .build();

        assertThat(schema.getAggFunction("sum_val").get()).isEqualTo(AggFunction.SUM);
        assertThat(schema.getAggFunction("max_val").get()).isEqualTo(AggFunction.MAX);
        assertThat(schema.getAggFunction("min_val").get()).isEqualTo(AggFunction.MIN);
        assertThat(schema.getAggFunction("last_val").get()).isEqualTo(AggFunction.LAST_VALUE);
        assertThat(schema.getAggFunction("last_non_null").get())
                .isEqualTo(AggFunction.LAST_VALUE_IGNORE_NULLS);
        assertThat(schema.getAggFunction("first_val").get()).isEqualTo(AggFunction.FIRST_VALUE);
        assertThat(schema.getAggFunction("first_non_null").get())
                .isEqualTo(AggFunction.FIRST_VALUE_IGNORE_NULLS);
        assertThat(schema.getAggFunction("listagg_val").get()).isEqualTo(AggFunction.LISTAGG);
        assertThat(schema.getAggFunction("string_agg_val").get()).isEqualTo(AggFunction.STRING_AGG);
        assertThat(schema.getAggFunction("bool_and_val").get()).isEqualTo(AggFunction.BOOL_AND);
        assertThat(schema.getAggFunction("bool_or_val").get()).isEqualTo(AggFunction.BOOL_OR);
    }

    @Test
    void testSchemaBuilderColumnWithAggFunctionThrowsExceptionForPrimaryKey() {
        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("value", DataTypes.BIGINT(), AggFunction.SUM)
                                        .primaryKey("id")
                                        .column("id", DataTypes.INT(), AggFunction.SUM)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot set aggregation function for primary key column");
    }

    @Test
    void testSchemaEqualityWithAggFunction() {
        Schema schema1 =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.BIGINT(), AggFunction.SUM)
                        .primaryKey("id")
                        .build();

        Schema schema2 =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.BIGINT(), AggFunction.SUM)
                        .primaryKey("id")
                        .build();

        Schema schema3 =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.BIGINT(), AggFunction.MAX)
                        .primaryKey("id")
                        .build();

        assertThat(schema1).isEqualTo(schema2);
        assertThat(schema1).isNotEqualTo(schema3);
    }

    @Test
    void testSchemaFromSchemaPreservesAggFunction() {
        Schema original =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.BIGINT(), AggFunction.SUM)
                        .column("max_val", DataTypes.INT(), AggFunction.MAX)
                        .primaryKey("id")
                        .build();

        Schema copied = Schema.newBuilder().fromSchema(original).build();

        assertThat(copied.getAggFunction("value")).isPresent();
        assertThat(copied.getAggFunction("value").get()).isEqualTo(AggFunction.SUM);
        assertThat(copied.getAggFunction("max_val")).isPresent();
        assertThat(copied.getAggFunction("max_val").get()).isEqualTo(AggFunction.MAX);
    }
}
