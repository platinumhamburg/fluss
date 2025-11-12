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

package org.apache.fluss.utils.json;

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SchemaJsonSerde}. */
public class SchemaJsonSerdeTest extends JsonSerdeTestBase<Schema> {

    static final Schema SCHEMA_0 =
            Schema.newBuilder()
                    .column("a", DataTypes.INT())
                    .withComment("a is first column")
                    .column("b", DataTypes.STRING())
                    .withComment("b is second column")
                    .column("c", DataTypes.CHAR(10))
                    .withComment("c is third column")
                    .primaryKey("a", "c")
                    .build();

    static final Schema SCHEMA_1 =
            Schema.newBuilder()
                    .fromColumns(
                            Arrays.asList(
                                    new Schema.Column("a", DataTypes.INT()),
                                    new Schema.Column(
                                            "b", DataTypes.STRING(), "b is second column"),
                                    new Schema.Column(
                                            "c", DataTypes.TIMESTAMP(), "c is third column")))
                    .primaryKey(Collections.singletonList("a"))
                    .build();

    static final Schema SCHEMA_2 = Schema.newBuilder().fromSchema(SCHEMA_1).build();

    static final Schema SCHEMA_3 =
            Schema.newBuilder()
                    .column("a", DataTypes.BIGINT())
                    .withComment("a is first column")
                    .column("b", DataTypes.STRING())
                    .withComment("b is second column")
                    .column("c", DataTypes.TIMESTAMP(6))
                    .withComment("c is third column")
                    .build();

    static final Schema SCHEMA_4 =
            Schema.newBuilder()
                    .column("a", DataTypes.INT())
                    .withComment("a is first column")
                    .column("b", DataTypes.INT())
                    .withComment("b is second column")
                    .column("c", DataTypes.CHAR(10))
                    .withComment("c is third column")
                    .primaryKey("a", "c")
                    .enableAutoIncrement("b")
                    .build();
    static final Schema SCHEMA_WITH_SINGLE_INDEX =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .column("age", DataTypes.INT())
                    .primaryKey("id")
                    .index("name_idx", "name")
                    .build();

    static final Schema SCHEMA_WITH_MULTIPLE_INDEXES =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .column("age", DataTypes.INT())
                    .column("city", DataTypes.STRING())
                    .primaryKey("id")
                    .index("name_idx", "name")
                    .index("age_city_idx", "age", "city")
                    .build();

    static final String SCHEMA_JSON_0 =
            "{\"version\":2,\"columns\":[{\"name\":\"a\",\"data_type\":{\"type\":\"INTEGER\",\"nullable\":false},\"comment\":\"a is first column\",\"id\":0},{\"name\":\"b\",\"data_type\":{\"type\":\"STRING\"},\"comment\":\"b is second column\",\"id\":1},{\"name\":\"c\",\"data_type\":{\"type\":\"CHAR\",\"nullable\":false,\"length\":10},\"comment\":\"c is third column\",\"id\":2}],\"primary_key\":[\"a\",\"c\"],\"highest_field_id\":2}";
    static final String SCHEMA_JSON_1 =
            "{\"version\":2,\"columns\":[{\"name\":\"a\",\"data_type\":{\"type\":\"INTEGER\",\"nullable\":false},\"id\":0},{\"name\":\"b\",\"data_type\":{\"type\":\"STRING\"},\"comment\":\"b is second column\",\"id\":1},{\"name\":\"c\",\"data_type\":{\"type\":\"TIMESTAMP_WITHOUT_TIME_ZONE\",\"precision\":6},\"comment\":\"c is third column\",\"id\":2}],\"primary_key\":[\"a\"],\"highest_field_id\":2}";
    // shouldn't contain primary_key fields
    static final String SCHEMA_JSON_3 =
            "{\"version\":2,\"columns\":[{\"name\":\"a\",\"data_type\":{\"type\":\"BIGINT\"},\"comment\":\"a is first column\",\"id\":0},{\"name\":\"b\",\"data_type\":{\"type\":\"STRING\"},\"comment\":\"b is second column\",\"id\":1},{\"name\":\"c\",\"data_type\":{\"type\":\"TIMESTAMP_WITHOUT_TIME_ZONE\",\"precision\":6},\"comment\":\"c is third column\",\"id\":2}],\"highest_field_id\":2}";

    static final String SCHEMA_JSON_4 =
            "{\"version\":2,\"columns\":[{\"name\":\"a\",\"data_type\":{\"type\":\"INTEGER\",\"nullable\":false},\"comment\":\"a is first column\",\"id\":0},{\"name\":\"b\",\"data_type\":{\"type\":\"INTEGER\",\"nullable\":false},\"comment\":\"b is second column\",\"id\":1},{\"name\":\"c\",\"data_type\":{\"type\":\"CHAR\",\"nullable\":false,\"length\":10},\"comment\":\"c is third column\",\"id\":2}],\"primary_key\":[\"a\",\"c\"],\"auto_increment_column\":[\"b\"],\"highest_field_id\":2}";

    static final String SCHEMA_JSON_WITH_SINGLE_INDEX =
            "{\"version\":2,\"columns\":[{\"name\":\"id\",\"data_type\":{\"type\":\"INTEGER\",\"nullable\":false},\"id\":0},{\"name\":\"name\",\"data_type\":{\"type\":\"STRING\"},\"id\":1},{\"name\":\"age\",\"data_type\":{\"type\":\"INTEGER\"},\"id\":2}],\"primary_key\":[\"id\"],\"highest_field_id\":2,\"indexes\":[{\"name\":\"name_idx\",\"columns\":[\"name\"]}]}";

    static final String SCHEMA_JSON_WITH_MULTIPLE_INDEXES =
            "{\"version\":2,\"columns\":[{\"name\":\"id\",\"data_type\":{\"type\":\"INTEGER\",\"nullable\":false},\"id\":0},{\"name\":\"name\",\"data_type\":{\"type\":\"STRING\"},\"id\":1},{\"name\":\"age\",\"data_type\":{\"type\":\"INTEGER\"},\"id\":2},{\"name\":\"city\",\"data_type\":{\"type\":\"STRING\"},\"id\":3}],\"primary_key\":[\"id\"],\"highest_field_id\":3,\"indexes\":[{\"name\":\"name_idx\",\"columns\":[\"name\"]},{\"name\":\"age_city_idx\",\"columns\":[\"age\",\"city\"]}]}";

    SchemaJsonSerdeTest() {
        super(SchemaJsonSerde.INSTANCE);
    }

    @Override
    protected Schema[] createObjects() {
        return new Schema[] {
            SCHEMA_0,
            SCHEMA_1,
            SCHEMA_2,
            SCHEMA_3,
            SCHEMA_4,
            SCHEMA_WITH_SINGLE_INDEX,
            SCHEMA_WITH_MULTIPLE_INDEXES
        };
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            SCHEMA_JSON_0,
            SCHEMA_JSON_1,
            SCHEMA_JSON_1,
            SCHEMA_JSON_3,
            SCHEMA_JSON_4,
            SCHEMA_JSON_WITH_SINGLE_INDEX,
            SCHEMA_JSON_WITH_MULTIPLE_INDEXES
        };
    }

    @Test
    void testCompatibilityFromJsonLackOfColumnId() {
        String[] jsons = jsonLackOfColumnId();
        Schema[] expectedSchema = new Schema[] {SCHEMA_0, SCHEMA_1, SCHEMA_2, SCHEMA_3};
        for (int i = 0; i < jsons.length; i++) {
            assertThat(
                            JsonSerdeUtils.readValue(
                                    jsons[i].getBytes(StandardCharsets.UTF_8),
                                    SchemaJsonSerde.INSTANCE))
                    .isEqualTo(expectedSchema[i]);
        }
    }

    protected String[] jsonLackOfColumnId() {
        String oldSchemaJson1 =
                "{\"version\":1,\"columns\":[{\"name\":\"a\",\"data_type\":{\"type\":\"INTEGER\",\"nullable\":false},\"comment\":\"a is first column\"},{\"name\":\"b\",\"data_type\":{\"type\":\"STRING\"},\"comment\":\"b is second column\"},{\"name\":\"c\",\"data_type\":{\"type\":\"CHAR\",\"nullable\":false,\"length\":10},\"comment\":\"c is third column\"}],\"primary_key\":[\"a\",\"c\"]}";

        String oldSchemaJson2 =
                "{\"version\":1,\"columns\":[{\"name\":\"a\",\"data_type\":{\"type\":\"INTEGER\",\"nullable\":false}},{\"name\":\"b\",\"data_type\":{\"type\":\"STRING\"},\"comment\":\"b is second column\"},{\"name\":\"c\",\"data_type\":{\"type\":\"TIMESTAMP_WITHOUT_TIME_ZONE\",\"precision\":6},\"comment\":\"c is third column\"}],\"primary_key\":[\"a\"]}";

        // shouldn't contain primary_key fields
        String oldSchemaJson3 =
                "{\"version\":1,\"columns\":[{\"name\":\"a\",\"data_type\":{\"type\":\"BIGINT\"},\"comment\":\"a is first column\"},{\"name\":\"b\",\"data_type\":{\"type\":\"STRING\"},\"comment\":\"b is second column\"},{\"name\":\"c\",\"data_type\":{\"type\":\"TIMESTAMP_WITHOUT_TIME_ZONE\",\"precision\":6},\"comment\":\"c is third column\"}]}";

        return new String[] {oldSchemaJson1, oldSchemaJson2, oldSchemaJson2, oldSchemaJson3};
    }
}
