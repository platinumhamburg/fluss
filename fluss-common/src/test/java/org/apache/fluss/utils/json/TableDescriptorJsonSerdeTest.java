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

import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableType;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TableDescriptorJsonSerde}. */
public class TableDescriptorJsonSerdeTest extends JsonSerdeTestBase<TableDescriptor> {
    TableDescriptorJsonSerdeTest() {
        super(TableDescriptorJsonSerde.INSTANCE);
    }

    @Override
    protected TableDescriptor[] createObjects() {
        TableDescriptor[] tableDescriptors = new TableDescriptor[3];

        tableDescriptors[0] =
                TableDescriptor.builder()
                        .schema(SchemaJsonSerdeTest.SCHEMA_0)
                        .comment("first table")
                        .partitionedBy("c")
                        .distributedBy(16, "a")
                        .property("option-1", "100")
                        .property("option-2", "200")
                        .customProperties(Collections.singletonMap("custom-1", "\"value-1\""))
                        .build();

        tableDescriptors[1] =
                TableDescriptor.builder()
                        .schema(SchemaJsonSerdeTest.SCHEMA_1)
                        .distributedBy(32)
                        .property("option-3", "300")
                        .property("option-4", "400")
                        .logFormat(LogFormat.INDEXED)
                        .kvFormat(KvFormat.INDEXED)
                        .build();

        tableDescriptors[2] =
                TableDescriptor.builder()
                        .schema(SchemaJsonSerdeTest.SCHEMA_1)
                        .tableType(TableType.INDEX_TABLE)
                        .parentTableId(42L)
                        .distributedBy(8, "a")
                        .build();

        return tableDescriptors;
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":2,\"schema\":"
                    + SchemaJsonSerdeTest.SCHEMA_JSON_0
                    + ",\"comment\":\"first table\",\"partition_key\":[\"c\"],\"bucket_key\":[\"a\"],\"bucket_count\":16,\"table_type\":\"TABLE\",\"properties\":{\"option-2\":\"200\",\"option-1\":\"100\"},"
                    + "\"custom_properties\":{\"custom-1\":\"\\\"value-1\\\"\"}}",
            "{\"version\":2,\"schema\":"
                    + SchemaJsonSerdeTest.SCHEMA_JSON_1
                    + ",\"partition_key\":[],\"bucket_key\":[\"a\"],\"bucket_count\":32,\"table_type\":\"TABLE\",\"properties\":{\"option-3\":\"300\",\"option-4\":\"400\","
                    + "\"table.log.format\":\"INDEXED\",\"table.kv.format\":\"INDEXED\"},\"custom_properties\":{}}",
            "{\"version\":2,\"schema\":"
                    + SchemaJsonSerdeTest.SCHEMA_JSON_1
                    + ",\"partition_key\":[],\"bucket_key\":[\"a\"],\"bucket_count\":8,\"table_type\":\"INDEX_TABLE\",\"parent_table_id\":42,\"properties\":{},\"custom_properties\":{}}"
        };
    }

    @Test
    void testBackwardCompatibilityDefaultsTableType() {
        String oldJson =
                "{\"version\":1,\"schema\":"
                        + SchemaJsonSerdeTest.SCHEMA_JSON_1
                        + ",\"partition_key\":[],\"bucket_key\":[\"a\"],\"bucket_count\":32,\"properties\":{},\"custom_properties\":{}}";

        TableDescriptor descriptor =
                JsonSerdeUtils.readValue(
                        oldJson.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                        TableDescriptorJsonSerde.INSTANCE);

        assertThat(descriptor.getTableType()).isEqualTo(TableType.TABLE);
        assertThat(descriptor.getParentTableId()).isEmpty();
    }
}
