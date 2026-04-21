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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableType;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Json serializer and deserializer for {@link TableDescriptor}. */
@Internal
public class TableDescriptorJsonSerde
        implements JsonSerializer<TableDescriptor>, JsonDeserializer<TableDescriptor> {

    public static final TableDescriptorJsonSerde INSTANCE = new TableDescriptorJsonSerde();

    static final String SCHEMA_NAME = "schema";
    static final String COMMENT_NAME = "comment";
    static final String PARTITION_KEY_NAME = "partition_key";
    static final String BUCKET_KEY_NAME = "bucket_key";
    static final String BUCKET_COUNT_NAME = "bucket_count";
    static final String TABLE_TYPE_NAME = "table_type";
    static final String PARENT_TABLE_ID_NAME = "parent_table_id";
    static final String PROPERTIES_NAME = "properties";
    static final String CUSTOM_PROPERTIES_NAME = "custom_properties";

    private static final String VERSION_KEY = "version";
    private static final int VERSION = 2;

    @Override
    public void serialize(TableDescriptor tableDescriptor, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();

        // serialize data version.
        generator.writeNumberField(VERSION_KEY, VERSION);

        // serialize schema
        generator.writeFieldName(SCHEMA_NAME);
        SchemaJsonSerde.INSTANCE.serialize(tableDescriptor.getSchema(), generator);

        // serialize comment.
        if (tableDescriptor.getComment().isPresent()) {
            generator.writeStringField(COMMENT_NAME, tableDescriptor.getComment().get());
        }

        // serialize partition key.
        generator.writeArrayFieldStart(PARTITION_KEY_NAME);
        for (String partitionKey : tableDescriptor.getPartitionKeys()) {
            generator.writeString(partitionKey);
        }
        generator.writeEndArray();

        // serialize tableDistribution.
        if (tableDescriptor.getTableDistribution().isPresent()) {
            TableDescriptor.TableDistribution distribution =
                    tableDescriptor.getTableDistribution().get();
            generator.writeArrayFieldStart(BUCKET_KEY_NAME);
            for (String bucketKey : distribution.getBucketKeys()) {
                generator.writeString(bucketKey);
            }
            generator.writeEndArray();
            if (distribution.getBucketCount().isPresent()) {
                generator.writeNumberField(BUCKET_COUNT_NAME, distribution.getBucketCount().get());
            }
        }

        generator.writeStringField(TABLE_TYPE_NAME, tableDescriptor.getTableType().name());
        if (tableDescriptor.getParentTableId().isPresent()) {
            generator.writeNumberField(
                    PARENT_TABLE_ID_NAME, tableDescriptor.getParentTableId().getAsLong());
        }

        // serialize properties.
        generator.writeObjectFieldStart(PROPERTIES_NAME);
        for (Map.Entry<String, String> entry : tableDescriptor.getProperties().entrySet()) {
            generator.writeObjectField(entry.getKey(), entry.getValue());
        }
        generator.writeEndObject();

        // serialize custom properties.
        generator.writeObjectFieldStart(CUSTOM_PROPERTIES_NAME);
        for (Map.Entry<String, String> entry : tableDescriptor.getCustomProperties().entrySet()) {
            generator.writeObjectField(entry.getKey(), entry.getValue());
        }
        generator.writeEndObject();

        generator.writeEndObject();
    }

    @Override
    public TableDescriptor deserialize(JsonNode node) {
        TableDescriptor.Builder builder = TableDescriptor.builder();
        int version = node.has(VERSION_KEY) ? node.get(VERSION_KEY).asInt() : 1;

        Schema schema = SchemaJsonSerde.INSTANCE.deserialize(node.get(SCHEMA_NAME));
        builder.schema(schema);

        JsonNode commentNode = node.get(COMMENT_NAME);
        if (commentNode != null) {
            builder.comment(commentNode.asText());
        }

        Iterator<JsonNode> partitionJsons = node.get(PARTITION_KEY_NAME).elements();
        List<String> partitionKeys = new ArrayList<>();
        while (partitionJsons.hasNext()) {
            partitionKeys.add(partitionJsons.next().asText());
        }
        builder.partitionedBy(partitionKeys);

        if (node.has(BUCKET_KEY_NAME) || node.has(BUCKET_COUNT_NAME)) {
            Iterator<JsonNode> bucketJsons = node.get(BUCKET_KEY_NAME).elements();
            List<String> bucketKeys = new ArrayList<>();
            while (bucketJsons.hasNext()) {
                bucketKeys.add(bucketJsons.next().asText());
            }

            JsonNode bucketCountNode = node.get(BUCKET_COUNT_NAME);

            if (bucketCountNode != null) {
                builder.distributedBy(bucketCountNode.asInt(), bucketKeys);
            } else {
                builder.distributedBy(null, bucketKeys);
            }
        }

        if (version >= 2 && node.has(TABLE_TYPE_NAME)) {
            builder.tableType(TableType.valueOf(node.get(TABLE_TYPE_NAME).asText()));
        } else {
            builder.tableType(TableType.TABLE);
        }
        if (version >= 2 && node.has(PARENT_TABLE_ID_NAME)) {
            builder.parentTableId(node.get(PARENT_TABLE_ID_NAME).asLong());
        }

        builder.properties(deserializeProperties(node.get(PROPERTIES_NAME)));
        builder.customProperties(deserializeProperties(node.get(CUSTOM_PROPERTIES_NAME)));

        return builder.build();
    }

    private Map<String, String> deserializeProperties(JsonNode node) {
        HashMap<String, String> properties = new HashMap<>();
        Iterator<String> optionsKeys = node.fieldNames();
        while (optionsKeys.hasNext()) {
            String key = optionsKeys.next();
            properties.put(key, node.get(key).asText());
        }
        return properties;
    }
}
