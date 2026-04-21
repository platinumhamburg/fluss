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
import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Json serializer and deserializer for {@link Schema}. */
@Internal
public class SchemaJsonSerde implements JsonSerializer<Schema>, JsonDeserializer<Schema> {

    public static final SchemaJsonSerde INSTANCE = new SchemaJsonSerde();

    private static final String COLUMNS_NAME = "columns";
    private static final String PRIMARY_KEY_NAME = "primary_key";
    private static final String AUTO_INCREMENT_COLUMN_NAME = "auto_increment_column";
    private static final String INDEXES_NAME = "indexes";
    private static final String INDEX_NAME_FIELD = "name";
    private static final String INDEX_TYPE_FIELD = "index_type";
    private static final String INDEX_COLUMNS_FIELD = "columns";
    private static final String INDEX_PROPERTIES_FIELD = "properties";
    private static final String VERSION_KEY = "version";
    private static final String HIGHEST_FIELD_ID = "highest_field_id";
    private static final int VERSION = 2;

    @Override
    public void serialize(Schema schema, JsonGenerator generator) throws IOException {
        generator.writeStartObject();

        // serialize data version.
        // Write version 2 only when indexes are present, otherwise version 1
        int version = schema.getIndexes().isEmpty() ? 1 : VERSION;
        generator.writeNumberField(VERSION_KEY, version);

        // serialize columns name.
        generator.writeArrayFieldStart(COLUMNS_NAME);
        for (Schema.Column column : schema.getColumns()) {
            ColumnJsonSerde.INSTANCE.serialize(column, generator);
        }
        generator.writeEndArray();

        Optional<Schema.PrimaryKey> primaryKey = schema.getPrimaryKey();
        if (primaryKey.isPresent()) {
            generator.writeArrayFieldStart(PRIMARY_KEY_NAME);
            for (String columnName : primaryKey.get().getColumnNames()) {
                generator.writeString(columnName);
            }
            generator.writeEndArray();
        }
        List<String> autoIncrementColumnNames = schema.getAutoIncrementColumnNames();
        if (!autoIncrementColumnNames.isEmpty()) {
            generator.writeArrayFieldStart(AUTO_INCREMENT_COLUMN_NAME);
            for (String columnName : autoIncrementColumnNames) {
                generator.writeString(columnName);
            }
            generator.writeEndArray();
        }

        generator.writeNumberField(HIGHEST_FIELD_ID, schema.getHighestFieldId());

        // serialize indexes
        List<Schema.Index> indexes = schema.getIndexes();
        if (!indexes.isEmpty()) {
            generator.writeArrayFieldStart(INDEXES_NAME);
            for (Schema.Index index : indexes) {
                generator.writeStartObject();
                generator.writeStringField(INDEX_NAME_FIELD, index.getIndexName());
                generator.writeStringField(INDEX_TYPE_FIELD, index.getIndexType().name());
                generator.writeArrayFieldStart(INDEX_COLUMNS_FIELD);
                for (String columnName : index.getColumnNames()) {
                    generator.writeString(columnName);
                }
                generator.writeEndArray();
                Map<String, String> props = index.getProperties();
                if (!props.isEmpty()) {
                    generator.writeObjectFieldStart(INDEX_PROPERTIES_FIELD);
                    for (Map.Entry<String, String> entry : props.entrySet()) {
                        generator.writeStringField(entry.getKey(), entry.getValue());
                    }
                    generator.writeEndObject();
                }
                generator.writeEndObject();
            }
            generator.writeEndArray();
        }

        generator.writeEndObject();
    }

    @Override
    public Schema deserialize(JsonNode node) {
        // Handle backward compatibility - version 1 doesn't have indexes
        int version = node.has(VERSION_KEY) ? node.get(VERSION_KEY).asInt() : 1;
        Iterator<JsonNode> columnJsons = node.get(COLUMNS_NAME).elements();
        List<Schema.Column> columns = new ArrayList<>();
        while (columnJsons.hasNext()) {
            columns.add(ColumnJsonSerde.INSTANCE.deserialize(columnJsons.next()));
        }
        Schema.Builder builder = Schema.newBuilder().fromColumns(columns);

        if (node.has(PRIMARY_KEY_NAME)) {
            Iterator<JsonNode> primaryKeyJsons = node.get(PRIMARY_KEY_NAME).elements();
            List<String> primaryKeys = new ArrayList<>();
            while (primaryKeyJsons.hasNext()) {
                primaryKeys.add(primaryKeyJsons.next().asText());
            }
            builder.primaryKey(primaryKeys);
        }

        if (node.has(AUTO_INCREMENT_COLUMN_NAME)) {
            Iterator<JsonNode> autoIncrementColumnJsons =
                    node.get(AUTO_INCREMENT_COLUMN_NAME).elements();
            while (autoIncrementColumnJsons.hasNext()) {
                builder.enableAutoIncrement(autoIncrementColumnJsons.next().asText());
            }
        }

        if (node.has(HIGHEST_FIELD_ID)) {
            builder.highestFieldId(node.get(HIGHEST_FIELD_ID).asInt());
        }

        // deserialize indexes (only available in version 2 and later)
        if (version >= 2 && node.has(INDEXES_NAME)) {
            Iterator<JsonNode> indexJsons = node.get(INDEXES_NAME).elements();
            while (indexJsons.hasNext()) {
                JsonNode indexNode = indexJsons.next();
                String indexName = indexNode.get(INDEX_NAME_FIELD).asText();

                // index_type: default to SECONDARY for backward compatibility
                IndexType indexType = IndexType.SECONDARY;
                if (indexNode.has(INDEX_TYPE_FIELD)) {
                    String typeName = indexNode.get(INDEX_TYPE_FIELD).asText();
                    try {
                        indexType = IndexType.valueOf(typeName);
                    } catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException(
                                String.format(
                                        "Unknown index type '%s' for index '%s'. "
                                                + "You may need to upgrade to a newer version.",
                                        typeName, indexName),
                                e);
                    }
                }

                Iterator<JsonNode> indexColumnJsons = indexNode.get(INDEX_COLUMNS_FIELD).elements();
                List<String> indexColumns = new ArrayList<>();
                while (indexColumnJsons.hasNext()) {
                    indexColumns.add(indexColumnJsons.next().asText());
                }

                // properties: default to empty map for backward compatibility
                Map<String, String> properties = Collections.emptyMap();
                if (indexNode.has(INDEX_PROPERTIES_FIELD)) {
                    properties = new LinkedHashMap<>();
                    Iterator<Map.Entry<String, JsonNode>> fields =
                            indexNode.get(INDEX_PROPERTIES_FIELD).fields();
                    while (fields.hasNext()) {
                        Map.Entry<String, JsonNode> field = fields.next();
                        properties.put(field.getKey(), field.getValue().asText());
                    }
                }

                builder.index(indexName, indexType, indexColumns, properties);
            }
        }

        return builder.build();
    }
}
