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

package org.apache.fluss.server.utils;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.server.index.IndexTableDescriptorFactory;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link TableDescriptorValidation#validateTableDescriptor} focused on Plan 1's
 * namespaced {@code secondary-index.<name>.{columns,bucket.num}} table properties and the
 * standalone {@code index.visibility} option.
 *
 * <p>The validator was originally a strict allow-list: every property key had to live in the static
 * {@code TABLE_OPTIONS} map. The per-index keys are dynamic (one entry per index declared via
 * {@link Schema.Builder#index}), so they have no static {@link
 * org.apache.fluss.config.ConfigOption} backing them and therefore cannot be in {@code
 * TABLE_OPTIONS}. These tests pin down the namespace handling: per-index suffixes are accepted,
 * {@code index.visibility} still parses, and unknown suffixes (or invalid index names) are rejected
 * with a clear error.
 */
class TableDescriptorValidationTest {

    private static final int MAX_BUCKET_NUM = 1024;
    private static final String INDEX_NAME = "idx_user";

    @Test
    void testValidateAcceptsSecondaryIndexColumnsProperty() {
        TableDescriptor descriptor =
                baseDescriptorBuilder()
                        .property(ConfigOptions.secondaryIndexColumnsKey(INDEX_NAME), "user_id")
                        .build();

        assertThatCode(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        descriptor, MAX_BUCKET_NUM, null))
                .doesNotThrowAnyException();
    }

    @Test
    void testValidateAcceptsSecondaryIndexBucketNumProperty() {
        TableDescriptor descriptor =
                baseDescriptorBuilder()
                        .property(ConfigOptions.secondaryIndexBucketNumKey(INDEX_NAME), "8")
                        .build();

        assertThatCode(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        descriptor, MAX_BUCKET_NUM, null))
                .doesNotThrowAnyException();
    }

    /**
     * {@code index.visibility} is a standalone {@link org.apache.fluss.config.ConfigOption} outside
     * the {@code table.*} namespace; this test guards against regressions that reject it.
     */
    @Test
    void testValidateAcceptsIndexVisibilityProperty() {
        TableDescriptor descriptor =
                baseDescriptorBuilder()
                        .property(ConfigOptions.INDEX_VISIBILITY, IndexVisibility.SYNC)
                        .build();

        assertThatCode(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        descriptor, MAX_BUCKET_NUM, null))
                .doesNotThrowAnyException();
    }

    @Test
    void testValidateRejectsUnknownSecondaryIndexSuffix() {
        TableDescriptor descriptor =
                baseDescriptorBuilder()
                        .property(
                                ConfigOptions.SECONDARY_INDEX_PREFIX + INDEX_NAME + ".weird",
                                "anything")
                        .build();

        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        descriptor, MAX_BUCKET_NUM, null))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("Unknown secondary-index sub-property")
                .hasMessageContaining(".columns")
                .hasMessageContaining(".bucket.num");
    }

    @Test
    void testValidateRejectsSecondaryIndexNameWithDoubleUnderscore() {
        TableDescriptor descriptor =
                baseDescriptorBuilder()
                        .property(ConfigOptions.secondaryIndexBucketNumKey("bad__name"), "1")
                        .build();

        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        descriptor, MAX_BUCKET_NUM, null))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("double underscores");
    }

    @Test
    void testValidateRejectsSecondaryIndexNameWithIllegalCharacter() {
        TableDescriptor descriptor =
                baseDescriptorBuilder()
                        .property(ConfigOptions.secondaryIndexColumnsKey("bad-name"), "user_id")
                        .build();

        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        descriptor, MAX_BUCKET_NUM, null))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("letters, digits, and underscores");
    }

    /**
     * Sanity-check that pre-existing strict validation still applies to non-namespaced unknown keys
     * (i.e. the namespaced bypass did not weaken the surrounding contract).
     */
    @Test
    void testValidateStillRejectsUnknownTablePrefixProperty() {
        TableDescriptor descriptor =
                baseDescriptorBuilder().property("table.totally.bogus", "1").build();

        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        descriptor, MAX_BUCKET_NUM, null))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("'table.totally.bogus'");
    }

    @Test
    void testSecondaryIndexRequiresFullChangelogImage() {
        TableDescriptor walDescriptor =
                baseDescriptorBuilder()
                        .property(ConfigOptions.TABLE_CHANGELOG_IMAGE, ChangelogImage.WAL)
                        .build();

        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        walDescriptor, MAX_BUCKET_NUM, null))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("changelog.image")
                .hasMessageContaining("FULL");
    }

    @Test
    void testSecondaryIndexWithFullChangelogImagePasses() {
        TableDescriptor fullDescriptor =
                baseDescriptorBuilder()
                        .property(ConfigOptions.TABLE_CHANGELOG_IMAGE, ChangelogImage.FULL)
                        .build();

        assertThatCode(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        fullDescriptor, MAX_BUCKET_NUM, null))
                .doesNotThrowAnyException();
    }

    @Test
    void testSecondaryIndexDefaultChangelogImageIsFull() {
        // Default changelog.image is FULL, so tables with indexes should pass without explicit set
        TableDescriptor defaultDescriptor = baseDescriptorBuilder().build();

        assertThatCode(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        defaultDescriptor, MAX_BUCKET_NUM, null))
                .doesNotThrowAnyException();
    }

    @Test
    void testRejectsSecondaryIndexOnUnknownColumn() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .primaryKey("id")
                        .index("idx_missing", "missing")
                        .build();
        TableDescriptor descriptor = descriptorBuilder(schema).build();

        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        descriptor, MAX_BUCKET_NUM, null))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("idx_missing")
                .hasMessageContaining("missing");
    }

    @Test
    void testRejectsSecondaryIndexOnUnsupportedColumnType() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("tags", DataTypes.ARRAY(DataTypes.INT()))
                        .primaryKey("id")
                        .index("idx_tags", "tags")
                        .build();
        TableDescriptor descriptor = descriptorBuilder(schema).build();

        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        descriptor, MAX_BUCKET_NUM, null))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("idx_tags")
                .hasMessageContaining("unsupported type");
    }

    @Test
    void testRejectsSecondaryIndexOnTableWithoutPrimaryKey() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("user_id", DataTypes.BIGINT())
                        .index("idx_user", "user_id")
                        .build();
        TableDescriptor descriptor = descriptorBuilder(schema).build();

        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        descriptor, MAX_BUCKET_NUM, null))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("secondary indexes")
                .hasMessageContaining("primary key");
    }

    @Test
    void testRejectsVersionThreeForDataTable() {
        TableDescriptor descriptor =
                descriptorBuilder(
                                Schema.newBuilder()
                                        .column("id", DataTypes.BIGINT())
                                        .column("user_id", DataTypes.BIGINT())
                                        .primaryKey("id")
                                        .build())
                        .property(
                                ConfigOptions.TABLE_KV_FORMAT_VERSION,
                                ConfigOptions.KV_FORMAT_VERSION_3)
                        .build();

        assertThatThrownBy(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        descriptor, MAX_BUCKET_NUM, null))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("kv format version 3")
                .hasMessageContaining("partitioned secondary index tables");
    }

    @Test
    void testAcceptsVersionThreeForPartitionedIndexTable() {
        TableDescriptor mainDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.BIGINT())
                                        .column("user_id", DataTypes.BIGINT())
                                        .column("dt", DataTypes.STRING())
                                        .primaryKey("id", "dt")
                                        .index(INDEX_NAME, "user_id")
                                        .build())
                        .partitionedBy("dt")
                        .property(ConfigOptions.secondaryIndexBucketNumKey(INDEX_NAME), "1")
                        .build();
        TableDescriptor indexDescriptor =
                IndexTableDescriptorFactory.derive(mainDescriptor, 1L, "db.orders", INDEX_NAME)
                        .withReplicationFactor(1);

        assertThatCode(
                        () ->
                                TableDescriptorValidation.validateTableDescriptor(
                                        indexDescriptor, MAX_BUCKET_NUM, null))
                .doesNotThrowAnyException();
    }

    // ---------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------

    private static Schema buildSchemaWithIndex() {
        return Schema.newBuilder()
                .column("user_id", DataTypes.BIGINT())
                .column("order_id", DataTypes.BIGINT())
                .primaryKey("order_id")
                .index(INDEX_NAME, "user_id")
                .build();
    }

    /**
     * Build a minimally-valid TableDescriptor that {@link
     * TableDescriptorValidation#validateTableDescriptor} accepts: a primary-key schema with one
     * declared index, an explicit bucket count + key, and the mandatory {@code
     * table.replication.factor} property (otherwise {@code checkReplicationFactor} short-circuits
     * before we ever reach the property loop we are testing).
     */
    private static TableDescriptor.Builder baseDescriptorBuilder() {
        return descriptorBuilder(buildSchemaWithIndex());
    }

    private static TableDescriptor.Builder descriptorBuilder(Schema schema) {
        String distributionColumn =
                schema.getPrimaryKey().map(pk -> pk.getColumnNames().get(0)).orElse("id");
        return TableDescriptor.builder()
                .schema(schema)
                .distributedBy(1, distributionColumn)
                .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1);
    }
}
