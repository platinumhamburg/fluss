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

package org.apache.fluss.config;

import org.apache.fluss.metadata.TableType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the namespaced secondary-index related {@link ConfigOptions}. */
class SecondaryIndexConfigOptionsTest {

    @Test
    void testVisibilityDefaultIsSync() {
        assertThat(ConfigOptions.SECONDARY_INDEX_VISIBILITY.defaultValue())
                .isEqualTo(ConfigOptions.SecondaryIndexVisibility.SYNC);
    }

    @Test
    void testPerIndexColumnsKeyUsesNamespacedForm() {
        assertThat(ConfigOptions.secondaryIndexColumnsKey("idx_user"))
                .isEqualTo("secondary-index.idx_user.columns");
    }

    @Test
    void testPerIndexBucketNumKeyUsesNamespacedForm() {
        assertThat(ConfigOptions.secondaryIndexBucketNumKey("idx_user"))
                .isEqualTo("secondary-index.idx_user.bucket.num");
    }

    @Test
    void testNamespacePrefixAndSuffixConstants() {
        assertThat(ConfigOptions.SECONDARY_INDEX_PREFIX).isEqualTo("secondary-index.");
        assertThat(ConfigOptions.SECONDARY_INDEX_COLUMNS_SUFFIX).isEqualTo(".columns");
        assertThat(ConfigOptions.SECONDARY_INDEX_BUCKET_NUM_SUFFIX).isEqualTo(".bucket.num");
    }

    @Test
    void testTableTypeOptionDefaultsToDataTable() {
        assertThat(ConfigOptions.TABLE_TYPE.defaultValue()).isEqualTo(TableType.DATA_TABLE);
        assertThat(ConfigOptions.TABLE_TYPE.key()).isEqualTo("table.type");
    }

    @Test
    void testIndexMetaMainTableIdHasNoDefault() {
        assertThat(ConfigOptions.TABLE_INDEX_META_MAIN_TABLE_ID.key())
                .isEqualTo("table.index-meta.main-table-id");
        assertThat(ConfigOptions.TABLE_INDEX_META_MAIN_TABLE_ID.defaultValue()).isNull();
    }

    @Test
    void testIndexMetaMainTableNameHasNoDefault() {
        assertThat(ConfigOptions.TABLE_INDEX_META_MAIN_TABLE_NAME.key())
                .isEqualTo("table.index-meta.main-table-name");
        assertThat(ConfigOptions.TABLE_INDEX_META_MAIN_TABLE_NAME.defaultValue()).isNull();
    }
}
