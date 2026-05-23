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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableDescriptorLegacyIndexAliasTest {

    @Test
    void testLegacySingleIndexColumnsTranslatedToNamespacedDefaultName() {
        Map<String, String> props = new HashMap<>();
        props.put("table.secondary-index.columns", "user_id,region_id");

        Map<String, String> normalized = TableDescriptor.translateLegacyIndexProperties(props);

        assertThat(normalized).doesNotContainKey("table.secondary-index.columns");
        assertThat(normalized)
                .containsEntry("secondary-index.idx_default.columns", "user_id,region_id");
        // input map preserved
        assertThat(props).containsKey("table.secondary-index.columns");
    }

    @Test
    void testLegacyMultiGroupSemicolonIsRejected() {
        Map<String, String> props = new HashMap<>();
        props.put("table.secondary-index.columns", "u;v");
        assertThatThrownBy(() -> TableDescriptor.translateLegacyIndexProperties(props))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(";");
    }

    @Test
    void testLegacyAndNamespacedKeysCannotCoexist() {
        Map<String, String> props = new HashMap<>();
        props.put("table.secondary-index.columns", "u");
        props.put("secondary-index.idx_x.columns", "u");
        assertThatThrownBy(() -> TableDescriptor.translateLegacyIndexProperties(props))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot coexist");
    }

    @Test
    void testNoLegacyKeyIsPassThrough() {
        Map<String, String> props = new HashMap<>();
        props.put("secondary-index.idx_user.columns", "user_id");
        Map<String, String> normalized = TableDescriptor.translateLegacyIndexProperties(props);
        assertThat(normalized).isEqualTo(props);
    }

    @Test
    void testEmptyLegacyValueIsPassThrough() {
        Map<String, String> props = new HashMap<>();
        props.put("table.secondary-index.columns", "");
        Map<String, String> normalized = TableDescriptor.translateLegacyIndexProperties(props);
        assertThat(normalized).isEqualTo(props);
    }
}
