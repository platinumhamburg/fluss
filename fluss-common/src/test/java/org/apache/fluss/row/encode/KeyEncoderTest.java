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

package org.apache.fluss.row.encode;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KeyEncoder}. */
class KeyEncoderTest {

    @Test
    void testKvFormatVersionThreeUsesVersionTwoKeyEncoding() {
        DataType[] dataTypes = new DataType[] {DataTypes.STRING(), DataTypes.BIGINT()};
        String[] fieldNames = new String[] {"name", "id"};
        RowType rowType = RowType.of(dataTypes, fieldNames);

        KeyEncoder versionTwoEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        rowType,
                        Collections.singletonList("name"),
                        tableConfigWithKvFormatVersion(ConfigOptions.KV_FORMAT_VERSION_2),
                        false);
        KeyEncoder versionThreeEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        rowType,
                        Collections.singletonList("name"),
                        tableConfigWithKvFormatVersion(ConfigOptions.KV_FORMAT_VERSION_3),
                        false);

        assertThat(versionThreeEncoder.encodeKey(row("Alice", 1L)))
                .isEqualTo(versionTwoEncoder.encodeKey(row("Alice", 1L)));
    }

    @Test
    void testMissingKvFormatVersionDefaultsToVersionTwoKeyEncoding() {
        DataType[] dataTypes = new DataType[] {DataTypes.STRING(), DataTypes.BIGINT()};
        String[] fieldNames = new String[] {"name", "id"};
        RowType rowType = RowType.of(dataTypes, fieldNames);

        KeyEncoder versionTwoEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        rowType,
                        Collections.singletonList("name"),
                        paimonTableConfigWithKvFormatVersion(ConfigOptions.KV_FORMAT_VERSION_2),
                        false);
        KeyEncoder missingVersionEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        rowType,
                        Collections.singletonList("name"),
                        paimonTableConfigWithoutKvFormatVersion(),
                        false);
        KeyEncoder versionOneEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        rowType,
                        Collections.singletonList("name"),
                        paimonTableConfigWithKvFormatVersion(1),
                        false);

        byte[] lookupKey = missingVersionEncoder.encodeKey(row("Alice", 1L));
        assertThat(lookupKey).isEqualTo(versionTwoEncoder.encodeKey(row("Alice", 1L)));
        assertThat(lookupKey).isNotEqualTo(versionOneEncoder.encodeKey(row("Alice", 1L)));
    }

    private static TableConfig tableConfigWithKvFormatVersion(int kvFormatVersion) {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.TABLE_KV_FORMAT_VERSION, kvFormatVersion);
        return new TableConfig(conf);
    }

    private static TableConfig paimonTableConfigWithKvFormatVersion(int kvFormatVersion) {
        Configuration conf = paimonConfiguration();
        conf.set(ConfigOptions.TABLE_KV_FORMAT_VERSION, kvFormatVersion);
        return new TableConfig(conf);
    }

    private static TableConfig paimonTableConfigWithoutKvFormatVersion() {
        return new TableConfig(paimonConfiguration());
    }

    private static Configuration paimonConfiguration() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.TABLE_DATALAKE_FORMAT, DataLakeFormat.PAIMON);
        return conf;
    }
}
