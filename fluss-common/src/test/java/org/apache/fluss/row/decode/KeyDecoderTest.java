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

package org.apache.fluss.row.decode;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KeyDecoder}. */
class KeyDecoderTest {

    @Test
    void testKvFormatVersionThreeDecodesVersionTwoKeys() {
        DataType[] dataTypes =
                new DataType[] {DataTypes.INT(), DataTypes.STRING(), DataTypes.BIGINT()};
        String[] fieldNames = new String[] {"id", "name", "value"};
        RowType rowType = RowType.of(dataTypes, fieldNames);

        KeyEncoder versionTwoEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        rowType,
                        Arrays.asList("id", "name"),
                        tableConfigWithKvFormatVersion(ConfigOptions.KV_FORMAT_VERSION_2),
                        false);
        KeyDecoder versionThreeDecoder =
                KeyDecoder.ofPrimaryKeyDecoder(
                        rowType,
                        Arrays.asList("id", "name"),
                        (short) ConfigOptions.KV_FORMAT_VERSION_3,
                        null,
                        false);

        InternalRow decoded =
                versionThreeDecoder.decodeKey(versionTwoEncoder.encodeKey(row(1, "Alice", 10L)));
        assertThat(decoded.getInt(0)).isEqualTo(1);
        assertThat(decoded.getString(1).toString()).isEqualTo("Alice");
    }

    private static TableConfig tableConfigWithKvFormatVersion(int kvFormatVersion) {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.TABLE_KV_FORMAT_VERSION, kvFormatVersion);
        return new TableConfig(conf);
    }
}
