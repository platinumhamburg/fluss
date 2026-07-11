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

package org.apache.fluss.server.index;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metadata.TableType;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for hiding versioned Index Table tombstone rows from lookup results. */
class IndexEntryVisibilityFilterTest {

    private static final short SCHEMA_ID = 3;
    private static final Schema INDEX_SCHEMA =
            Schema.newBuilder()
                    .fromColumns(
                            Arrays.asList(
                                    new Schema.Column("idx", DataTypes.BIGINT()),
                                    new Schema.Column("pk", DataTypes.BIGINT()),
                                    new Schema.Column("__source_offset", DataTypes.BIGINT()),
                                    new Schema.Column("__index_deleted", DataTypes.BOOLEAN())))
                    .primaryKey("idx", "pk")
                    .build();

    @Test
    void pointLookupNullsDeletedEntriesAndPreservesPositions() {
        IndexEntryVisibilityFilter filter = newFilter();
        byte[] live = encodeValue(10L, 1L, 7L, false);
        byte[] deleted = encodeValue(10L, 2L, 8L, true);

        List<byte[]> filtered = filter.filterPointLookup(Arrays.asList(live, deleted, null));

        assertThat(filtered).containsExactly(live, null, null);
    }

    @Test
    void prefixLookupRemovesDeletedEntries() {
        IndexEntryVisibilityFilter filter = newFilter();
        byte[] live = encodeValue(10L, 1L, 7L, false);
        byte[] deleted = encodeValue(10L, 2L, 8L, true);

        List<byte[]> filtered = filter.filterPrefixLookup(Arrays.asList(live, deleted));

        assertThat(filtered).containsExactly(live);
    }

    private static IndexEntryVisibilityFilter newFilter() {
        return IndexEntryVisibilityFilter.forIndexTable(
                indexTableInfo(), new TestingSchemaGetter(SCHEMA_ID, INDEX_SCHEMA));
    }

    private static TableInfo indexTableInfo() {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(INDEX_SCHEMA)
                        .kvFormat(KvFormat.COMPACTED)
                        .distributedBy(1, "idx")
                        .property(ConfigOptions.TABLE_TYPE, TableType.INDEX_TABLE)
                        .property(
                                ConfigOptions.TABLE_KV_FORMAT_VERSION,
                                ConfigOptions.KV_FORMAT_VERSION_2)
                        .build();
        return TableInfo.of(
                TablePath.of("test_db", "orders__idx_user"),
                2002L,
                SCHEMA_ID,
                descriptor,
                "file:///tmp/remote",
                1L,
                1L);
    }

    private static byte[] encodeValue(long idx, long pk, long sourceOffset, boolean deleted) {
        DataType[] valueTypes =
                new DataType[] {
                    DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BOOLEAN()
                };
        RowEncoder rowEncoder = RowEncoder.create(KvFormat.COMPACTED, valueTypes);
        rowEncoder.startNewRow();
        rowEncoder.encodeField(0, idx);
        rowEncoder.encodeField(1, pk);
        rowEncoder.encodeField(2, sourceOffset);
        rowEncoder.encodeField(3, deleted);
        BinaryRow row = rowEncoder.finishRow();
        return ValueEncoder.encodeValue(SCHEMA_ID, row);
    }
}
