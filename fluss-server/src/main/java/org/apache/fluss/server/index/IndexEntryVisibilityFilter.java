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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.utils.IndexTableUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Hides versioned Index Table tombstone rows from lookup results.
 *
 * <p>Index push deletes are encoded as ordinary upsert rows with {@code __index_deleted=true} so the
 * Versioned merge engine can reject stale failover writes by {@code __source_offset}. This filter is
 * the read-side half of that contract: point lookup keeps response cardinality by replacing deleted
 * values with {@code null}; prefix lookup removes deleted values from the candidate list.
 */
@Internal
final class IndexEntryVisibilityFilter {

    private final ValueDecoder valueDecoder;
    private final int deletedMarkerPosition;

    private IndexEntryVisibilityFilter(ValueDecoder valueDecoder, int deletedMarkerPosition) {
        this.valueDecoder = valueDecoder;
        this.deletedMarkerPosition = deletedMarkerPosition;
    }

    @Nullable
    static IndexEntryVisibilityFilter forIndexTable(
            TableInfo tableInfo, SchemaGetter schemaGetter) {
        if (!tableInfo.isIndexTable()) {
            return null;
        }
        Schema schema = tableInfo.getSchema();
        int deletedMarkerPosition =
                schema.getColumnNames().indexOf(IndexTableUtils.INDEX_DELETED_SYSTEM_COLUMN);
        if (deletedMarkerPosition < 0) {
            return null;
        }
        int kvFormatVersion =
                tableInfo
                        .getTableConfig()
                        .getKvFormatVersion()
                        .orElse(ConfigOptions.KV_FORMAT_VERSION_2);
        ValueDecoder valueDecoder =
                new ValueDecoder(
                        schemaGetter, tableInfo.getTableConfig().getKvFormat(), kvFormatVersion);
        return new IndexEntryVisibilityFilter(
                valueDecoder, deletedMarkerPosition);
    }

    List<byte[]> filterPointLookup(List<byte[]> rawResults) {
        if (rawResults.isEmpty()) {
            return rawResults;
        }
        List<byte[]> filtered = null;
        for (int i = 0; i < rawResults.size(); i++) {
            byte[] value = rawResults.get(i);
            boolean deleted = value != null && isDeleted(value);
            if (deleted && filtered == null) {
                filtered = new ArrayList<>(rawResults);
            }
            if (deleted) {
                filtered.set(i, null);
            }
        }
        return filtered == null ? rawResults : filtered;
    }

    List<byte[]> filterPrefixLookup(List<byte[]> rawResults) {
        if (rawResults.isEmpty()) {
            return rawResults;
        }
        List<byte[]> filtered = null;
        for (int i = 0; i < rawResults.size(); i++) {
            byte[] value = rawResults.get(i);
            boolean deleted = value != null && isDeleted(value);
            if (deleted && filtered == null) {
                filtered = new ArrayList<>(rawResults.subList(0, i));
            }
            if (filtered != null) {
                if (!deleted) {
                    filtered.add(value);
                }
            }
        }
        return filtered == null ? rawResults : filtered;
    }

    private boolean isDeleted(byte[] valueBytes) {
        BinaryValue value = valueDecoder.decodeValue(valueBytes);
        BinaryRow row = value.row;
        return row.getFieldCount() > deletedMarkerPosition
                && !row.isNullAt(deletedMarkerPosition)
                && row.getBoolean(deletedMarkerPosition);
    }
}
