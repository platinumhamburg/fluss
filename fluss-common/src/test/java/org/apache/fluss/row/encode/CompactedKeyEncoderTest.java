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
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.compacted.CompactedRowDeserializer;
import org.apache.fluss.row.compacted.CompactedRowReader;
import org.apache.fluss.row.encode.hudi.HudiKeyEncoder;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.row.indexed.IndexedRowTest;
import org.apache.fluss.row.indexed.IndexedRowWriter;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.apache.fluss.row.TestInternalRowGenerator.createAllRowType;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link CompactedKeyEncoder}. */
class CompactedKeyEncoderTest {

    @Test
    void testEncodeKey() {
        // test int, long as primary key
        final RowType rowType = RowType.of(DataTypes.INT(), DataTypes.BIGINT(), DataTypes.INT());
        InternalRow row = row(1, 3L, 2);
        CompactedKeyEncoder encoder = new CompactedKeyEncoder(rowType);

        byte[] bytes = encoder.encodeKey(row);
        assertThat(bytes).isEqualTo(new byte[] {1, 3, 2});

        row = row(2, 5L, 6);
        bytes = encoder.encodeKey(row);
        assertThat(bytes).isEqualTo(new byte[] {2, 5, 6});
    }

    @Test
    void testEncodeKeyWithKeyNames() {
        final DataType[] dataTypes =
                new DataType[] {DataTypes.STRING(), DataTypes.BIGINT(), DataTypes.STRING()};
        final String[] fieldNames = new String[] {"partition", "f1", "f2"};
        final RowType rowType = RowType.of(dataTypes, fieldNames);

        InternalRow row = row("p1", 1L, "a2");
        List<String> pk = Collections.singletonList("f2");

        CompactedKeyEncoder keyEncoder = CompactedKeyEncoder.createKeyEncoder(rowType, pk);
        byte[] encodedBytes = keyEncoder.encodeKey(row);

        // decode it, should only get "a2"
        InternalRow encodedKey =
                decodeRow(
                        new DataType[] {
                            DataTypes.STRING().copy(false),
                        },
                        encodedBytes);
        assertThat(encodedKey.getFieldCount()).isEqualTo(1);
        assertThat(encodedKey.getString(0).toString()).isEqualTo("a2");
    }

    @Test
    void testBucketKeyEncoderReusesPrimaryKeyEncoderWhenCompatible() {
        RowType rowType = RowType.of(new DataType[] {DataTypes.INT()}, new String[] {"id"});
        List<String> primaryKeys = Collections.singletonList("id");
        KeyEncoder primaryKeyEncoder = CompactedKeyEncoder.createKeyEncoder(rowType, primaryKeys);

        assertThat(
                        KeyEncoder.ofBucketKeyEncoder(
                                rowType, primaryKeys, tableConfig(), true, primaryKeyEncoder))
                .isSameAs(primaryKeyEncoder);
    }

    @Test
    void testHudiBucketKeyEncoderDoesNotReusePrimaryKeyEncoder() {
        RowType rowType = RowType.of(new DataType[] {DataTypes.INT()}, new String[] {"id"});
        List<String> primaryKeys = Collections.singletonList("id");
        KeyEncoder primaryKeyEncoder = CompactedKeyEncoder.createKeyEncoder(rowType, primaryKeys);

        KeyEncoder bucketKeyEncoder =
                KeyEncoder.ofBucketKeyEncoder(
                        rowType,
                        primaryKeys,
                        tableConfig(DataLakeFormat.HUDI),
                        true,
                        primaryKeyEncoder);

        assertThat(bucketKeyEncoder).isInstanceOf(HudiKeyEncoder.class);
        assertThat(bucketKeyEncoder).isNotSameAs(primaryKeyEncoder);
    }

    @Test
    void testGetKey() {
        // test int, long as primary key
        final RowType rowType =
                RowType.of(
                        DataTypes.INT(), DataTypes.BIGINT(), DataTypes.INT(), DataTypes.STRING());
        int[] pkIndexes = new int[] {0, 1, 2};
        final CompactedKeyEncoder compactedKeyEncoder = new CompactedKeyEncoder(rowType, pkIndexes);

        InternalRow row = row(1, 3L, 2, "a1");

        byte[] keyBytes = compactedKeyEncoder.encodeKey(row);
        assertThat(keyBytes).isEqualTo(new byte[] {1, 3, 2});

        // should throw exception when the column is null
        assertThatThrownBy(
                        () -> {
                            InternalRow nullRow = row(1, 2L, null, "a2");
                            compactedKeyEncoder.encodeKey(nullRow);
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Null value is not allowed for compacted key encoder in position 2 with type INT.");

        // test int, string as primary key
        RowType rowType1 =
                RowType.of(
                        DataTypes.STRING(),
                        DataTypes.INT(),
                        DataTypes.STRING(),
                        DataTypes.STRING());
        pkIndexes = new int[] {1, 2};
        final CompactedKeyEncoder keyEncoder1 = new CompactedKeyEncoder(rowType1, pkIndexes);
        row =
                row(
                        BinaryString.fromString("a1"),
                        1,
                        BinaryString.fromString("a2"),
                        BinaryString.fromString("a3"));
        keyBytes = keyEncoder1.encodeKey(row);

        InternalRow keyRow =
                decodeRow(
                        new DataType[] {
                            DataTypes.INT().copy(false), DataTypes.STRING().copy(false),
                        },
                        keyBytes);
        assertThat(keyRow.getInt(0)).isEqualTo(1);
        assertThat(keyRow.getString(1).toString()).isEqualTo("a2");
    }

    @Test
    void testEncodeKeyCanBeCalledConcurrently() throws Exception {
        final RowType rowType = RowType.of(DataTypes.INT(), DataTypes.INT());
        final CompactedKeyEncoder keyEncoder = new CompactedKeyEncoder(rowType);
        CountDownLatch firstEncodeReady = new CountDownLatch(1);
        CountDownLatch secondEncodeDone = new CountDownLatch(1);
        InternalRow blockedRow =
                new BlockingSecondFieldRow(row(1, 2), firstEncodeReady, secondEncodeDone);
        InternalRow interferingRow = row(3, 4);

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        try {
            Future<byte[]> blockedEncode =
                    executorService.submit(() -> keyEncoder.encodeKey(blockedRow));
            assertThat(firstEncodeReady.await(10, TimeUnit.SECONDS)).isTrue();

            Future<byte[]> interferingEncode =
                    executorService.submit(
                            () -> {
                                try {
                                    return keyEncoder.encodeKey(interferingRow);
                                } finally {
                                    secondEncodeDone.countDown();
                                }
                            });

            assertThat(interferingEncode.get(10, TimeUnit.SECONDS)).isEqualTo(new byte[] {3, 4});
            assertThat(blockedEncode.get(10, TimeUnit.SECONDS)).isEqualTo(new byte[] {1, 2});
        } finally {
            secondEncodeDone.countDown();
            executorService.shutdownNow();
        }
    }

    @Test
    void testGetKeyForAllTypes() throws Exception {
        // just test the InternalRowKeyGetter can handle all datatypes as primary key
        RowType rowType = createAllRowType();
        DataType[] dataTypes = rowType.getChildren().toArray(new DataType[0]);
        try (IndexedRowWriter writer = IndexedRowTest.genRecordForAllTypes(dataTypes)) {
            IndexedRow row = new IndexedRow(dataTypes);
            row.pointTo(writer.segment(), 0, writer.position());
            // the last column will be null, we exclude the last column as primary key
            int[] pkIndexes = IntStream.range(0, rowType.getFieldCount() - 1).toArray();
            DataType[] keyDataTypes = new DataType[pkIndexes.length];
            for (int i = 0; i < pkIndexes.length; i++) {
                keyDataTypes[i] = dataTypes[pkIndexes[i]].copy(false);
            }

            final CompactedKeyEncoder keyEncoder = new CompactedKeyEncoder(rowType, pkIndexes);
            byte[] keyBytes = keyEncoder.encodeKey(row);

            InternalRow keyRow = decodeRow(keyDataTypes, keyBytes);

            // get the field getter for the key field
            InternalRow.FieldGetter[] fieldGetters =
                    new InternalRow.FieldGetter[keyDataTypes.length];
            for (int i = 0; i < keyDataTypes.length; i++) {
                fieldGetters[i] = InternalRow.createFieldGetter(keyDataTypes[i], i);
            }
            // get the field from key row and origin row, and then check each field
            for (int i = 0; i < keyDataTypes.length; i++) {
                assertThat(fieldGetters[i].getFieldOrNull(keyRow))
                        .as("Field " + i + " of type " + keyDataTypes[i])
                        .isEqualTo(fieldGetters[i].getFieldOrNull(row));
            }
        }
    }

    private InternalRow decodeRow(DataType[] dataTypes, byte[] values) {
        // use 0 as field count, then the null bits will be 0
        CompactedRowReader compactedRowReader = new CompactedRowReader(0);
        compactedRowReader.pointTo(MemorySegment.wrap(values), 0, values.length);

        CompactedRowDeserializer compactedRowDeserializer = new CompactedRowDeserializer(dataTypes);
        GenericRow genericRow = new GenericRow(dataTypes.length);
        compactedRowDeserializer.deserialize(compactedRowReader, genericRow);
        return genericRow;
    }

    private static TableConfig tableConfig() {
        return new TableConfig(new Configuration());
    }

    private static TableConfig tableConfig(DataLakeFormat dataLakeFormat) {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.TABLE_DATALAKE_FORMAT, dataLakeFormat);
        return new TableConfig(configuration);
    }

    private static final class BlockingSecondFieldRow implements InternalRow {

        private final InternalRow delegate;
        private final CountDownLatch firstEncodeReady;
        private final CountDownLatch secondEncodeDone;

        private BlockingSecondFieldRow(
                InternalRow delegate,
                CountDownLatch firstEncodeReady,
                CountDownLatch secondEncodeDone) {
            this.delegate = delegate;
            this.firstEncodeReady = firstEncodeReady;
            this.secondEncodeDone = secondEncodeDone;
        }

        @Override
        public int getFieldCount() {
            return delegate.getFieldCount();
        }

        @Override
        public boolean isNullAt(int pos) {
            return delegate.isNullAt(pos);
        }

        @Override
        public boolean getBoolean(int pos) {
            return delegate.getBoolean(pos);
        }

        @Override
        public byte getByte(int pos) {
            return delegate.getByte(pos);
        }

        @Override
        public short getShort(int pos) {
            return delegate.getShort(pos);
        }

        @Override
        public int getInt(int pos) {
            if (pos == 1) {
                firstEncodeReady.countDown();
                try {
                    assertThat(secondEncodeDone.await(10, TimeUnit.SECONDS)).isTrue();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
            return delegate.getInt(pos);
        }

        @Override
        public long getLong(int pos) {
            return delegate.getLong(pos);
        }

        @Override
        public float getFloat(int pos) {
            return delegate.getFloat(pos);
        }

        @Override
        public double getDouble(int pos) {
            return delegate.getDouble(pos);
        }

        @Override
        public BinaryString getChar(int pos, int length) {
            return delegate.getChar(pos, length);
        }

        @Override
        public BinaryString getString(int pos) {
            return delegate.getString(pos);
        }

        @Override
        public org.apache.fluss.row.Decimal getDecimal(int pos, int precision, int scale) {
            return delegate.getDecimal(pos, precision, scale);
        }

        @Override
        public org.apache.fluss.row.TimestampNtz getTimestampNtz(int pos, int precision) {
            return delegate.getTimestampNtz(pos, precision);
        }

        @Override
        public org.apache.fluss.row.TimestampLtz getTimestampLtz(int pos, int precision) {
            return delegate.getTimestampLtz(pos, precision);
        }

        @Override
        public byte[] getBinary(int pos, int length) {
            return delegate.getBinary(pos, length);
        }

        @Override
        public byte[] getBytes(int pos) {
            return delegate.getBytes(pos);
        }

        @Override
        public org.apache.fluss.row.InternalArray getArray(int pos) {
            return delegate.getArray(pos);
        }

        @Override
        public org.apache.fluss.row.InternalMap getMap(int pos) {
            return delegate.getMap(pos);
        }

        @Override
        public InternalRow getRow(int pos, int numFields) {
            return delegate.getRow(pos, numFields);
        }
    }
}
