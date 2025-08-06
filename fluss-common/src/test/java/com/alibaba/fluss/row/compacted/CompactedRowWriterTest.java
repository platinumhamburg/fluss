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

package com.alibaba.fluss.row.compacted;

import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TestInternalRowGenerator;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.row.indexed.IndexedRowTest;
import com.alibaba.fluss.row.indexed.IndexedRowWriter;
import com.alibaba.fluss.testutils.InternalRowAssert;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CompactedRowWriter}. */
class CompactedRowWriterTest {

    @Test
    public void testAllBitsNumber() {
        int intValue = 0;
        for (int i = 0; i < 32; i++) {
            intValue |= 1 << i;
            testInt(intValue);
        }

        long longValue = 0;
        for (int i = 0; i < 64; i++) {
            longValue |= 1L << i;
            testLong(longValue);
        }
    }

    @Test
    public void testRandomNumber() {
        Random rnd = new Random();
        for (int i = 0; i < 10000; i++) {
            testInt(rnd.nextInt());
            testLong(rnd.nextLong());
        }
    }

    private void testInt(int value) {
        CompactedRowWriter writer = new CompactedRowWriter(1);
        writer.writeInt(value);

        CompactedRowReader reader = new CompactedRowReader(1);
        reader.pointTo(writer.segment(), 0, writer.position());
        assertThat(reader.readInt()).isEqualTo(value);
    }

    private void testLong(long value) {
        CompactedRowWriter writer = new CompactedRowWriter(1);
        writer.writeLong(value);

        CompactedRowReader reader = new CompactedRowReader(1);

        // set limit
        reader.pointTo(writer.segment(), 0, writer.position());
        assertThat(reader.readLong()).isEqualTo(value);

        // free limit
        reader.pointTo(writer.segment(), 0, writer.buffer().length);
        assertThat(reader.readLong()).isEqualTo(value);
    }

    @Test
    public void testTypes() {
        DataType[] allDataTypes = TestInternalRowGenerator.createAllTypes();
        CompactedRowWriter writer = new CompactedRowWriter(allDataTypes);
        CompactedRowReader reader = new CompactedRowReader(allDataTypes);
        InternalRow.FieldGetter[] getters = new InternalRow.FieldGetter[allDataTypes.length];
        CompactedRowWriter.FieldWriter[] writers =
                new CompactedRowWriter.FieldWriter[allDataTypes.length];
        CompactedRowReader.FieldReader[] readers =
                new CompactedRowReader.FieldReader[allDataTypes.length];
        for (int i = 0; i < allDataTypes.length; i++) {
            getters[i] = InternalRow.createFieldGetter(allDataTypes[i], i);
            writers[i] = CompactedRowWriter.createFieldWriter(allDataTypes[i]);
            readers[i] = CompactedRowReader.createFieldReader(allDataTypes[i]);
        }
        for (int i = 0; i < 1000; i++) {
            IndexedRowWriter indexedRowWriter = IndexedRowTest.genRecordForAllTypes(allDataTypes);
            IndexedRow indexedRow = new IndexedRow(allDataTypes);
            indexedRow.pointTo(indexedRowWriter.segment(), 0, writer.position());

            // writing compacted rows
            writer.reset();
            for (int j = 0; j < allDataTypes.length; j++) {
                writers[j].writeField(writer, j, getters[j].getFieldOrNull(indexedRow));
            }

            // reading
            reader.pointTo(writer.segment(), 0, writer.position());
            GenericRow readRow = new GenericRow(allDataTypes.length);
            for (int j = 0; j < allDataTypes.length; j++) {
                readRow.setField(j, readers[j].readField(reader, j));
            }
            InternalRowAssert.assertThatRow(readRow)
                    .withSchema(RowType.of(allDataTypes))
                    .isEqualTo(indexedRow);
        }
    }

    private BinaryString randomVarString() {
        Random rnd = new Random();
        return BinaryString.fromString(String.valueOf(rnd.nextLong()));
    }

    @Test
    public void testWriteVarSegments() {
        CompactedRowWriter writer = new CompactedRowWriter(2);
        CompactedRowReader reader = new CompactedRowReader(2);
        for (int i = 0; i < 1; i++) {
            // writing
            writer.reset();
            BinaryString s1 = randomVarString();
            BinaryString s2 = randomVarString();
            BinaryString s3 = randomVarString();
            writer.writeString(s1);
            writer.writeString(s2);
            writer.writeString(s3);

            // reading
            reader.pointTo(writer.segment(), 0, writer.position());
            assertThat(reader.readString()).isEqualTo(s1);
            assertThat(reader.readString()).isEqualTo(s2);
            assertThat(reader.readString()).isEqualTo(s3);
        }
    }

    @Test
    void testTooManyFields() {
        int numFields = 2000;
        CompactedRowWriter compactedRowWriter = new CompactedRowWriter(numFields);

        for (int i = 1; i <= 3; i++) {
            compactedRowWriter.reset();
            for (int j = 0; j < numFields; j++) {
                compactedRowWriter.writeInt(i * j);
            }

            CompactedRowReader compactedRowReader = new CompactedRowReader(numFields);
            compactedRowReader.pointTo(
                    compactedRowWriter.segment(), 0, compactedRowWriter.position());
            for (int j = 0; j < numFields; j++) {
                assertThat(compactedRowReader.readInt()).isEqualTo(i * j);
            }
        }
    }
}
