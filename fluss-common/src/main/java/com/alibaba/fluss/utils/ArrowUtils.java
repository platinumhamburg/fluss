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

package com.alibaba.fluss.utils;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.compression.ArrowCompressionFactory;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.row.DataGetters;
import com.alibaba.fluss.row.arrow.ArrowReader;
import com.alibaba.fluss.row.arrow.vectors.ArrowArrayColumnVector;
import com.alibaba.fluss.row.arrow.vectors.ArrowBigIntColumnVector;
import com.alibaba.fluss.row.arrow.vectors.ArrowBinaryColumnVector;
import com.alibaba.fluss.row.arrow.vectors.ArrowBooleanColumnVector;
import com.alibaba.fluss.row.arrow.vectors.ArrowDateColumnVector;
import com.alibaba.fluss.row.arrow.vectors.ArrowDecimalColumnVector;
import com.alibaba.fluss.row.arrow.vectors.ArrowDoubleColumnVector;
import com.alibaba.fluss.row.arrow.vectors.ArrowFloatColumnVector;
import com.alibaba.fluss.row.arrow.vectors.ArrowIntColumnVector;
import com.alibaba.fluss.row.arrow.vectors.ArrowMapColumnVector;
import com.alibaba.fluss.row.arrow.vectors.ArrowRowColumnVector;
import com.alibaba.fluss.row.arrow.vectors.ArrowSmallIntColumnVector;
import com.alibaba.fluss.row.arrow.vectors.ArrowTimeColumnVector;
import com.alibaba.fluss.row.arrow.vectors.ArrowTimestampLtzColumnVector;
import com.alibaba.fluss.row.arrow.vectors.ArrowTimestampNtzColumnVector;
import com.alibaba.fluss.row.arrow.vectors.ArrowTinyIntColumnVector;
import com.alibaba.fluss.row.arrow.vectors.ArrowVarBinaryColumnVector;
import com.alibaba.fluss.row.arrow.vectors.ArrowVarCharColumnVector;
import com.alibaba.fluss.row.arrow.writers.ArrowArrayWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowBigIntWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowBinaryWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowBooleanWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowDateWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowDecimalWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowDoubleWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowFieldWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowFloatWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowIntWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowMapWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowRowWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowSmallIntWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowTimeWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowTimestampLtzWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowTimestampNtzWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowTinyIntWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowVarBinaryWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowVarCharWriter;
import com.alibaba.fluss.row.columnar.ColumnVector;
import com.alibaba.fluss.shaded.arrow.com.google.flatbuffers.FlatBufferBuilder;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.flatbuf.MessageHeader;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.flatbuf.RecordBatch;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.memory.ArrowBuf;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.BigIntVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.BitVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.DateDayVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.DecimalVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.FieldVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.FixedSizeBinaryVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.Float4Vector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.Float8Vector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.IntVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.SmallIntVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.TimeMicroVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.TimeMilliVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.TimeNanoVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.TimeSecVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.TimeStampVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.TinyIntVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.TypeLayout;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.ValueVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.VarBinaryVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.VarCharVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.VectorLoader;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.complex.ListVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.complex.MapVector;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.compression.NoCompressionCodec;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.ipc.ReadChannel;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.ipc.WriteChannel;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowBodyCompression;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowBuffer;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.FBSerializables;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.IpcOption;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.MessageSerializer;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.types.DateUnit;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.types.FloatingPointPrecision;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.types.TimeUnit;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.types.Types;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.ArrowType;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Field;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.FieldType;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Schema;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.util.DataSizeRoundingUtil;
import com.alibaba.fluss.types.ArrayType;
import com.alibaba.fluss.types.BigIntType;
import com.alibaba.fluss.types.BinaryType;
import com.alibaba.fluss.types.BooleanType;
import com.alibaba.fluss.types.BytesType;
import com.alibaba.fluss.types.CharType;
import com.alibaba.fluss.types.DataField;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypeDefaultVisitor;
import com.alibaba.fluss.types.DateType;
import com.alibaba.fluss.types.DecimalType;
import com.alibaba.fluss.types.DoubleType;
import com.alibaba.fluss.types.FloatType;
import com.alibaba.fluss.types.IntType;
import com.alibaba.fluss.types.LocalZonedTimestampType;
import com.alibaba.fluss.types.MapType;
import com.alibaba.fluss.types.MultisetType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.types.SmallIntType;
import com.alibaba.fluss.types.StringType;
import com.alibaba.fluss.types.TimeType;
import com.alibaba.fluss.types.TimestampType;
import com.alibaba.fluss.types.TinyIntType;
import com.alibaba.fluss.types.VarBinaryType;
import com.alibaba.fluss.types.VarCharType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.MessageSerializer.deserializeRecordBatch;
import static com.alibaba.fluss.utils.Preconditions.checkArgument;

/** Utilities for Arrow. */
@Internal
public class ArrowUtils {

    static final String PARQUET_FIELD_NAME = "PARQUET:field_name";

    /** Returns the Arrow schema of the specified type. */
    public static Schema toArrowSchema(RowType rowType) {
        List<Field> fields =
                rowType.getFields().stream()
                        .map(f -> toArrowField(f.getName(), f.getType(), 0))
                        .collect(Collectors.toList());
        return new Schema(fields);
    }

    /**
     * Creates an {@link ArrowReader} for the specified memory segment and {@link VectorSchemaRoot}.
     */
    public static ArrowReader createArrowReader(
            MemorySegment segment,
            int arrowOffset,
            int arrowLength,
            VectorSchemaRoot schemaRoot,
            BufferAllocator allocator,
            RowType rowType) {
        ByteBuffer arrowBatchBuffer = segment.wrap(arrowOffset, arrowLength);
        try (ReadChannel channel =
                        new ReadChannel(new ByteBufferReadableChannel(arrowBatchBuffer));
                ArrowRecordBatch batch = deserializeRecordBatch(channel, allocator)) {
            VectorLoader vectorLoader =
                    new VectorLoader(schemaRoot, ArrowCompressionFactory.INSTANCE);
            vectorLoader.load(batch);
            List<ColumnVector> columnVectors = new ArrayList<>();
            List<FieldVector> fieldVectors = schemaRoot.getFieldVectors();
            for (int i = 0; i < fieldVectors.size(); i++) {
                columnVectors.add(
                        createArrowColumnVector(fieldVectors.get(i), rowType.getTypeAt(i)));
            }
            return new ArrowReader(schemaRoot, columnVectors.toArray(new ColumnVector[0]));
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize ArrowRecordBatch.", e);
        }
    }

    /**
     * Serialize metadata of a {@link ArrowRecordBatch} into write channel. This avoids to create an
     * instance of {@link ArrowRecordBatch}.
     *
     * @return the serialized size in bytes
     * @see MessageSerializer#serialize(WriteChannel, ArrowRecordBatch)
     * @see ArrowRecordBatch#writeTo(FlatBufferBuilder)
     */
    public static int serializeArrowRecordBatchMetadata(
            WriteChannel writeChannel,
            long numRecords,
            List<ArrowFieldNode> nodes,
            List<ArrowBuffer> buffersLayout,
            ArrowBodyCompression arrowBodyCompression,
            long arrowBodyLength)
            throws IOException {
        checkArgument(arrowBodyLength % 8 == 0, "batch is not aligned");
        FlatBufferBuilder builder = new FlatBufferBuilder();

        RecordBatch.startNodesVector(builder, nodes.size());
        int nodesOffset = FBSerializables.writeAllStructsToVector(builder, nodes);
        RecordBatch.startBuffersVector(builder, buffersLayout.size());
        int buffersOffset = FBSerializables.writeAllStructsToVector(builder, buffersLayout);
        int compressOffset = 0;
        if (arrowBodyCompression.getCodec() != NoCompressionCodec.COMPRESSION_TYPE) {
            compressOffset = arrowBodyCompression.writeTo(builder);
        }

        RecordBatch.startRecordBatch(builder);
        RecordBatch.addLength(builder, numRecords);
        RecordBatch.addNodes(builder, nodesOffset);
        RecordBatch.addBuffers(builder, buffersOffset);
        if (arrowBodyCompression.getCodec() != NoCompressionCodec.COMPRESSION_TYPE) {
            RecordBatch.addCompression(builder, compressOffset);
        }
        int batchOffset = RecordBatch.endRecordBatch(builder);
        ByteBuffer metadata =
                MessageSerializer.serializeMessage(
                        builder,
                        MessageHeader.RecordBatch,
                        batchOffset,
                        arrowBodyLength,
                        IpcOption.DEFAULT);

        return MessageSerializer.writeMessageBuffer(writeChannel, metadata.remaining(), metadata);
    }

    /** Estimates the size of {@link ArrowRecordBatch} metadata for the given schema. */
    public static int estimateArrowMetadataLength(
            Schema arrowSchema, ArrowBodyCompression bodyCompression) {
        List<Field> fields = flattenFields(arrowSchema.getFields());
        List<ArrowFieldNode> nodes = createFieldNodes(fields);
        List<ArrowBuffer> buffersLayout = createBuffersLayout(fields);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        WriteChannel writeChannel = new WriteChannel(Channels.newChannel(out));
        try {
            return ArrowUtils.serializeArrowRecordBatchMetadata(
                    writeChannel, 1L, nodes, buffersLayout, bodyCompression, 8L);
        } catch (IOException e) {
            throw new FlussRuntimeException("Failed to estimate Arrow metadata size", e);
        }
    }

    public static long estimateArrowBodyLength(VectorSchemaRoot root) {
        long bufferSize = 0;
        for (FieldVector vector : root.getFieldVectors()) {
            for (ArrowBuf buf : vector.getFieldBuffers()) {
                bufferSize += buf.readableBytes();
                bufferSize = DataSizeRoundingUtil.roundUpTo8Multiple(bufferSize);
            }
        }
        return bufferSize;
    }

    // ------------------------------------------------------------------------------------------

    private static List<Field> flattenFields(List<Field> fields) {
        List<Field> allFields = new ArrayList<>();
        for (Field f : fields) {
            allFields.add(f);
            allFields.addAll(flattenFields(f.getChildren()));
        }
        return allFields;
    }

    private static List<ArrowFieldNode> createFieldNodes(List<Field> fields) {
        List<ArrowFieldNode> fieldNodes = new ArrayList<>();
        for (Field ignored : fields) {
            // use dummy values for now, which is ok for just estimating the size
            fieldNodes.add(new ArrowFieldNode(1L, 1L));
        }
        return fieldNodes;
    }

    private static List<ArrowBuffer> createBuffersLayout(List<Field> fields) {
        List<ArrowBuffer> buffers = new ArrayList<>();
        for (Field f : fields) {
            int bufferLayoutCount = TypeLayout.getTypeBufferCount(f.getType());
            for (int i = 0; i < bufferLayoutCount; i++) {
                // use dummy values for now, which is ok for just estimating the size
                buffers.add(new ArrowBuffer(1L, 1L));
            }
        }
        return buffers;
    }

    public static ArrowFieldWriter<DataGetters> createArrowFieldWriter(
            ValueVector vector, DataType dataType) {
        if (vector instanceof TinyIntVector) {
            return ArrowTinyIntWriter.forField((TinyIntVector) vector);
        } else if (vector instanceof SmallIntVector) {
            return ArrowSmallIntWriter.forField((SmallIntVector) vector);
        } else if (vector instanceof IntVector) {
            return ArrowIntWriter.forField((IntVector) vector);
        } else if (vector instanceof BigIntVector) {
            return ArrowBigIntWriter.forField((BigIntVector) vector);
        } else if (vector instanceof BitVector) {
            return ArrowBooleanWriter.forField((BitVector) vector);
        } else if (vector instanceof Float4Vector) {
            return ArrowFloatWriter.forField((Float4Vector) vector);
        } else if (vector instanceof Float8Vector) {
            return ArrowDoubleWriter.forField((Float8Vector) vector);
        } else if (vector instanceof VarCharVector) {
            return ArrowVarCharWriter.forField((VarCharVector) vector);
        } else if (vector instanceof FixedSizeBinaryVector) {
            return ArrowBinaryWriter.forField((FixedSizeBinaryVector) vector);
        } else if (vector instanceof VarBinaryVector) {
            return ArrowVarBinaryWriter.forField((VarBinaryVector) vector);
        } else if (vector instanceof DecimalVector) {
            DecimalVector decimalVector = (DecimalVector) vector;
            return ArrowDecimalWriter.forField(
                    decimalVector, getPrecision(decimalVector), decimalVector.getScale());
        } else if (vector instanceof DateDayVector) {
            return ArrowDateWriter.forField((DateDayVector) vector);
        } else if (vector instanceof TimeSecVector
                || vector instanceof TimeMilliVector
                || vector instanceof TimeMicroVector
                || vector instanceof TimeNanoVector) {
            return ArrowTimeWriter.forField(vector);
        } else if (vector instanceof TimeStampVector
                && ((ArrowType.Timestamp) vector.getField().getType()).getTimezone() == null) {
            int precision;
            if (dataType instanceof LocalZonedTimestampType) {
                precision = ((LocalZonedTimestampType) dataType).getPrecision();
                return ArrowTimestampLtzWriter.forField(vector, precision);
            } else {
                precision = ((TimestampType) dataType).getPrecision();
                return ArrowTimestampNtzWriter.forField(vector, precision);
            }
        } else if (vector instanceof ListVector && dataType instanceof ArrayType) {
            DataType elementType = ((ArrayType) dataType).getElementType();
            return ArrowArrayWriter.forField(vector, elementType);
        } else if (vector instanceof MapVector && dataType instanceof MapType) {
            DataType keyType = ((MapType) dataType).getKeyType();
            DataType valueType = ((MapType) dataType).getValueType();
            return ArrowMapWriter.forField(vector, keyType, valueType);
        } else if (dataType instanceof RowType) {
            return ArrowRowWriter.forField((FieldVector) vector, (RowType) dataType);
        } else {
            throw new UnsupportedOperationException(
                    String.format("Unsupported type %s.", dataType));
        }
    }

    public static ColumnVector createArrowColumnVector(ValueVector vector, DataType dataType) {
        if (vector instanceof TinyIntVector) {
            return new ArrowTinyIntColumnVector((TinyIntVector) vector);
        } else if (vector instanceof SmallIntVector) {
            return new ArrowSmallIntColumnVector((SmallIntVector) vector);
        } else if (vector instanceof IntVector) {
            return new ArrowIntColumnVector((IntVector) vector);
        } else if (vector instanceof BigIntVector) {
            return new ArrowBigIntColumnVector((BigIntVector) vector);
        } else if (vector instanceof BitVector) {
            return new ArrowBooleanColumnVector((BitVector) vector);
        } else if (vector instanceof Float4Vector) {
            return new ArrowFloatColumnVector((Float4Vector) vector);
        } else if (vector instanceof Float8Vector) {
            return new ArrowDoubleColumnVector((Float8Vector) vector);
        } else if (vector instanceof VarCharVector) {
            return new ArrowVarCharColumnVector((VarCharVector) vector);
        } else if (vector instanceof FixedSizeBinaryVector) {
            return new ArrowBinaryColumnVector((FixedSizeBinaryVector) vector);
        } else if (vector instanceof VarBinaryVector) {
            return new ArrowVarBinaryColumnVector((VarBinaryVector) vector);
        } else if (vector instanceof DecimalVector) {
            return new ArrowDecimalColumnVector((DecimalVector) vector);
        } else if (vector instanceof DateDayVector) {
            return new ArrowDateColumnVector((DateDayVector) vector);
        } else if (vector instanceof TimeSecVector
                || vector instanceof TimeMilliVector
                || vector instanceof TimeMicroVector
                || vector instanceof TimeNanoVector) {
            return new ArrowTimeColumnVector(vector);
        } else if (vector instanceof TimeStampVector
                && ((ArrowType.Timestamp) vector.getField().getType()).getTimezone() == null) {
            if (dataType instanceof LocalZonedTimestampType) {
                return new ArrowTimestampLtzColumnVector(vector);
            } else {
                return new ArrowTimestampNtzColumnVector(vector);
            }
        } else if (vector instanceof ListVector && dataType instanceof ArrayType) {
            DataType elementType = ((ArrayType) dataType).getElementType();
            return new ArrowArrayColumnVector((ListVector) vector, elementType);
        } else if (vector instanceof MapVector && dataType instanceof MapType) {
            DataType keyType = ((MapType) dataType).getKeyType();
            DataType valueType = ((MapType) dataType).getValueType();
            return new ArrowMapColumnVector((MapVector) vector, keyType, valueType);
        } else if (dataType instanceof RowType) {
            return new ArrowRowColumnVector((FieldVector) vector, (RowType) dataType);
        } else {
            throw new UnsupportedOperationException(
                    String.format("Unsupported type %s.", dataType));
        }
    }

    public static Field toArrowField(String fieldName, DataType dataType, int depth) {
        FieldType fieldType =
                FieldType.nullable(dataType.accept(DataTypeToArrowTypeConverter.INSTANCE));
        fieldType =
                new FieldType(
                        fieldType.isNullable(),
                        fieldType.getType(),
                        fieldType.getDictionary(),
                        Collections.singletonMap(PARQUET_FIELD_NAME, fieldName));

        List<Field> children = null;
        if (dataType instanceof ArrayType) {
            Field elementField =
                    toArrowField(
                            ListVector.DATA_VECTOR_NAME,
                            ((ArrayType) dataType).getElementType(),
                            depth + 1);
            FieldType elementType = elementField.getFieldType();

            elementField =
                    new Field(
                            elementField.getName(),
                            new FieldType(
                                    elementType.isNullable(),
                                    elementType.getType(),
                                    elementType.getDictionary(),
                                    Collections.singletonMap(
                                            PARQUET_FIELD_NAME, fieldName + "_element")),
                            elementField.getChildren());
            children = Collections.singletonList(elementField);
        } else if (dataType instanceof MapType) {
            MapType mapType = (MapType) dataType;
            Field keyField = toArrowField(MapVector.KEY_NAME, mapType.getKeyType(), depth + 1);
            FieldType keyType = keyField.getFieldType();
            keyField =
                    new Field(
                            keyField.getName(),
                            new FieldType(
                                    false,
                                    keyType.getType(),
                                    keyType.getDictionary(),
                                    Collections.singletonMap(
                                            PARQUET_FIELD_NAME, fieldName + "_key")),
                            keyField.getChildren());

            Field valueField =
                    toArrowField(MapVector.VALUE_NAME, mapType.getValueType(), depth + 1);
            FieldType valueType = valueField.getFieldType();
            valueField =
                    new Field(
                            valueField.getName(),
                            new FieldType(
                                    valueType.isNullable(),
                                    valueType.getType(),
                                    valueType.getDictionary(),
                                    Collections.singletonMap(
                                            PARQUET_FIELD_NAME, fieldName + "_value")),
                            valueField.getChildren());

            FieldType structType =
                    new FieldType(
                            false,
                            Types.MinorType.STRUCT.getType(),
                            null,
                            Collections.singletonMap(PARQUET_FIELD_NAME, fieldName));
            Field mapField =
                    new Field(
                            MapVector.DATA_VECTOR_NAME,
                            // data vector, key vector and value vector CANNOT be null
                            structType,
                            Arrays.asList(keyField, valueField));

            children = Collections.singletonList(mapField);
        } else if (dataType instanceof RowType) {
            RowType rowType = (RowType) dataType;
            children = new ArrayList<>();
            for (DataField field : rowType.getFields()) {
                children.add(toArrowField(field.getName(), field.getType(), 0));
            }
        }

        return new Field(fieldName, fieldType, children);
    }

    private static class DataTypeToArrowTypeConverter extends DataTypeDefaultVisitor<ArrowType> {

        private static final DataTypeToArrowTypeConverter INSTANCE =
                new DataTypeToArrowTypeConverter();

        @Override
        public ArrowType visit(TinyIntType tinyIntType) {
            return new ArrowType.Int(8, true);
        }

        @Override
        public ArrowType visit(SmallIntType smallIntType) {
            return new ArrowType.Int(2 * 8, true);
        }

        @Override
        public ArrowType visit(IntType intType) {
            return new ArrowType.Int(4 * 8, true);
        }

        @Override
        public ArrowType visit(BigIntType bigIntType) {
            return new ArrowType.Int(8 * 8, true);
        }

        @Override
        public ArrowType visit(BooleanType booleanType) {
            return ArrowType.Bool.INSTANCE;
        }

        @Override
        public ArrowType visit(FloatType floatType) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        }

        @Override
        public ArrowType visit(DoubleType doubleType) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        }

        @Override
        public ArrowType visit(CharType varCharType) {
            return ArrowType.Utf8.INSTANCE;
        }

        @Override
        public ArrowType visit(StringType stringType) {
            return ArrowType.Utf8.INSTANCE;
        }

        @Override
        public ArrowType visit(BinaryType binaryType) {
            return new ArrowType.FixedSizeBinary(binaryType.getLength());
        }

        @Override
        public ArrowType visit(BytesType bytesType) {
            return ArrowType.Binary.INSTANCE;
        }

        @Override
        public ArrowType visit(DecimalType decimalType) {
            return new ArrowType.Decimal(decimalType.getPrecision(), decimalType.getScale());
        }

        @Override
        public ArrowType visit(DateType dateType) {
            return new ArrowType.Date(DateUnit.DAY);
        }

        @Override
        public ArrowType visit(TimeType timeType) {
            if (timeType.getPrecision() == 0) {
                return new ArrowType.Time(TimeUnit.SECOND, 32);
            } else if (timeType.getPrecision() >= 1 && timeType.getPrecision() <= 3) {
                return new ArrowType.Time(TimeUnit.MILLISECOND, 32);
            } else if (timeType.getPrecision() >= 4 && timeType.getPrecision() <= 6) {
                return new ArrowType.Time(TimeUnit.MICROSECOND, 64);
            } else {
                return new ArrowType.Time(TimeUnit.NANOSECOND, 64);
            }
        }

        @Override
        public ArrowType visit(LocalZonedTimestampType localZonedTimestampType) {
            if (localZonedTimestampType.getPrecision() == 0) {
                return new ArrowType.Timestamp(TimeUnit.SECOND, null);
            } else if (localZonedTimestampType.getPrecision() >= 1
                    && localZonedTimestampType.getPrecision() <= 3) {
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            } else if (localZonedTimestampType.getPrecision() >= 4
                    && localZonedTimestampType.getPrecision() <= 6) {
                return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
            } else {
                return new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
            }
        }

        @Override
        public ArrowType visit(TimestampType timestampType) {
            if (timestampType.getPrecision() == 0) {
                return new ArrowType.Timestamp(TimeUnit.SECOND, null);
            } else if (timestampType.getPrecision() >= 1 && timestampType.getPrecision() <= 3) {
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            } else if (timestampType.getPrecision() >= 4 && timestampType.getPrecision() <= 6) {
                return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
            } else {
                return new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
            }
        }

        @Override
        public ArrowType visit(ArrayType arrayType) {
            return Types.MinorType.LIST.getType();
        }

        @Override
        public ArrowType visit(MapType mapType) {
            return new ArrowType.Map(false);
        }

        @Override
        public ArrowType visit(RowType rowType) {
            return ArrowType.Struct.INSTANCE;
        }

        @Override
        public ArrowType visit(MultisetType multisetType) {
            throw new UnsupportedOperationException("Doesn't support MultisetType.");
        }

        @Override
        public ArrowType visit(VarCharType varCharType) {
            return Types.MinorType.VARCHAR.getType();
        }

        @Override
        public ArrowType visit(VarBinaryType varBinaryType) {
            return Types.MinorType.VARBINARY.getType();
        }

        @Override
        protected ArrowType defaultMethod(DataType dataType) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Unsupported data type %s currently.", dataType.asSummaryString()));
        }
    }

    private static int getPrecision(DecimalVector decimalVector) {
        int precision = -1;
        try {
            java.lang.reflect.Field precisionField =
                    decimalVector.getClass().getDeclaredField("precision");
            precisionField.setAccessible(true);
            precision = (int) precisionField.get(decimalVector);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            // should not happen, ignore
        }
        return precision;
    }
}
