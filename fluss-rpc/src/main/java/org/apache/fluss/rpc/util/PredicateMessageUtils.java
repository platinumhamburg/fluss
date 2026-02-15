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

package org.apache.fluss.rpc.util;

import org.apache.fluss.predicate.And;
import org.apache.fluss.predicate.CompoundPredicate;
import org.apache.fluss.predicate.Contains;
import org.apache.fluss.predicate.EndsWith;
import org.apache.fluss.predicate.Equal;
import org.apache.fluss.predicate.GreaterOrEqual;
import org.apache.fluss.predicate.GreaterThan;
import org.apache.fluss.predicate.In;
import org.apache.fluss.predicate.IsNotNull;
import org.apache.fluss.predicate.IsNull;
import org.apache.fluss.predicate.LeafFunction;
import org.apache.fluss.predicate.LeafPredicate;
import org.apache.fluss.predicate.LessOrEqual;
import org.apache.fluss.predicate.LessThan;
import org.apache.fluss.predicate.NotEqual;
import org.apache.fluss.predicate.NotIn;
import org.apache.fluss.predicate.Or;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateVisitor;
import org.apache.fluss.predicate.StartsWith;
import org.apache.fluss.record.Filter;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.rpc.messages.PbCompoundPredicate;
import org.apache.fluss.rpc.messages.PbDataType;
import org.apache.fluss.rpc.messages.PbFieldRef;
import org.apache.fluss.rpc.messages.PbFilter;
import org.apache.fluss.rpc.messages.PbLeafPredicate;
import org.apache.fluss.rpc.messages.PbLiteralValue;
import org.apache.fluss.rpc.messages.PbPredicate;
import org.apache.fluss.types.BigIntType;
import org.apache.fluss.types.BinaryType;
import org.apache.fluss.types.BooleanType;
import org.apache.fluss.types.BytesType;
import org.apache.fluss.types.CharType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.DateType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.DoubleType;
import org.apache.fluss.types.FloatType;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.SmallIntType;
import org.apache.fluss.types.StringType;
import org.apache.fluss.types.TimeType;
import org.apache.fluss.types.TimestampType;
import org.apache.fluss.types.TinyIntType;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Utils for converting Predicate to PbPredicate and vice versa. */
public class PredicateMessageUtils {

    // -------------------------------------------------------------------------
    //  Deserialization: PbPredicate -> Predicate
    // -------------------------------------------------------------------------

    public static Predicate toPredicate(PbPredicate pbPredicate) {
        PredicateType type = PredicateType.fromValue(pbPredicate.getType());
        switch (type) {
            case LEAF:
                return toLeafPredicate(pbPredicate.getLeaf());
            case COMPOUND:
                return toCompoundPredicate(pbPredicate.getCompound());
            default:
                throw new IllegalArgumentException("Unknown predicate type: " + type);
        }
    }

    public static CompoundPredicate toCompoundPredicate(PbCompoundPredicate pbCompound) {
        List<Predicate> children =
                pbCompound.getChildrensList().stream()
                        .map(PredicateMessageUtils::toPredicate)
                        .collect(Collectors.toList());
        return new CompoundPredicate(
                CompoundFunctionCode.fromValue(pbCompound.getFunction()).getFunction(), children);
    }

    private static LeafPredicate toLeafPredicate(PbLeafPredicate pbLeaf) {
        PbFieldRef fieldRef = pbLeaf.getFieldRef();
        List<Object> literals =
                pbLeaf.getLiteralsList().stream()
                        .map(PredicateMessageUtils::toLiteralValue)
                        .collect(Collectors.toList());

        return new LeafPredicate(
                LeafFunctionCode.fromValue(pbLeaf.getFunction()).getFunction(),
                toDataType(fieldRef.getDataType()),
                fieldRef.getFieldIndex(),
                fieldRef.getFieldName(),
                literals);
    }

    private static DataType toDataType(PbDataType pbType) {
        DataTypeRoot root = DataTypeRootCode.fromValue(pbType.getRoot()).getDataTypeRoot();
        boolean nullable = pbType.isNullable();
        switch (root) {
            case BOOLEAN:
                return new BooleanType(nullable);
            case TINYINT:
                return new TinyIntType(nullable);
            case SMALLINT:
                return new SmallIntType(nullable);
            case INTEGER:
                return new IntType(nullable);
            case BIGINT:
                return new BigIntType(nullable);
            case FLOAT:
                return new FloatType(nullable);
            case DOUBLE:
                return new DoubleType(nullable);
            case CHAR:
                return new CharType(nullable, pbType.getLength());
            case STRING:
                return new StringType(nullable);
            case DECIMAL:
                return new DecimalType(nullable, pbType.getPrecision(), pbType.getScale());
            case DATE:
                return new DateType(nullable);
            case TIME_WITHOUT_TIME_ZONE:
                return new TimeType(nullable, pbType.getPrecision());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return new TimestampType(nullable, pbType.getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new LocalZonedTimestampType(nullable, pbType.getPrecision());
            case BINARY:
                return new BinaryType(pbType.getLength());
            case BYTES:
                return new BytesType(nullable);
            default:
                throw new IllegalArgumentException("Unknown data type root: " + root);
        }
    }

    private static Object toLiteralValue(PbLiteralValue pbLiteral) {
        if (pbLiteral.isIsNull()) {
            return null;
        }
        DataTypeRoot root =
                DataTypeRootCode.fromValue(pbLiteral.getType().getRoot()).getDataTypeRoot();
        switch (root) {
            case BOOLEAN:
                return pbLiteral.isBooleanValue();
            case TINYINT:
                return (byte) pbLiteral.getIntValue();
            case SMALLINT:
                return (short) pbLiteral.getIntValue();
            case INTEGER:
                return pbLiteral.getIntValue();
            case BIGINT:
                return pbLiteral.getBigintValue();
            case FLOAT:
                return pbLiteral.getFloatValue();
            case DOUBLE:
                return pbLiteral.getDoubleValue();
            case CHAR:
            case STRING:
                String stringValue = pbLiteral.getStringValue();
                return stringValue == null ? null : BinaryString.fromString(stringValue);
            case DECIMAL:
                if (pbLiteral.hasDecimalBytes()) {
                    return Decimal.fromUnscaledBytes(
                            pbLiteral.getDecimalBytes(),
                            pbLiteral.getType().getPrecision(),
                            pbLiteral.getType().getScale());
                } else {
                    return Decimal.fromUnscaledLong(
                            pbLiteral.getDecimalValue(),
                            pbLiteral.getType().getPrecision(),
                            pbLiteral.getType().getScale());
                }
            case DATE:
                return LocalDate.ofEpochDay(pbLiteral.getBigintValue());
            case TIME_WITHOUT_TIME_ZONE:
                return LocalTime.ofNanoOfDay(pbLiteral.getIntValue() * 1_000_000L);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return TimestampNtz.fromMillis(
                        pbLiteral.getTimestampMillisValue(),
                        pbLiteral.getTimestampNanoOfMillisValue());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TimestampLtz.fromEpochMillis(
                        pbLiteral.getTimestampMillisValue(),
                        pbLiteral.getTimestampNanoOfMillisValue());
            case BINARY:
            case BYTES:
                return pbLiteral.getBinaryValue();
            default:
                throw new IllegalArgumentException("Unknown literal value type: " + root);
        }
    }

    // -------------------------------------------------------------------------
    //  Serialization: Predicate -> PbPredicate
    // -------------------------------------------------------------------------

    public static PbPredicate toPbPredicate(Predicate predicate) {
        return predicate.visit(
                new PredicateVisitor<PbPredicate>() {
                    @Override
                    public PbPredicate visit(LeafPredicate predicate) {
                        PbFieldRef fieldRef = new PbFieldRef();
                        fieldRef.setDataType(toPbDataType(predicate.type()));
                        fieldRef.setFieldIndex(predicate.index());
                        fieldRef.setFieldName(predicate.fieldName());

                        PbLeafPredicate pbLeaf = new PbLeafPredicate();
                        pbLeaf.setFunction(
                                LeafFunctionCode.fromFunction(predicate.function()).getValue());
                        pbLeaf.setFieldRef(fieldRef);

                        List<PbLiteralValue> literals = new ArrayList<>();
                        for (Object literal : predicate.literals()) {
                            literals.add(toPbLiteralValue(predicate.type(), literal));
                        }
                        pbLeaf.addAllLiterals(literals);

                        PbPredicate pbPredicate = new PbPredicate();
                        pbPredicate.setType(PredicateType.LEAF.getValue());
                        pbPredicate.setLeaf(pbLeaf);
                        return pbPredicate;
                    }

                    @Override
                    public PbPredicate visit(CompoundPredicate predicate) {
                        PbCompoundPredicate pbCompound = new PbCompoundPredicate();
                        pbCompound.setFunction(
                                CompoundFunctionCode.fromFunction(predicate.function()).getValue());
                        pbCompound.addAllChildrens(
                                predicate.children().stream()
                                        .map(PredicateMessageUtils::toPbPredicate)
                                        .collect(Collectors.toList()));

                        PbPredicate pbPredicate = new PbPredicate();
                        pbPredicate.setType(PredicateType.COMPOUND.getValue());
                        pbPredicate.setCompound(pbCompound);
                        return pbPredicate;
                    }
                });
    }

    private static PbDataType toPbDataType(DataType dataType) {
        PbDataType pbDataType = new PbDataType();
        pbDataType.setNullable(dataType.isNullable());
        pbDataType.setRoot(DataTypeRootCode.fromDataTypeRoot(dataType.getTypeRoot()).getValue());

        // Set type-specific parameters
        if (dataType instanceof CharType) {
            pbDataType.setLength(((CharType) dataType).getLength());
        } else if (dataType instanceof DecimalType) {
            pbDataType.setPrecision(((DecimalType) dataType).getPrecision());
            pbDataType.setScale(((DecimalType) dataType).getScale());
        } else if (dataType instanceof TimeType) {
            pbDataType.setPrecision(((TimeType) dataType).getPrecision());
        } else if (dataType instanceof TimestampType) {
            pbDataType.setPrecision(((TimestampType) dataType).getPrecision());
        } else if (dataType instanceof LocalZonedTimestampType) {
            pbDataType.setPrecision(((LocalZonedTimestampType) dataType).getPrecision());
        } else if (dataType instanceof BinaryType) {
            pbDataType.setLength(((BinaryType) dataType).getLength());
        }
        return pbDataType;
    }

    private static PbLiteralValue toPbLiteralValue(DataType type, Object literal) {
        PbLiteralValue pbLiteral = new PbLiteralValue();
        pbLiteral.setType(toPbDataType(type));
        if (literal == null) {
            pbLiteral.setIsNull(true);
            return pbLiteral;
        }
        pbLiteral.setIsNull(false);
        switch (type.getTypeRoot()) {
            case CHAR:
            case STRING:
                pbLiteral.setStringValue(literal.toString());
                break;
            case BOOLEAN:
                pbLiteral.setBooleanValue((Boolean) literal);
                break;
            case BINARY:
            case BYTES:
                pbLiteral.setBinaryValue((byte[]) literal);
                break;
            case DECIMAL:
                Decimal decimal = (Decimal) literal;
                if (decimal.isCompact()) {
                    pbLiteral.setDecimalValue(decimal.toUnscaledLong());
                } else {
                    pbLiteral.setDecimalBytes(decimal.toUnscaledBytes());
                }
                break;
            case TINYINT:
                pbLiteral.setIntValue((Byte) literal);
                break;
            case SMALLINT:
                pbLiteral.setIntValue((Short) literal);
                break;
            case INTEGER:
                pbLiteral.setIntValue((Integer) literal);
                break;
            case DATE:
                pbLiteral.setBigintValue(((LocalDate) literal).toEpochDay());
                break;
            case TIME_WITHOUT_TIME_ZONE:
                pbLiteral.setIntValue((int) (((LocalTime) literal).toNanoOfDay() / 1_000_000L));
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                pbLiteral.setTimestampMillisValue(((TimestampNtz) literal).getMillisecond());
                pbLiteral.setTimestampNanoOfMillisValue(
                        ((TimestampNtz) literal).getNanoOfMillisecond());
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                pbLiteral.setTimestampMillisValue(((TimestampLtz) literal).getEpochMillisecond());
                pbLiteral.setTimestampNanoOfMillisValue(
                        ((TimestampLtz) literal).getNanoOfMillisecond());
                break;
            case BIGINT:
                pbLiteral.setBigintValue((Long) literal);
                break;
            case FLOAT:
                pbLiteral.setFloatValue((Float) literal);
                break;
            case DOUBLE:
                pbLiteral.setDoubleValue((Double) literal);
                break;
            default:
                throw new IllegalArgumentException("Unknown data type: " + type.getTypeRoot());
        }
        return pbLiteral;
    }

    // -------------------------------------------------------------------------
    //  Filter conversion
    // -------------------------------------------------------------------------

    public static PbFilter toPbFilter(Predicate predicate, int schemaId) {
        PbFilter pbFilter = new PbFilter();
        pbFilter.setPredicate(toPbPredicate(predicate));
        pbFilter.setSchemaId(schemaId);
        return pbFilter;
    }

    public static Filter toFilter(PbFilter pbFilter) {
        Predicate predicate = toPredicate(pbFilter.getPredicate());
        int schemaId = pbFilter.getSchemaId();
        return new Filter(predicate, schemaId);
    }

    // -------------------------------------------------------------------------
    //  Proto int32 <-> domain object mapping enums
    // -------------------------------------------------------------------------

    /** Maps PbPredicate.type int32 values to predicate kinds. */
    private enum PredicateType {
        LEAF(0),
        COMPOUND(1);

        private final int value;
        private static final Map<Integer, PredicateType> VALUE_MAP = new HashMap<>();

        static {
            for (PredicateType t : values()) {
                VALUE_MAP.put(t.value, t);
            }
        }

        PredicateType(int value) {
            this.value = value;
        }

        int getValue() {
            return value;
        }

        static PredicateType fromValue(int value) {
            PredicateType t = VALUE_MAP.get(value);
            if (t == null) {
                throw new IllegalArgumentException("Unknown predicate type: " + value);
            }
            return t;
        }
    }

    /** Maps PbLeafPredicate.function int32 values to {@link LeafFunction} instances. */
    private enum LeafFunctionCode {
        EQUAL(0, Equal.INSTANCE),
        NOT_EQUAL(1, NotEqual.INSTANCE),
        LESS_THAN(2, LessThan.INSTANCE),
        LESS_OR_EQUAL(3, LessOrEqual.INSTANCE),
        GREATER_THAN(4, GreaterThan.INSTANCE),
        GREATER_OR_EQUAL(5, GreaterOrEqual.INSTANCE),
        IS_NULL(6, IsNull.INSTANCE),
        IS_NOT_NULL(7, IsNotNull.INSTANCE),
        STARTS_WITH(8, StartsWith.INSTANCE),
        CONTAINS(9, Contains.INSTANCE),
        END_WITH(10, EndsWith.INSTANCE),
        IN(11, In.INSTANCE),
        NOT_IN(12, NotIn.INSTANCE);

        private final int value;
        private final LeafFunction function;
        private static final Map<Integer, LeafFunctionCode> VALUE_MAP = new HashMap<>();
        private static final Map<Class<? extends LeafFunction>, LeafFunctionCode> FUNCTION_MAP =
                new HashMap<>();

        static {
            for (LeafFunctionCode c : values()) {
                VALUE_MAP.put(c.value, c);
                FUNCTION_MAP.put(c.function.getClass(), c);
            }
        }

        LeafFunctionCode(int value, LeafFunction function) {
            this.value = value;
            this.function = function;
        }

        int getValue() {
            return value;
        }

        LeafFunction getFunction() {
            return function;
        }

        static LeafFunctionCode fromValue(int value) {
            LeafFunctionCode c = VALUE_MAP.get(value);
            if (c == null) {
                throw new IllegalArgumentException("Unknown leaf function: " + value);
            }
            return c;
        }

        static LeafFunctionCode fromFunction(LeafFunction function) {
            LeafFunctionCode c = FUNCTION_MAP.get(function.getClass());
            if (c == null) {
                throw new IllegalArgumentException("Unknown leaf function: " + function);
            }
            return c;
        }
    }

    /** Maps PbCompoundPredicate.function int32 values to {@link CompoundPredicate.Function}. */
    private enum CompoundFunctionCode {
        AND(0, And.INSTANCE),
        OR(1, Or.INSTANCE);

        private final int value;
        private final CompoundPredicate.Function function;
        private static final Map<Integer, CompoundFunctionCode> VALUE_MAP = new HashMap<>();
        private static final Map<Class<? extends CompoundPredicate.Function>, CompoundFunctionCode>
                FUNCTION_MAP = new HashMap<>();

        static {
            for (CompoundFunctionCode c : values()) {
                VALUE_MAP.put(c.value, c);
                FUNCTION_MAP.put(c.function.getClass(), c);
            }
        }

        CompoundFunctionCode(int value, CompoundPredicate.Function function) {
            this.value = value;
            this.function = function;
        }

        int getValue() {
            return value;
        }

        CompoundPredicate.Function getFunction() {
            return function;
        }

        static CompoundFunctionCode fromValue(int value) {
            CompoundFunctionCode c = VALUE_MAP.get(value);
            if (c == null) {
                throw new IllegalArgumentException("Unknown compound function: " + value);
            }
            return c;
        }

        static CompoundFunctionCode fromFunction(CompoundPredicate.Function function) {
            CompoundFunctionCode c = FUNCTION_MAP.get(function.getClass());
            if (c == null) {
                throw new IllegalArgumentException("Unknown compound function: " + function);
            }
            return c;
        }
    }

    /**
     * Maps PbDataType.root int32 values to {@link DataTypeRoot}.
     *
     * <p>Note: proto uses INT/VARCHAR while the domain model uses INTEGER/STRING.
     */
    private enum DataTypeRootCode {
        BOOLEAN(0, DataTypeRoot.BOOLEAN),
        TINYINT(1, DataTypeRoot.TINYINT),
        SMALLINT(2, DataTypeRoot.SMALLINT),
        INT(3, DataTypeRoot.INTEGER),
        BIGINT(4, DataTypeRoot.BIGINT),
        FLOAT(5, DataTypeRoot.FLOAT),
        DOUBLE(6, DataTypeRoot.DOUBLE),
        CHAR(7, DataTypeRoot.CHAR),
        VARCHAR(8, DataTypeRoot.STRING),
        DECIMAL(9, DataTypeRoot.DECIMAL),
        DATE(10, DataTypeRoot.DATE),
        TIME_WITHOUT_TIME_ZONE(11, DataTypeRoot.TIME_WITHOUT_TIME_ZONE),
        TIMESTAMP_WITHOUT_TIME_ZONE(12, DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE),
        TIMESTAMP_WITH_LOCAL_TIME_ZONE(13, DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE),
        BINARY(14, DataTypeRoot.BINARY),
        BYTES(15, DataTypeRoot.BYTES);

        private final int value;
        private final DataTypeRoot dataTypeRoot;
        private static final Map<Integer, DataTypeRootCode> VALUE_MAP = new HashMap<>();
        private static final Map<DataTypeRoot, DataTypeRootCode> ROOT_MAP = new HashMap<>();

        static {
            for (DataTypeRootCode c : values()) {
                VALUE_MAP.put(c.value, c);
                ROOT_MAP.put(c.dataTypeRoot, c);
            }
        }

        DataTypeRootCode(int value, DataTypeRoot dataTypeRoot) {
            this.value = value;
            this.dataTypeRoot = dataTypeRoot;
        }

        int getValue() {
            return value;
        }

        DataTypeRoot getDataTypeRoot() {
            return dataTypeRoot;
        }

        static DataTypeRootCode fromValue(int value) {
            DataTypeRootCode c = VALUE_MAP.get(value);
            if (c == null) {
                throw new IllegalArgumentException("Unknown data type root: " + value);
            }
            return c;
        }

        static DataTypeRootCode fromDataTypeRoot(DataTypeRoot root) {
            DataTypeRootCode c = ROOT_MAP.get(root);
            if (c == null) {
                throw new IllegalArgumentException("Unknown data type root: " + root);
            }
            return c;
        }
    }
}
