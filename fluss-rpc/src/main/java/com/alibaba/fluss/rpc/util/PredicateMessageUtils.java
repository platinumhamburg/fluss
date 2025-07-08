package com.alibaba.fluss.rpc.util;

import com.alibaba.fluss.predicate.And;
import com.alibaba.fluss.predicate.CompoundPredicate;
import com.alibaba.fluss.predicate.Contains;
import com.alibaba.fluss.predicate.EndsWith;
import com.alibaba.fluss.predicate.Equal;
import com.alibaba.fluss.predicate.GreaterOrEqual;
import com.alibaba.fluss.predicate.GreaterThan;
import com.alibaba.fluss.predicate.In;
import com.alibaba.fluss.predicate.IsNotNull;
import com.alibaba.fluss.predicate.IsNull;
import com.alibaba.fluss.predicate.LeafFunction;
import com.alibaba.fluss.predicate.LeafPredicate;
import com.alibaba.fluss.predicate.LessOrEqual;
import com.alibaba.fluss.predicate.LessThan;
import com.alibaba.fluss.predicate.NotEqual;
import com.alibaba.fluss.predicate.NotIn;
import com.alibaba.fluss.predicate.Or;
import com.alibaba.fluss.predicate.Predicate;
import com.alibaba.fluss.predicate.PredicateVisitor;
import com.alibaba.fluss.predicate.StartsWith;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.rpc.messages.predicate.PbCompoundFunction;
import com.alibaba.fluss.rpc.messages.predicate.PbCompoundPredicate;
import com.alibaba.fluss.rpc.messages.predicate.PbDataType;
import com.alibaba.fluss.rpc.messages.predicate.PbDataTypeRoot;
import com.alibaba.fluss.rpc.messages.predicate.PbFieldRef;
import com.alibaba.fluss.rpc.messages.predicate.PbLeafFunction;
import com.alibaba.fluss.rpc.messages.predicate.PbLeafPredicate;
import com.alibaba.fluss.rpc.messages.predicate.PbLiteralValue;
import com.alibaba.fluss.rpc.messages.predicate.PbPredicate;
import com.alibaba.fluss.rpc.messages.predicate.PbPredicateType;
import com.alibaba.fluss.types.BigIntType;
import com.alibaba.fluss.types.BinaryType;
import com.alibaba.fluss.types.BooleanType;
import com.alibaba.fluss.types.BytesType;
import com.alibaba.fluss.types.CharType;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DateType;
import com.alibaba.fluss.types.DecimalType;
import com.alibaba.fluss.types.DoubleType;
import com.alibaba.fluss.types.FloatType;
import com.alibaba.fluss.types.IntType;
import com.alibaba.fluss.types.LocalZonedTimestampType;
import com.alibaba.fluss.types.StringType;
import com.alibaba.fluss.types.TimeType;
import com.alibaba.fluss.types.TimestampType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.fluss.rpc.messages.predicate.PbDataTypeRoot.BOOLEAN;

/** Utils for converting Predicate to PbPredicate and vice versa. */
public class PredicateMessageUtils {
    public static Predicate toPredicate(PbPredicate pbPredicate) {
        switch (pbPredicate.getType()) {
            case LEAF:
                return toLeafPredicate(pbPredicate.getLeaf());
            case COMPOUND:
                return toCompoundPredicate(pbPredicate.getCompound());
            default:
                throw new IllegalArgumentException("Unknown predicate type in PbPredicate");
        }
    }

    public static CompoundPredicate toCompoundPredicate(PbCompoundPredicate pbCompound) {
        List<Predicate> children =
                pbCompound.getChildrensList().stream()
                        .map(PredicateMessageUtils::toPredicate)
                        .collect(Collectors.toList());
        return new CompoundPredicate(toCompoundFunction(pbCompound.getFunction()), children);
    }

    private static CompoundPredicate.Function toCompoundFunction(PbCompoundFunction function) {
        switch (function) {
            case AND:
                return And.INSTANCE;
            case OR:
                return Or.INSTANCE;
            default:
                throw new IllegalArgumentException("Unknown compound function: " + function);
        }
    }

    private static LeafPredicate toLeafPredicate(PbLeafPredicate pbLeaf) {
        PbFieldRef fieldRef = pbLeaf.getFieldRef();
        List<Object> literals =
                pbLeaf.getLiteralsList().stream()
                        .map(PredicateMessageUtils::toLiteralValue)
                        .collect(Collectors.toList());

        return new LeafPredicate(
                toLeafFunction(pbLeaf.getFunction()),
                toDataType(fieldRef.getDataType()),
                fieldRef.getFieldIndex(),
                fieldRef.getFieldName(),
                literals);
    }

    private static DataType toDataType(PbDataType type) {
        switch (type.getRoot()) {
            case BOOLEAN:
                return new BooleanType(type.isNullable());
            case INT:
                return new IntType(type.isNullable());
            case BIGINT:
                return new BigIntType(type.isNullable());
            case FLOAT:
                return new FloatType(type.isNullable());
            case DOUBLE:
                return new DoubleType(type.isNullable());
            case CHAR:
                return new CharType(type.isNullable(), type.getLength());
            case VARCHAR:
                return new StringType(type.isNullable());
            case DECIMAL:
                return new DecimalType(type.isNullable(), type.getPrecision(), type.getScale());
            case DATE:
                return new DateType(type.isNullable());
            case TIME:
                return new TimeType(type.isNullable(), type.getPrecision());
            case TIMESTAMP:
                return new TimestampType(type.isNullable(), type.getPrecision());
            case TIMESTAMP_LTZ:
                return new LocalZonedTimestampType(type.isNullable(), type.getPrecision());
            case BINARY:
                return new BinaryType(type.getLength());
            case BYTES:
                return new BytesType(type.isNullable());
            default:
                throw new IllegalArgumentException("Unknown data type in PbDataType");
        }
    }

    private static Object toLiteralValue(PbLiteralValue pbLiteral) {
        switch (pbLiteral.getType().getRoot()) {
            case BOOLEAN:
                return pbLiteral.isBooleanValue();
            case INT:
                return pbLiteral.getIntValue();
            case BIGINT:
                return pbLiteral.getBigintValue();
            case FLOAT:
                return pbLiteral.getFloatValue();
            case DOUBLE:
                return pbLiteral.getDoubleValue();
            case CHAR:
            case VARCHAR:
                return pbLiteral.getStringValue();
            case DECIMAL:
                return new BigDecimal(Arrays.toString(pbLiteral.getDecimalValue()));
            case DATE:
                return pbLiteral.getDateValue();
            case TIME:
                return pbLiteral.getTimeValue();
            case TIMESTAMP:
                return pbLiteral.getTimestampValue();
            case BINARY:
                return pbLiteral.getBinaryValue();
            default:
                throw new IllegalArgumentException("Unknown literal value in PbLiteralValue");
        }
    }

    private static LeafFunction toLeafFunction(PbLeafFunction function) {
        switch (function) {
            case EQUAL:
                return Equal.INSTANCE;
            case NOT_EQUAL:
                return NotEqual.INSTANCE;
            case LESS_THAN:
                return LessThan.INSTANCE;
            case LESS_OR_EQUAL:
                return LessOrEqual.INSTANCE;
            case GREATER_THAN:
                return GreaterThan.INSTANCE;
            case GREATER_OR_EQUAL:
                return GreaterOrEqual.INSTANCE;
            case IS_NULL:
                return IsNull.INSTANCE;
            case IS_NOT_NULL:
                return IsNotNull.INSTANCE;
            case STARTS_WITH:
                return StartsWith.INSTANCE;
            case CONTAINS:
                return Contains.INSTANCE;
            case END_WITH:
                return EndsWith.INSTANCE;
            case IN:
                return In.INSTANCE;
            case NOT_IN:
                return NotIn.INSTANCE;
            default:
                throw new IllegalArgumentException("Unknown leaf function: " + function);
        }
    }

    public static PbPredicate toPbPredicate(Predicate predicate) {

        return predicate.visit(
                new PredicateVisitor<PbPredicate>() {
                    @Override
                    public PbPredicate visit(LeafPredicate predicate) {
                        PbLeafPredicate pbLeafPredicate = new PbLeafPredicate();
                        PbFieldRef fieldRef = new PbFieldRef();
                        fieldRef.setDataType(toPbDataType(predicate.type()));
                        fieldRef.setFieldIndex(predicate.index());
                        fieldRef.setFieldName(predicate.fieldName());
                        pbLeafPredicate.setFunction(toPbLeafFunction(predicate.function()));
                        pbLeafPredicate.setFieldRef(fieldRef);

                        List<PbLiteralValue> literals = new ArrayList<>();
                        for (Object literal : predicate.literals()) {
                            literals.add(toPbLiteralValue(predicate.type(), literal));
                        }
                        pbLeafPredicate.addAllLiterals(literals);

                        PbPredicate pbPredicate = new PbPredicate();
                        pbPredicate.setLeaf(pbLeafPredicate);
                        pbPredicate.setType(PbPredicateType.LEAF);
                        return pbPredicate;
                    }

                    @Override
                    public PbPredicate visit(CompoundPredicate predicate) {
                        PbCompoundPredicate pbCompoundPredicate = new PbCompoundPredicate();
                        pbCompoundPredicate.setFunction(toPbCompoundFunction(predicate.function()));
                        pbCompoundPredicate.addAllChildrens(
                                predicate.children().stream()
                                        .map(PredicateMessageUtils::toPbPredicate)
                                        .collect(Collectors.toList()));
                        PbPredicate pbPredicate = new PbPredicate();
                        pbPredicate.setCompound(pbCompoundPredicate);
                        pbPredicate.setType(PbPredicateType.COMPOUND);
                        return pbPredicate;
                    }
                });
    }

    private static PbDataType toPbDataType(DataType dataType) {
        PbDataType pbDataType = new PbDataType();
        pbDataType.setNullable(dataType.isNullable());
        if (dataType instanceof BooleanType) {
            pbDataType.setRoot(BOOLEAN);
        } else if (dataType instanceof IntType) {
            pbDataType.setRoot(PbDataTypeRoot.INT);
        } else if (dataType instanceof BigIntType) {
            pbDataType.setRoot(PbDataTypeRoot.BIGINT);
        } else if (dataType instanceof FloatType) {
            pbDataType.setRoot(PbDataTypeRoot.FLOAT);
        } else if (dataType instanceof DoubleType) {
            pbDataType.setRoot(PbDataTypeRoot.DOUBLE);
        } else if (dataType instanceof CharType) {
            pbDataType.setRoot(PbDataTypeRoot.CHAR);
            pbDataType.setLength(((CharType) dataType).getLength());
        } else if (dataType instanceof StringType) {
            pbDataType.setRoot(PbDataTypeRoot.VARCHAR);
        } else if (dataType instanceof DecimalType) {
            pbDataType.setRoot(PbDataTypeRoot.DECIMAL);
            pbDataType.setPrecision(((DecimalType) dataType).getPrecision());
            pbDataType.setScale(((DecimalType) dataType).getScale());
        } else if (dataType instanceof DateType) {
            pbDataType.setRoot(PbDataTypeRoot.DATE);
        } else if (dataType instanceof TimeType) {
            pbDataType.setRoot(PbDataTypeRoot.TIME);
            pbDataType.setPrecision(((TimeType) dataType).getPrecision());
        } else if (dataType instanceof TimestampType) {
            pbDataType.setRoot(PbDataTypeRoot.TIMESTAMP);
            pbDataType.setPrecision(((TimestampType) dataType).getPrecision());
        } else if (dataType instanceof LocalZonedTimestampType) {
            pbDataType.setRoot(PbDataTypeRoot.TIMESTAMP_LTZ);
            pbDataType.setPrecision(((LocalZonedTimestampType) dataType).getPrecision());
        } else if (dataType instanceof BinaryType) {
            pbDataType.setRoot(PbDataTypeRoot.BINARY);
            pbDataType.setLength(((BinaryType) dataType).getLength());
        } else if (dataType instanceof BytesType) {
            pbDataType.setRoot(PbDataTypeRoot.BYTES);
        } else {
            throw new IllegalArgumentException("Unknown data type: " + dataType.getClass());
        }
        return pbDataType;
    }

    private static PbLiteralValue toPbLiteralValue(DataType type, Object literal) {
        PbLiteralValue pbLiteralValue = new PbLiteralValue();
        pbLiteralValue.setType(toPbDataType(type));
        // ordered by type root definition
        switch (type.getTypeRoot()) {
            case CHAR:
            case STRING:
                pbLiteralValue.setStringValue(literal.toString());
                break;
            case BOOLEAN:
                pbLiteralValue.setBooleanValue((Boolean) literal);
                break;
            case BINARY:
            case BYTES:
                pbLiteralValue.setBinaryValue((byte[]) literal);
                break;
            case DECIMAL:
                pbLiteralValue.setDecimalValue(((Decimal) literal).toUnscaledBytes());
                break;
            case TINYINT:
                pbLiteralValue.setIntValue((Byte) literal);
                break;
            case SMALLINT:
                pbLiteralValue.setIntValue((Short) literal);
                break;
            case INTEGER:
                pbLiteralValue.setIntValue((Integer) literal);
                break;
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                pbLiteralValue.setIntValue((Integer) literal);
                break;
            case BIGINT:
                pbLiteralValue.setBigintValue((Long) literal);
                break;
            case FLOAT:
                pbLiteralValue.setFloatValue((Float) literal);
                break;
            case DOUBLE:
                pbLiteralValue.setDoubleValue((Double) literal);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                pbLiteralValue.setTimestampValue(((TimestampNtz) literal).getMillisecond());
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                pbLiteralValue.setTimestampValue(((TimestampLtz) literal).getEpochMillisecond());
                break;
            default:
                throw new IllegalArgumentException("Unknown data type: " + type.getTypeRoot());
        }
        return pbLiteralValue;
    }

    private static PbLeafFunction toPbLeafFunction(LeafFunction function) {
        if (function == Equal.INSTANCE) {
            return PbLeafFunction.EQUAL;
        }
        if (function == NotEqual.INSTANCE) {
            return PbLeafFunction.NOT_EQUAL;
        }
        if (function == LessThan.INSTANCE) {
            return PbLeafFunction.LESS_THAN;
        }
        if (function == LessOrEqual.INSTANCE) {
            return PbLeafFunction.LESS_OR_EQUAL;
        }
        if (function == GreaterThan.INSTANCE) {
            return PbLeafFunction.GREATER_THAN;
        }
        if (function == GreaterOrEqual.INSTANCE) {
            return PbLeafFunction.GREATER_OR_EQUAL;
        }
        if (function == IsNull.INSTANCE) {
            return PbLeafFunction.IS_NULL;
        }
        if (function == IsNotNull.INSTANCE) {
            return PbLeafFunction.IS_NOT_NULL;
        }
        if (function == StartsWith.INSTANCE) {
            return PbLeafFunction.STARTS_WITH;
        }
        if (function == Contains.INSTANCE) {
            return PbLeafFunction.CONTAINS;
        }
        if (function == EndsWith.INSTANCE) {
            return PbLeafFunction.END_WITH;
        }
        if (function == In.INSTANCE) {
            return PbLeafFunction.IN;
        }
        if (function == NotIn.INSTANCE) {
            return PbLeafFunction.NOT_IN;
        }
        throw new IllegalArgumentException("Unknown leaf function: " + function);
    }

    private static PbCompoundFunction toPbCompoundFunction(
            CompoundPredicate.Function compoundFunction) {
        if (compoundFunction == And.INSTANCE) {
            return PbCompoundFunction.AND;
        }
        if (compoundFunction == Or.INSTANCE) {
            return PbCompoundFunction.OR;
        }
        throw new IllegalArgumentException("Unknown compound function: " + compoundFunction);
    }
}
