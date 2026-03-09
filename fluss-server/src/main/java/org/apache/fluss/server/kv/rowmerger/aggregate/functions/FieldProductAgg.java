/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.kv.rowmerger.aggregate.functions;

/* This file is based on source code of Apache Paimon Project (https://paimon.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

import org.apache.fluss.row.Decimal;
import org.apache.fluss.types.DataType;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static org.apache.fluss.row.Decimal.fromBigDecimal;
import static org.apache.fluss.server.kv.rowmerger.aggregate.functions.NarrowMathUtils.checkDecimalConsistency;
import static org.apache.fluss.server.kv.rowmerger.aggregate.functions.NarrowMathUtils.multiplyExactByte;
import static org.apache.fluss.server.kv.rowmerger.aggregate.functions.NarrowMathUtils.multiplyExactShort;

/** Product aggregator - computes the product of numeric values. */
public class FieldProductAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    public FieldProductAgg(DataType dataType) {
        super(dataType);
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        if (accumulator == null || inputField == null) {
            return accumulator == null ? inputField : accumulator;
        }

        Object product;

        // ordered by type root definition
        switch (typeRoot) {
            case DECIMAL:
                Decimal mergeFieldDD = (Decimal) accumulator;
                Decimal inFieldDD = (Decimal) inputField;
                checkDecimalConsistency(mergeFieldDD, inFieldDD);
                BigDecimal bigDecimal = mergeFieldDD.toBigDecimal();
                BigDecimal bigDecimal1 = inFieldDD.toBigDecimal();
                BigDecimal mul = bigDecimal.multiply(bigDecimal1);
                product = fromBigDecimal(mul, mergeFieldDD.precision(), mergeFieldDD.scale());
                break;
            case TINYINT:
                product = multiplyExactByte((byte) accumulator, (byte) inputField);
                break;
            case SMALLINT:
                product = multiplyExactShort((short) accumulator, (short) inputField);
                break;
            case INTEGER:
                product = Math.multiplyExact((int) accumulator, (int) inputField);
                break;
            case BIGINT:
                product = Math.multiplyExact((long) accumulator, (long) inputField);
                break;
            case FLOAT:
                product = (float) accumulator * (float) inputField;
                break;
            case DOUBLE:
                product = (double) accumulator * (double) inputField;
                break;
            default:
                String msg =
                        String.format(
                                "type %s not support in %s", typeRoot, this.getClass().getName());
                throw new IllegalArgumentException(msg);
        }
        return product;
    }

    @Override
    public Object retract(Object accumulator, Object retractField) {
        if (accumulator == null) {
            return null;
        }
        if (retractField == null) {
            return accumulator;
        }

        Object result;
        switch (typeRoot) {
            case DECIMAL:
                Decimal accDD = (Decimal) accumulator;
                Decimal retDD = (Decimal) retractField;
                checkDecimalConsistency(accDD, retDD);
                BigDecimal accBd = accDD.toBigDecimal();
                BigDecimal retBd = retDD.toBigDecimal();
                checkDivisorNotZero(retBd);
                BigDecimal div = accBd.divide(retBd, accDD.scale(), RoundingMode.HALF_EVEN);
                result = fromBigDecimal(div, accDD.precision(), accDD.scale());
                break;
            case TINYINT:
                checkDivisorNotZero((byte) retractField);
                result = (byte) ((byte) accumulator / (byte) retractField);
                break;
            case SMALLINT:
                checkDivisorNotZero((short) retractField);
                result = (short) ((short) accumulator / (short) retractField);
                break;
            case INTEGER:
                checkDivisorNotZero((int) retractField);
                result = (int) accumulator / (int) retractField;
                break;
            case BIGINT:
                checkDivisorNotZero((long) retractField);
                result = (long) accumulator / (long) retractField;
                break;
            case FLOAT:
                checkDivisorNotZero((float) retractField);
                result = (float) accumulator / (float) retractField;
                break;
            case DOUBLE:
                checkDivisorNotZero((double) retractField);
                result = (double) accumulator / (double) retractField;
                break;
            default:
                String msg =
                        String.format(
                                "type %s not support in %s", typeRoot, this.getClass().getName());
                throw new IllegalArgumentException(msg);
        }
        return result;
    }

    private static void checkDivisorNotZero(long divisor) {
        if (divisor == 0) {
            throw new ArithmeticException("Cannot retract product aggregation: division by zero");
        }
    }

    private static void checkDivisorNotZero(double divisor) {
        if (divisor == 0.0) {
            throw new ArithmeticException("Cannot retract product aggregation: division by zero");
        }
    }

    private static void checkDivisorNotZero(BigDecimal divisor) {
        if (divisor.signum() == 0) {
            throw new ArithmeticException("Cannot retract product aggregation: division by zero");
        }
    }
}
