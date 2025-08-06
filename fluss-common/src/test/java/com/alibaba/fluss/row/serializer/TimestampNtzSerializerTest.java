/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.row.serializer;

import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.utils.DateTimeUtils;
import com.alibaba.fluss.utils.Pair;

import java.util.Arrays;
import java.util.List;

/** Test for {@link TimestampNtzSerializer}. */
public abstract class TimestampNtzSerializerTest extends SerializerTestBase<TimestampNtz> {

    @Override
    protected Serializer<TimestampNtz> createSerializer() {
        return new TimestampNtzSerializer(getPrecision());
    }

    @Override
    protected boolean deepEquals(TimestampNtz t1, TimestampNtz t2) {
        return t1.equals(t2);
    }

    @Override
    protected TimestampNtz[] getTestData() {
        return new TimestampNtz[] {
            TimestampNtz.fromMillis(1),
            TimestampNtz.fromMillis(2),
            TimestampNtz.fromMillis(3),
            TimestampNtz.fromMillis(4)
        };
    }

    protected abstract int getPrecision();

    static final class TimestampNtzSerializer0Test extends TimestampNtzSerializerTest {
        @Override
        protected int getPrecision() {
            return 0;
        }

        @Override
        protected List<Pair<TimestampNtz, String>> getSerializableToStringTestData() {
            return Arrays.asList(
                    Pair.of(
                            DateTimeUtils.parseTimestampNtzData(
                                    "2019-01-01 00:00:00", getPrecision()),
                            "2019-01-01 00:00:00"),
                    Pair.of(
                            DateTimeUtils.parseTimestampNtzData(
                                    "2019-01-01 00:00:01", getPrecision()),
                            "2019-01-01 00:00:01"));
        }
    }

    static final class TimestampNtzSerializer3Test extends TimestampNtzSerializerTest {
        @Override
        protected int getPrecision() {
            return 3;
        }

        @Override
        protected List<Pair<TimestampNtz, String>> getSerializableToStringTestData() {
            return Arrays.asList(
                    Pair.of(
                            DateTimeUtils.parseTimestampNtzData(
                                    "2019-01-01 00:00:00.000", getPrecision()),
                            "2019-01-01 00:00:00.000"),
                    Pair.of(
                            DateTimeUtils.parseTimestampNtzData(
                                    "2019-01-01 00:00:00.001", getPrecision()),
                            "2019-01-01 00:00:00.001"));
        }
    }

    static final class TimestampNtzSerializer6Test extends TimestampNtzSerializerTest {
        @Override
        protected int getPrecision() {
            return 6;
        }

        @Override
        protected List<Pair<TimestampNtz, String>> getSerializableToStringTestData() {
            return Arrays.asList(
                    Pair.of(
                            DateTimeUtils.parseTimestampNtzData(
                                    "2019-01-01 00:00:00.000000", getPrecision()),
                            "2019-01-01 00:00:00.000000"),
                    Pair.of(
                            DateTimeUtils.parseTimestampNtzData(
                                    "2019-01-01 00:00:00.000001", getPrecision()),
                            "2019-01-01 00:00:00.000001"));
        }
    }

    static final class TimestampNtzSerializer8Test extends TimestampNtzSerializerTest {
        @Override
        protected int getPrecision() {
            return 8;
        }

        @Override
        protected List<Pair<TimestampNtz, String>> getSerializableToStringTestData() {
            return Arrays.asList(
                    Pair.of(
                            DateTimeUtils.parseTimestampNtzData(
                                    "2019-01-01 00:00:00.00000000", getPrecision()),
                            "2019-01-01 00:00:00.00000000"),
                    Pair.of(
                            DateTimeUtils.parseTimestampNtzData(
                                    "2019-01-01 00:00:00.00000001", getPrecision()),
                            "2019-01-01 00:00:00.00000001"));
        }
    }
}
