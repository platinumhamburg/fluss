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

import com.alibaba.fluss.memory.InputView;
import com.alibaba.fluss.memory.OutputView;

import java.io.IOException;

/** Type serializer for {@code Double} (and {@code double}, via auto-boxing). */
public final class DoubleSerializer extends SerializerSingleton<Double> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the IntSerializer. */
    public static final DoubleSerializer INSTANCE = new DoubleSerializer();

    @Override
    public Double copy(Double from) {
        return from;
    }

    @Override
    public void serialize(Double record, OutputView target) throws IOException {
        target.writeDouble(record);
    }

    @Override
    public Double deserialize(InputView source) throws IOException {
        return source.readDouble();
    }

    @Override
    public String serializeToString(Double record) {
        return record.toString();
    }

    @Override
    public Double deserializeFromString(String s) {
        return Double.valueOf(s);
    }
}
