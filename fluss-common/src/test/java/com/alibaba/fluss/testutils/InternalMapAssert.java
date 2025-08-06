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

package com.alibaba.fluss.testutils;

import com.alibaba.fluss.row.InternalMap;
import com.alibaba.fluss.types.MapType;

import org.assertj.core.api.AbstractAssert;

import static com.alibaba.fluss.testutils.InternalArrayAssert.assertThatArray;

/** Assertion class for {@link InternalMap}. */
public class InternalMapAssert extends AbstractAssert<InternalMapAssert, InternalMap> {
    MapType mapType;

    InternalMapAssert(InternalMap actual) {
        super(actual, InternalMapAssert.class);
    }

    public static InternalMapAssert assertThatMap(InternalMap actual) {
        return new InternalMapAssert(actual);
    }

    public InternalMapAssert withMapType(MapType mapType) {
        this.mapType = mapType;
        return this;
    }

    public InternalMapAssert isEqualTo(InternalMap expected) {
        assert mapType != null;
        assertThatArray(actual.keyArray())
                .withElementType(mapType.getKeyType())
                .isEqualTo(expected.keyArray());
        assertThatArray(actual.valueArray())
                .withElementType(mapType.getValueType())
                .isEqualTo(expected.valueArray());
        return this;
    }
}
