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

package org.apache.fluss.flink.utils;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SecondaryIndexColumnNames}. */
class SecondaryIndexColumnNamesTest {

    @Test
    void testJsonRoundTripPreservesArbitraryColumnNames() {
        assertThat(
                        SecondaryIndexColumnNames.decode(
                                SecondaryIndexColumnNames.encode(
                                        Arrays.asList("last,name", " code ", "quote\"name"))))
                .containsExactly("last,name", " code ", "quote\"name");
    }

    @Test
    void testLegacyCommaSeparatedColumnsRemainSupported() {
        assertThat(SecondaryIndexColumnNames.decode("name, email"))
                .containsExactly("name", "email");
    }

    @Test
    void testLegacyColumnsRejectEmptyItems() {
        assertThatThrownBy(() -> SecondaryIndexColumnNames.decode("name,,email"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty column name");
        assertThatThrownBy(() -> SecondaryIndexColumnNames.decode("name,"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty column name");
    }
}
