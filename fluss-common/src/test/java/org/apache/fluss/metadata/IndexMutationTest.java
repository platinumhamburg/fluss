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

package org.apache.fluss.metadata;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexMutationTest {

    @Test
    void testUpsertCarriesKeyValueAndSourceOffset() {
        byte[] key = new byte[] {1, 2};
        byte[] value = new byte[] {3, 4};
        IndexMutation m =
                IndexMutation.upsert(
                        /* indexTableId= */ 7L,
                        /* indexBucket= */ 3,
                        key,
                        value,
                        /* sourceOffset= */ 42L);
        assertThat(m.getOp()).isEqualTo(IndexMutationOp.UPSERT);
        assertThat(m.getIndexTableId()).isEqualTo(7L);
        assertThat(m.getIndexBucket()).isEqualTo(3);
        assertThat(m.getKey()).isEqualTo(key);
        assertThat(m.getValue()).isEqualTo(value);
        assertThat(m.getSourceOffset()).isEqualTo(42L);
    }

    @Test
    void testDeleteCarriesKeyWithNullValue() {
        IndexMutation m = IndexMutation.delete(7L, 3, new byte[] {1}, 42L);
        assertThat(m.getOp()).isEqualTo(IndexMutationOp.DELETE);
        assertThat(m.getValue()).isNull();
    }

    @Test
    void testUpsertRejectsNullKey() {
        assertThatThrownBy(() -> IndexMutation.upsert(1L, 0, null, new byte[0], 1L))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testUpsertRejectsNullValue() {
        assertThatThrownBy(() -> IndexMutation.upsert(1L, 0, new byte[0], null, 1L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testDeleteRejectsNullKey() {
        assertThatThrownBy(() -> IndexMutation.delete(1L, 0, null, 1L))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testEqualsAndHashCodeUseAllFields() {
        IndexMutation a = IndexMutation.upsert(1L, 0, new byte[] {1}, new byte[] {2}, 5L);
        IndexMutation b = IndexMutation.upsert(1L, 0, new byte[] {1}, new byte[] {2}, 5L);
        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
    }

    @Test
    void testEqualsDifferentiatesByOp() {
        IndexMutation a = IndexMutation.upsert(1L, 0, new byte[] {1}, new byte[] {2}, 5L);
        IndexMutation b = IndexMutation.delete(1L, 0, new byte[] {1}, 5L);
        assertThat(a).isNotEqualTo(b);
    }
}
