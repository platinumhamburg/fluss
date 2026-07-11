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

package org.apache.fluss.server.index;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.WriterKey;

import org.junit.jupiter.api.Test;

import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link IndexWriterKey}. */
class IndexWriterKeyTest {

    @Test
    void testPartitionedSourceBucketRoundTrip() {
        WriterKey key = IndexWriterKey.encode(new TableBucket(99L, Long.MAX_VALUE, 3));

        assertThat(key.high()).isEqualTo(Long.MAX_VALUE);
        assertThat(key.low()).isEqualTo(Long.MIN_VALUE | 3L);
        IndexWriterKey.SourceBucket decoded = IndexWriterKey.decode(key);
        assertThat(decoded.getPartitionId()).isEqualTo(OptionalLong.of(Long.MAX_VALUE));
        assertThat(decoded.getBucketId()).isEqualTo(3);
    }

    @Test
    void testUnpartitionedSourceBucketRoundTrip() {
        WriterKey key = IndexWriterKey.encode(new TableBucket(99L, Integer.MAX_VALUE));

        assertThat(key).isEqualTo(new WriterKey(0L, Integer.MAX_VALUE));
        assertThat(IndexWriterKey.decode(key).getPartitionId()).isEmpty();
    }

    @Test
    void testRejectsNonCanonicalWriterKeys() {
        assertThatThrownBy(() -> IndexWriterKey.decode(new WriterKey(1L, 3L)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                IndexWriterKey.decode(
                                        new WriterKey(1L, Long.MIN_VALUE | (1L << 40))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> IndexWriterKey.encode(new TableBucket(99L, -1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> IndexWriterKey.encode(new TableBucket(99L, -1L, 0)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
