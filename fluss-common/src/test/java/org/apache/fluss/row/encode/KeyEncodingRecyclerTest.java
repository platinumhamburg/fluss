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

package org.apache.fluss.row.encode;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KeyEncodingRecycler}. */
class KeyEncodingRecyclerTest {

    @Test
    void testReusesRetainedWriter() {
        AtomicInteger createdWriters = new AtomicInteger();
        KeyEncodingRecycler<TestingWriter> recycler =
                new KeyEncodingRecycler<>(
                        () -> new TestingWriter(createdWriters.incrementAndGet(), 16),
                        TestingWriter::reset,
                        TestingWriter::retainedSizeInBytes,
                        1,
                        64);

        TestingWriter writer = recycler.borrow();
        writer.markDirty();
        recycler.recycle(writer);

        TestingWriter reusedWriter = recycler.borrow();
        assertThat(reusedWriter).isSameAs(writer);
        assertThat(reusedWriter.isDirty()).isFalse();
        assertThat(createdWriters.get()).isEqualTo(1);
    }

    @Test
    void testDropsWriterWhenRetainedSlotsAreFull() {
        AtomicInteger createdWriters = new AtomicInteger();
        KeyEncodingRecycler<TestingWriter> recycler =
                new KeyEncodingRecycler<>(
                        () -> new TestingWriter(createdWriters.incrementAndGet(), 16),
                        TestingWriter::reset,
                        TestingWriter::retainedSizeInBytes,
                        1,
                        64);

        TestingWriter firstWriter = recycler.borrow();
        TestingWriter secondWriter = recycler.borrow();
        recycler.recycle(firstWriter);
        recycler.recycle(secondWriter);

        assertThat(firstWriter.resetCount()).isEqualTo(1);
        assertThat(secondWriter.resetCount()).isZero();

        TestingWriter retainedWriter = recycler.borrow();
        TestingWriter temporaryWriter = recycler.borrow();
        assertThat(retainedWriter).isIn(firstWriter, secondWriter);
        assertThat(temporaryWriter).isNotSameAs(firstWriter).isNotSameAs(secondWriter);
        assertThat(createdWriters.get()).isEqualTo(3);
    }

    @Test
    void testDropsOversizedWriter() {
        AtomicInteger createdWriters = new AtomicInteger();
        KeyEncodingRecycler<TestingWriter> recycler =
                new KeyEncodingRecycler<>(
                        () -> new TestingWriter(createdWriters.incrementAndGet(), 128),
                        TestingWriter::reset,
                        TestingWriter::retainedSizeInBytes,
                        1,
                        64);

        TestingWriter oversizedWriter = recycler.borrow();
        recycler.recycle(oversizedWriter);

        assertThat(oversizedWriter.resetCount()).isZero();

        TestingWriter newWriter = recycler.borrow();
        assertThat(newWriter).isNotSameAs(oversizedWriter);
        assertThat(createdWriters.get()).isEqualTo(2);
    }

    private static final class TestingWriter {

        private final int id;
        private final int retainedSizeInBytes;
        private boolean dirty;
        private int resetCount;

        private TestingWriter(int id, int retainedSizeInBytes) {
            this.id = id;
            this.retainedSizeInBytes = retainedSizeInBytes;
        }

        private void markDirty() {
            dirty = true;
        }

        private void reset() {
            resetCount++;
            dirty = false;
        }

        private boolean isDirty() {
            return dirty;
        }

        private int retainedSizeInBytes() {
            return retainedSizeInBytes;
        }

        private int resetCount() {
            return resetCount;
        }

        @Override
        public String toString() {
            return "TestingWriter{" + "id=" + id + '}';
        }
    }
}
