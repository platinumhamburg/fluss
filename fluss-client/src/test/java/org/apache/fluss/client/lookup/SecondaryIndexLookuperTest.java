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

package org.apache.fluss.client.lookup;

import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SecondaryIndexLookuper}. */
class SecondaryIndexLookuperTest {

    @Test
    void testHop1ForwardsLookupKeyToPrefixLookuper() throws Exception {
        InternalRow expectedKey = new GenericRow(1);
        InternalRow indexHit = new GenericRow(1);
        StubLookuper indexLookuper =
                new StubLookuper(
                        CompletableFuture.completedFuture(
                                new LookupResult(Collections.singletonList(indexHit))));
        StubLookuper mainLookuper =
                new StubLookuper(
                        CompletableFuture.completedFuture(
                                new LookupResult(Collections.<InternalRow>emptyList())));

        SecondaryIndexLookuper lookuper =
                new SecondaryIndexLookuper(
                        indexLookuper,
                        mainLookuper,
                        new int[] {0},
                        new InternalRow.FieldGetter[] {row -> row});

        LookupResult result = lookuper.lookup(expectedKey).get();

        assertThat(indexLookuper.lastLookupKey).isSameAs(expectedKey);
        assertThat(mainLookuper.lastLookupKey).isNull();
        assertThat(result.getRowList()).containsExactly(indexHit);
    }

    private static final class StubLookuper implements Lookuper {
        final CompletableFuture<LookupResult> next;
        volatile InternalRow lastLookupKey;

        StubLookuper(CompletableFuture<LookupResult> next) {
            this.next = next;
        }

        @Override
        public CompletableFuture<LookupResult> lookup(InternalRow key) {
            this.lastLookupKey = key;
            return next;
        }
    }
}
