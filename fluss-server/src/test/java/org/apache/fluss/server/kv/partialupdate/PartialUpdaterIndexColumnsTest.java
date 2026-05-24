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

package org.apache.fluss.server.kv.partialupdate;

import org.apache.fluss.exception.InvalidTargetColumnException;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression guard for {@link PartialUpdater}: secondary-index columns MUST NOT be exempted from
 * the "non-target columns must be nullable" sanity check. In Fluss V1 there was a code path that
 * silently allowed NOT NULL index columns to sit outside the target column set, which caused
 * partial-update merges to push wrong index entries downstream. The V2 implementation has no such
 * exemption and these tests pin that contract.
 */
class PartialUpdaterIndexColumnsTest {

    private static final short SCHEMA_ID = 1;

    @Test
    void testNotNullIndexColumnNotInPartialUpdateTargetIsRejected() {
        // user_id is a NOT NULL non-PK column that backs a secondary index. It is NOT included in
        // the partial-update target columns {id, note}, so the sanity check must reject the writer.
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT().copy(false))
                        .column("user_id", DataTypes.BIGINT().copy(false))
                        .column("note", DataTypes.STRING().copy(true))
                        .primaryKey("id")
                        .index("idx_user", "user_id")
                        .build();

        assertThatThrownBy(
                        () ->
                                new PartialUpdater(
                                        KvFormat.COMPACTED,
                                        SCHEMA_ID,
                                        schema,
                                        new int[] {0, 2}))
                .isInstanceOf(InvalidTargetColumnException.class)
                .hasMessageContaining("user_id");
    }

    @Test
    void testNullableIndexColumnNotInPartialUpdateTargetIsAccepted() {
        // user_id is a nullable index column. Leaving it out of the partial-update target column
        // set must be allowed — the sanity check only fires on NOT NULL non-PK columns.
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT().copy(false))
                        .column("user_id", DataTypes.BIGINT().copy(true))
                        .column("note", DataTypes.STRING().copy(true))
                        .primaryKey("id")
                        .index("idx_user", "user_id")
                        .build();

        PartialUpdater updater =
                new PartialUpdater(KvFormat.COMPACTED, SCHEMA_ID, schema, new int[] {0, 2});
        assertThat(updater).isNotNull();
    }
}
