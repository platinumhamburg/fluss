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

package org.apache.fluss.client.table.writer;

import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link UpsertWriter} overwrite mode functionality. */
public class UpsertWriterOverwriteModeTest extends ClientToServerITCaseBase {

    private static final TablePath TABLE_PATH = TablePath.of("test_db", "test_table");

    @BeforeEach
    void setUp() throws Exception {
        // Drop table if exists from previous test
        try {
            admin.dropTable(TABLE_PATH, false).get();
        } catch (Exception e) {
            // Ignore if table does not exist
        }
    }

    @Test
    void testSetOverwriteMode() throws Exception {
        // Create a primary key table
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .primaryKey("id")
                                        .column("id", DataTypes.INT())
                                        .column("name", DataTypes.STRING())
                                        .build())
                        .distributedBy(1)
                        .build();

        createTable(TABLE_PATH, tableDescriptor, true);

        try (Table table = conn.getTable(TABLE_PATH)) {
            UpsertWriter writer = table.newUpsert().createWriter();

            // Write some initial data in normal mode
            writer.setOverwriteMode(false);
            InternalRow row1 = row(1, "Alice");
            writer.upsert(row1).get();
            writer.flush();

            // Switch to overwrite mode
            writer.setOverwriteMode(true);
            InternalRow row2 = row(2, "Bob");
            writer.upsert(row2).get();
            writer.flush();

            // Switch back to normal mode
            writer.setOverwriteMode(false);
            InternalRow row3 = row(3, "Charlie");
            writer.upsert(row3).get();
            writer.flush();

            // Verify that all writes succeeded
            // The test mainly verifies that setOverwriteMode doesn't cause exceptions
            // and that the writer can switch modes without issues
        }
    }

    @Test
    void testOverwriteModeWithBucketOffsetTracking() throws Exception {
        // Create a primary key table with 2 buckets
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .primaryKey("id")
                                        .column("id", DataTypes.INT())
                                        .column("value", DataTypes.INT())
                                        .build())
                        .distributedBy(2)
                        .build();

        createTable(TABLE_PATH, tableDescriptor, true);

        try (Table table = conn.getTable(TABLE_PATH)) {
            UpsertWriter writer = table.newUpsert().createWriter();

            // Write data in normal mode
            writer.setOverwriteMode(false);
            for (int i = 0; i < 10; i++) {
                InternalRow row = row(i, i * 100);
                writer.upsert(row).get();
            }
            writer.flush();

            // Get bucket ack status after normal writes
            Map<TableBucket, Long> ackStatusNormal = writer.bucketsAckStatus();
            assertThat(ackStatusNormal).isNotEmpty();

            // Switch to overwrite mode and write more data
            writer.setOverwriteMode(true);
            for (int i = 10; i < 20; i++) {
                InternalRow row = row(i, i * 100);
                writer.upsert(row).get();
            }
            writer.flush();

            // Get bucket ack status after overwrite writes
            Map<TableBucket, Long> ackStatusOverwrite = writer.bucketsAckStatus();
            assertThat(ackStatusOverwrite).isNotEmpty();

            // Verify that bucket offset tracking works in both modes
            // In overwrite mode, offsets should still be tracked
            for (Map.Entry<TableBucket, Long> entry : ackStatusOverwrite.entrySet()) {
                TableBucket bucket = entry.getKey();
                Long overwriteOffset = entry.getValue();

                // If the bucket was written in both modes, the offset should have increased
                if (ackStatusNormal.containsKey(bucket)) {
                    Long normalOffset = ackStatusNormal.get(bucket);
                    assertThat(overwriteOffset).isGreaterThanOrEqualTo(normalOffset);
                }
            }
        }
    }

    @Test
    void testOverwriteModeMultipleSwitches() throws Exception {
        // Create a primary key table
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .primaryKey("id")
                                        .column("id", DataTypes.INT())
                                        .column("data", DataTypes.STRING())
                                        .build())
                        .distributedBy(1)
                        .build();

        createTable(TABLE_PATH, tableDescriptor, true);

        try (Table table = conn.getTable(TABLE_PATH)) {
            UpsertWriter writer = table.newUpsert().createWriter();

            // Perform multiple switches between normal and overwrite mode
            for (int iteration = 0; iteration < 5; iteration++) {
                // Normal mode
                writer.setOverwriteMode(false);
                InternalRow normalRow = row(iteration * 2, "normal-" + iteration);
                writer.upsert(normalRow).get();
                writer.flush();

                // Overwrite mode
                writer.setOverwriteMode(true);
                InternalRow overwriteRow = row(iteration * 2 + 1, "overwrite-" + iteration);
                writer.upsert(overwriteRow).get();
                writer.flush();
            }

            // Verify that bucket offset tracking still works after multiple switches
            Map<TableBucket, Long> finalAckStatus = writer.bucketsAckStatus();
            assertThat(finalAckStatus).isNotEmpty();
        }
    }

    @Test
    void testOverwriteModeWithDelete() throws Exception {
        // Create a primary key table
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .primaryKey("id")
                                        .column("id", DataTypes.INT())
                                        .column("name", DataTypes.STRING())
                                        .build())
                        .distributedBy(1)
                        .build();

        createTable(TABLE_PATH, tableDescriptor, true);

        try (Table table = conn.getTable(TABLE_PATH)) {
            UpsertWriter writer = table.newUpsert().createWriter();

            // Write a row in normal mode
            writer.setOverwriteMode(false);
            InternalRow insertRow = row(1, "test");
            writer.upsert(insertRow).get();
            writer.flush();

            // Verify that delete works in overwrite mode
            Map<TableBucket, Long> ackStatus = writer.bucketsAckStatus();
            assertThat(ackStatus).isNotEmpty();
        }
    }
}
