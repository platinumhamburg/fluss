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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.LogTestUtils;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import java.io.File;

/** Shared test utilities for index-related tests. */
class IndexTestUtils {

    static final int PAGE_SIZE = 64 * 1024;
    static final long DATA_TABLE_ID = 1000L;
    static final long INDEX_TABLE_ID = 10001L;
    static final int INDEX_BUCKET_COUNT = 3;
    static final TablePath DATA_TABLE_PATH = TablePath.of("test_db", "test_table");

    static final RowType INDEX_TEST_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("id", DataTypes.INT()),
                    new DataField("name", DataTypes.STRING()),
                    new DataField("email", DataTypes.STRING()));

    static IndexedRow createIndexTestRow(int id, String name, String email) {
        return DataTestUtils.indexedRow(INDEX_TEST_ROW_TYPE, new Object[] {id, name, email});
    }

    /** Creates a {@link LogTablet} for index tests, parameterized by {@link LogFormat}. */
    static LogTablet createLogTablet(File tempDir, FlussScheduler scheduler, LogFormat logFormat)
            throws Exception {
        File logDir =
                LogTestUtils.makeRandomLogTabletDir(
                        tempDir,
                        DATA_TABLE_PATH.getDatabaseName(),
                        DATA_TABLE_ID,
                        DATA_TABLE_PATH.getTableName());

        return LogTablet.create(
                PhysicalTablePath.of(DATA_TABLE_PATH),
                logDir,
                new Configuration(),
                TestingMetricGroups.TABLET_SERVER_METRICS,
                0L,
                scheduler,
                logFormat,
                1,
                false,
                SystemClock.getInstance(),
                true);
    }

    private IndexTestUtils() {}
}
