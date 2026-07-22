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

package org.apache.fluss.flink.action.orphan.job;

import org.apache.fluss.flink.action.orphan.fs.SafeDeleter;

/** Task-local scalar progress retained even when cleanup terminates before producing statistics. */
final class ScanTaskProgress implements SafeDeleter.DeletionProgressListener {

    private long scannedFiles;
    private long plannedFiles;
    private long plannedDirs;
    private long plannedBytes;
    private long deletedFiles;
    private long emptyDirsRemoved;
    private long deleteFailures;
    private long bytesReclaimed;

    void recordScannedFile() {
        scannedFiles++;
    }

    void recordPlannedFile(long bytes) {
        plannedFiles++;
        plannedBytes += bytes;
    }

    void recordPlannedDirectory() {
        plannedDirs++;
    }

    @Override
    public void fileDeleted(long bytes) {
        deletedFiles++;
        bytesReclaimed += bytes;
    }

    @Override
    public void fileDeleteFailed() {
        deleteFailures++;
    }

    @Override
    public void directoryDeleted() {
        emptyDirsRemoved++;
    }

    @Override
    public void directoryDeleteFailed() {
        deleteFailures++;
    }

    CleanupCounters snapshot() {
        return new CleanupCounters(
                scannedFiles,
                plannedFiles,
                plannedDirs,
                plannedBytes,
                deletedFiles,
                emptyDirsRemoved,
                deleteFailures,
                bytesReclaimed);
    }
}
