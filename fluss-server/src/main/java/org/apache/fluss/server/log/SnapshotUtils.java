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

package org.apache.fluss.server.log;

import org.apache.fluss.annotation.VisibleForTesting;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.fluss.utils.FlussPaths.STATE_SNAPSHOT_FILE_SUFFIX;
import static org.apache.fluss.utils.FlussPaths.WRITER_SNAPSHOT_FILE_SUFFIX;

/** Utility class for loading snapshots. */
public class SnapshotUtils {

    public static final Predicate<Path> STATE_SNAPSHOT_FILTER =
            path -> path.getFileName().toString().endsWith(STATE_SNAPSHOT_FILE_SUFFIX);
    public static final Predicate<Path> WRITER_SNAPSHOT_FILTER =
            path -> path.getFileName().toString().endsWith(WRITER_SNAPSHOT_FILE_SUFFIX);

    public static ConcurrentSkipListMap<Long, SnapshotFile> loadStateSnapshots(File dir)
            throws IOException {
        return loadSnapshots(dir, STATE_SNAPSHOT_FILTER);
    }

    public static ConcurrentSkipListMap<Long, SnapshotFile> loadSnapshots(
            File dir, Predicate<Path> snapshotFilter) throws IOException {
        ConcurrentSkipListMap<Long, SnapshotFile> offsetToSnapshots = new ConcurrentSkipListMap<>();
        List<SnapshotFile> snapshotFiles = listSnapshotFiles(dir, snapshotFilter);
        for (SnapshotFile snapshotFile : snapshotFiles) {
            offsetToSnapshots.put(snapshotFile.offset, snapshotFile);
        }
        return offsetToSnapshots;
    }

    @VisibleForTesting
    public static List<SnapshotFile> listSnapshotFiles(File dir, Predicate<Path> snapshotFilter)
            throws IOException {
        if (dir.exists() && dir.isDirectory()) {
            try (Stream<Path> paths = Files.list(dir.toPath())) {
                return paths.filter(snapshotFilter)
                        .map(path -> new SnapshotFile(path.toFile()))
                        .collect(Collectors.toList());
            }
        } else {
            return Collections.emptyList();
        }
    }
}
