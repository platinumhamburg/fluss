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

package org.apache.fluss.server.log.checkpoint;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.exception.LogStorageException;
import org.apache.fluss.utils.types.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This class persists bucket state to a file.
 *
 * <p>Format version 1 uses StateDef ID (int) instead of namespace string for better performance.
 */
@Internal
public final class StateCheckpointFile {
    private static final Pattern WHITE_SPACES_PATTERN = Pattern.compile("\\s+");
    static final int CURRENT_VERSION = 1;

    private final CheckpointFile<Tuple2<Integer, Tuple2<String, String>>> checkpoint;

    public StateCheckpointFile(File file) throws IOException {
        this.checkpoint =
                new CheckpointFile<>(
                        file,
                        StateCheckpointFile.CURRENT_VERSION,
                        new StateCheckpointFile.Formatter());
    }

    public void write(Map<Integer, Map<String, String>> states) {
        List<Tuple2<Integer, Tuple2<String, String>>> list = new ArrayList<>(states.size());
        for (Map.Entry<Integer, Map<String, String>> stateDefEntry : states.entrySet()) {
            for (Map.Entry<String, String> stateEntry : stateDefEntry.getValue().entrySet()) {
                list.add(
                        Tuple2.of(
                                stateDefEntry.getKey(),
                                Tuple2.of(stateEntry.getKey(), stateEntry.getValue())));
            }
        }
        try {
            checkpoint.write(list);
        } catch (IOException e) {
            String msg = "Error while writing to checkpoint file " + checkpoint.getAbsolutePath();
            throw new LogStorageException(msg, e);
        }
    }

    public Map<Integer, Map<String, String>> read() {
        List<Tuple2<Integer, Tuple2<String, String>>> list;
        try {
            list = checkpoint.read();
        } catch (IOException e) {
            String msg = "Error while reading checkpoint file " + checkpoint.getAbsolutePath();
            throw new LogStorageException(msg, e);
        }

        Map<Integer, Map<String, String>> result = new HashMap<>();
        for (Tuple2<Integer, Tuple2<String, String>> tuple : list) {
            result.computeIfAbsent(tuple.f0, k -> new HashMap<>()).put(tuple.f1.f0, tuple.f1.f1);
        }
        return result;
    }

    /** Formatter for state checkpoint file. */
    public static class Formatter
            implements CheckpointFile.EntryFormatter<Tuple2<Integer, Tuple2<String, String>>> {

        @Override
        public String toString(Tuple2<Integer, Tuple2<String, String>> entry) {
            return entry.f0 + " " + entry.f1.f0 + " " + entry.f1.f1;
        }

        @Override
        public Optional<Tuple2<Integer, Tuple2<String, String>>> fromString(String line) {
            String[] parts = WHITE_SPACES_PATTERN.split(line);
            if (parts.length == 3) {
                int stateDefId = Integer.parseInt(parts[0]);
                String stateKey = parts[1];
                String stateValue = parts[2];
                return Optional.of(Tuple2.of(stateDefId, Tuple2.of(stateKey, stateValue)));
            } else {
                return Optional.empty();
            }
        }
    }
}
