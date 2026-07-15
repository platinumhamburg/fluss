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

import org.apache.fluss.annotation.Internal;

import java.io.Serializable;

/** Low-cardinality file rule decision counters for one cleanup object type. */
@Internal
public final class RuleDecisionCounters implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long scannedFiles;
    private final long scannedBytes;
    private final long keepActiveFiles;
    private final long keepActiveBytes;
    private final long newerThanCutoffFiles;
    private final long newerThanCutoffBytes;
    private final long mtimeUnavailableFiles;
    private final long mtimeUnavailableBytes;
    private final long unknownFileTypeFiles;
    private final long unknownFileTypeBytes;
    private final long candidateFiles;
    private final long candidateBytes;

    private RuleDecisionCounters(
            long scannedFiles,
            long scannedBytes,
            long keepActiveFiles,
            long keepActiveBytes,
            long newerThanCutoffFiles,
            long newerThanCutoffBytes,
            long mtimeUnavailableFiles,
            long mtimeUnavailableBytes,
            long unknownFileTypeFiles,
            long unknownFileTypeBytes,
            long candidateFiles,
            long candidateBytes) {
        this.scannedFiles = scannedFiles;
        this.scannedBytes = scannedBytes;
        this.keepActiveFiles = keepActiveFiles;
        this.keepActiveBytes = keepActiveBytes;
        this.newerThanCutoffFiles = newerThanCutoffFiles;
        this.newerThanCutoffBytes = newerThanCutoffBytes;
        this.mtimeUnavailableFiles = mtimeUnavailableFiles;
        this.mtimeUnavailableBytes = mtimeUnavailableBytes;
        this.unknownFileTypeFiles = unknownFileTypeFiles;
        this.unknownFileTypeBytes = unknownFileTypeBytes;
        this.candidateFiles = candidateFiles;
        this.candidateBytes = candidateBytes;
    }

    public static RuleDecisionCounters empty() {
        return new RuleDecisionCounters(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
    }

    public static RuleDecisionCounters scanned(long bytes) {
        return new RuleDecisionCounters(1L, bytes, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
    }

    public static RuleDecisionCounters candidate(long bytes) {
        return new RuleDecisionCounters(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 1L, bytes);
    }

    public static RuleDecisionCounters keepActive(long bytes) {
        return new RuleDecisionCounters(0L, 0L, 1L, bytes, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
    }

    public static RuleDecisionCounters newerThanCutoff(long bytes) {
        return new RuleDecisionCounters(0L, 0L, 0L, 0L, 1L, bytes, 0L, 0L, 0L, 0L, 0L, 0L);
    }

    public static RuleDecisionCounters mtimeUnavailable(long bytes) {
        return new RuleDecisionCounters(0L, 0L, 0L, 0L, 0L, 0L, 1L, bytes, 0L, 0L, 0L, 0L);
    }

    public static RuleDecisionCounters unknownFileType(long bytes) {
        return new RuleDecisionCounters(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 1L, bytes, 0L, 0L);
    }

    public RuleDecisionCounters add(RuleDecisionCounters other) {
        return new RuleDecisionCounters(
                scannedFiles + other.scannedFiles,
                scannedBytes + other.scannedBytes,
                keepActiveFiles + other.keepActiveFiles,
                keepActiveBytes + other.keepActiveBytes,
                newerThanCutoffFiles + other.newerThanCutoffFiles,
                newerThanCutoffBytes + other.newerThanCutoffBytes,
                mtimeUnavailableFiles + other.mtimeUnavailableFiles,
                mtimeUnavailableBytes + other.mtimeUnavailableBytes,
                unknownFileTypeFiles + other.unknownFileTypeFiles,
                unknownFileTypeBytes + other.unknownFileTypeBytes,
                candidateFiles + other.candidateFiles,
                candidateBytes + other.candidateBytes);
    }

    public boolean isConsistent() {
        return scannedFiles
                        == keepActiveFiles
                                + newerThanCutoffFiles
                                + mtimeUnavailableFiles
                                + unknownFileTypeFiles
                                + candidateFiles
                && scannedBytes
                        == keepActiveBytes
                                + newerThanCutoffBytes
                                + mtimeUnavailableBytes
                                + unknownFileTypeBytes
                                + candidateBytes;
    }

    public long scannedFiles() {
        return scannedFiles;
    }

    public long scannedBytes() {
        return scannedBytes;
    }

    public long keepActiveFiles() {
        return keepActiveFiles;
    }

    public long keepActiveBytes() {
        return keepActiveBytes;
    }

    public long newerThanCutoffFiles() {
        return newerThanCutoffFiles;
    }

    public long newerThanCutoffBytes() {
        return newerThanCutoffBytes;
    }

    public long mtimeUnavailableFiles() {
        return mtimeUnavailableFiles;
    }

    public long mtimeUnavailableBytes() {
        return mtimeUnavailableBytes;
    }

    public long unknownFileTypeFiles() {
        return unknownFileTypeFiles;
    }

    public long unknownFileTypeBytes() {
        return unknownFileTypeBytes;
    }

    public long candidateFiles() {
        return candidateFiles;
    }

    public long candidateBytes() {
        return candidateBytes;
    }
}
