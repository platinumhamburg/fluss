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

package org.apache.fluss.flink.action.orphan.rule;

import org.apache.fluss.fs.FsPath;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class RuleEvaluationTest {

    private static final String SEGMENT_ID = "11111111-1111-1111-1111-111111111111";
    private static final long CUTOFF = 1_000L;

    @Test
    void explainsActiveLogSegmentReference() {
        String relativePath = SEGMENT_ID + "/00000000000000000000.log";
        FileMeta file = file("/log/db/t-1/0/" + relativePath, 100L);
        BucketActiveRefs refs =
                new BucketActiveRefs(
                        Collections.singleton(relativePath),
                        Collections.<String>emptySet(),
                        Collections.<String>emptySet());

        RuleEvaluation evaluation = new LogSegmentRule().evaluateDetailed(file, refs, CUTOFF);

        assertThat(evaluation.decision()).isEqualTo(Decision.KEEP_ACTIVE);
        assertThat(evaluation.reasonCode()).isEqualTo("keep_active");
        assertThat(evaluation.referenceType()).contains("log_segment");
        assertThat(evaluation.referenceMatchKind()).contains("relative_path");
        assertThat(evaluation.referenceKey()).contains(relativePath);
    }

    @Test
    void explainsActiveManifestRemotePath() {
        String path = "/log/db/t-1/0/metadata/current.manifest";
        FileMeta file = file(path, 100L);
        BucketActiveRefs refs =
                new BucketActiveRefs(
                        Collections.<String>emptySet(),
                        Collections.<String>emptySet(),
                        Collections.singleton(path));

        RuleEvaluation evaluation = new LogManifestRule(true).evaluateDetailed(file, refs, CUTOFF);

        assertThat(evaluation.decision()).isEqualTo(Decision.KEEP_ACTIVE);
        assertThat(evaluation.referenceType()).contains("log_manifest");
        assertThat(evaluation.referenceMatchKind()).contains("remote_path");
        assertThat(evaluation.referenceKey()).contains(path);
    }

    @Test
    void explainsActiveSnapshotDirectory() {
        FileMeta file = file("/kv/db/t-1/0/snap-9/001.sst", 100L);
        BucketActiveRefs refs =
                new BucketActiveRefs(
                        Collections.<String>emptySet(),
                        Collections.singleton("snap-9"),
                        Collections.<String>emptySet());

        RuleEvaluation evaluation = new KvSnapshotFileRule().evaluateDetailed(file, refs, CUTOFF);

        assertThat(evaluation.decision()).isEqualTo(Decision.KEEP_ACTIVE);
        assertThat(evaluation.referenceType()).contains("kv_snapshot");
        assertThat(evaluation.referenceMatchKind()).contains("snapshot_directory");
        assertThat(evaluation.referenceKey()).contains("snap-9");
    }

    @Test
    void distinguishesConservativePolicyFromReferenceMatch() {
        RuleEvaluation manifest =
                new LogManifestRule()
                        .evaluateDetailed(
                                file("/log/db/t-1/0/metadata/orphan.manifest", 100L),
                                BucketActiveRefs.empty(),
                                CUTOFF);
        RuleEvaluation sharedSst =
                new KvSharedSstRule()
                        .evaluateDetailed(
                                file("/kv/db/t-1/0/shared/001.sst", 100L),
                                BucketActiveRefs.empty(),
                                CUTOFF);

        assertThat(manifest.reasonCode()).isEqualTo("conservative_policy");
        assertThat(manifest.referenceKey()).isEmpty();
        assertThat(sharedSst.reasonCode()).isEqualTo("conservative_policy");
        assertThat(sharedSst.referenceKey()).isEmpty();
    }

    @Test
    void explainsAgeAndRecognitionDecisions() {
        LogSegmentRule rule = new LogSegmentRule();

        assertThat(
                        rule.evaluateDetailed(
                                        file(
                                                "/log/db/t-1/0/"
                                                        + SEGMENT_ID
                                                        + "/00000000000000000000.log",
                                                CUTOFF),
                                        BucketActiveRefs.empty(),
                                        CUTOFF)
                                .reasonCode())
                .isEqualTo("newer_than_cutoff");
        assertThat(
                        rule.evaluateDetailed(
                                        file(
                                                "/log/db/t-1/0/"
                                                        + SEGMENT_ID
                                                        + "/00000000000000000000.log",
                                                Long.MAX_VALUE),
                                        BucketActiveRefs.empty(),
                                        CUTOFF)
                                .reasonCode())
                .isEqualTo("mtime_unavailable");
        assertThat(
                        rule.evaluateDetailed(
                                        file("/log/db/t-1/0/not-a-segment/file.txt", 100L),
                                        BucketActiveRefs.empty(),
                                        CUTOFF)
                                .reasonCode())
                .isEqualTo("unknown_file_type");
        assertThat(
                        rule.evaluateDetailed(
                                        file(
                                                "/log/db/t-1/0/"
                                                        + SEGMENT_ID
                                                        + "/00000000000000000000.log",
                                                100L),
                                        BucketActiveRefs.empty(),
                                        CUTOFF)
                                .reasonCode())
                .isEqualTo("candidate");
    }

    private static FileMeta file(String path, long modificationTime) {
        return new FileMeta(new FsPath(path), 123L, modificationTime);
    }
}
