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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.utils.FlussPaths;

/**
 * Rule for manifest files under the {@code metadata/} directory of a log bucket.
 *
 * <p>Default behavior is to return {@link Decision#KEEP_ACTIVE} for every manifest. The asymmetry
 * is the reason: mis-deleting an active manifest leaves the coordinator's manifest pointer dangling
 * and breaks the bucket's metadata chain entirely, while keeping orphan manifests is structurally
 * harmless (KB-sized files). Operators opt into the destructive path via {@code
 * allowDeleteManifest=true} (driven by the {@code --allow-delete-manifest} CLI flag); only then
 * does the rule consult the active-manifest set and apply the file-level age threshold.
 */
@Internal
public final class LogManifestRule implements FileRule {

    private final boolean allowDeleteManifest;

    /** Default-conservative constructor: {@code allowDeleteManifest=false}. */
    public LogManifestRule() {
        this(false);
    }

    public LogManifestRule(boolean allowDeleteManifest) {
        this.allowDeleteManifest = allowDeleteManifest;
    }

    @Override
    public RuleId id() {
        return RuleId.LOG_MANIFEST;
    }

    @Override
    public RuleEvaluation evaluateDetailed(
            FileMeta file, BucketActiveRefs activeRefs, long cutoffMillis) {
        FsPath path = file.path();
        FsPath parent = path.getParent();
        if (parent == null
                || !FlussPaths.REMOTE_LOG_METADATA_DIR_NAME.equals(parent.getName())
                || !path.getName().endsWith(".manifest")) {
            return RuleEvaluation.decision(Decision.SKIP_UNKNOWN, "unknown_file_type");
        }

        // Default-conservative: never delete a manifest. Keeping orphans is harmless; deleting an
        // active manifest leaves the coordinator's manifest pointer dangling and breaks the
        // bucket's metadata chain.
        if (!allowDeleteManifest) {
            return RuleEvaluation.decision(Decision.KEEP_ACTIVE, "conservative_policy");
        }

        // Opt-in path: preserve the original active-set + cutoff semantics. The "current" bucket
        // manifest is always present in logActiveManifestPaths (the server emits one path per
        // bucket in ListRemoteLogManifests), so a single set lookup suffices.
        String pathString = path.toString();
        if (activeRefs.logActiveManifestPaths().contains(pathString)) {
            return RuleEvaluation.active("keep_active", "log_manifest", "remote_path", pathString);
        }

        Decision decision = MtimePolicy.evaluateInactiveFile(file.modificationTime(), cutoffMillis);
        if (decision == Decision.MTIME_UNAVAILABLE) {
            return RuleEvaluation.decision(decision, "mtime_unavailable");
        }
        if (decision == Decision.DEFER) {
            return RuleEvaluation.decision(decision, "newer_than_cutoff");
        }
        return RuleEvaluation.decision(decision, "candidate");
    }
}
