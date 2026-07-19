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

package org.apache.fluss.flink.action.orphan.fs;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.flink.action.orphan.audit.AuditFailureDetail;
import org.apache.fluss.flink.action.orphan.audit.AuditLogger;
import org.apache.fluss.flink.action.orphan.audit.AuditStage;
import org.apache.fluss.flink.action.orphan.audit.CleanupObjectType;
import org.apache.fluss.flink.action.orphan.audit.ScopeIdentity;
import org.apache.fluss.flink.action.orphan.rule.Decision;
import org.apache.fluss.flink.action.orphan.rule.FileMeta;
import org.apache.fluss.flink.action.orphan.rule.RuleId;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.shaded.guava32.com.google.common.util.concurrent.RateLimiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Sole entry point for filesystem deletion within the orphan cleanup package.
 *
 * <p>Only two operations are exposed:
 *
 * <ul>
 *   <li>{@link #deleteFile} - delete a single file (never recursive).
 *   <li>{@link #deleteEmptyDir} - delete a directory only if it is currently empty.
 * </ul>
 *
 * <p>By design there is no recursive-delete API; any caller that needs deletion under {@code
 * fluss-flink-common/.../action/orphan/} should go through this class. The single-entry-point
 * invariant is currently enforced only by convention — there is no Checkstyle rule guarding it.
 */
@Internal
public final class SafeDeleter {

    private static final Logger LOG = LoggerFactory.getLogger(SafeDeleter.class);

    private final FileSystem fs;
    private final boolean dryRun;
    private final AuditLogger audit;
    private final RateLimiter remoteFsOpRateLimiter;
    private final ScopeIdentity scope;

    public SafeDeleter(
            FileSystem fs, boolean dryRun, AuditLogger audit, RateLimiter remoteFsOpRateLimiter) {
        this(fs, dryRun, audit, remoteFsOpRateLimiter, ScopeIdentity.global());
    }

    public SafeDeleter(
            FileSystem fs,
            boolean dryRun,
            AuditLogger audit,
            RateLimiter remoteFsOpRateLimiter,
            ScopeIdentity scope) {
        this.fs = fs;
        this.dryRun = dryRun;
        this.audit = audit;
        this.remoteFsOpRateLimiter = remoteFsOpRateLimiter;
        this.scope = scope;
    }

    /**
     * Delete a single file.
     *
     * @return {@code true} if the file was actually deleted (or recorded as would-be-deleted under
     *     {@code dryRun}); {@code false} if {@link FileSystem#delete} returned {@code false}
     *     (deletion silently failed — e.g. permissions, transient remote-store error). Callers
     *     should track {@code false} returns as delete failures in their run summary.
     */
    public boolean deleteFile(FsPath file, Decision decision, RuleId ruleId) {
        return deleteFile(new FileMeta(file, -1L, -1L), decision, ruleId);
    }

    public boolean deleteFile(FileMeta file, Decision decision, RuleId ruleId) {
        return deleteFileInternal(file, decision, ruleId, null);
    }

    public boolean deleteFile(FileMeta file, Decision decision, RuleId ruleId, long cutoffMillis) {
        return deleteFileInternal(file, decision, ruleId, cutoffMillis);
    }

    private boolean deleteFileInternal(
            FileMeta file, Decision decision, RuleId ruleId, Long cutoffMillis) {
        checkArgument(
                decision == Decision.DELETE,
                "deleteFile must only be called for Decision.DELETE, got %s",
                decision);
        if (dryRun) {
            if (cutoffMillis == null) {
                audit.logWouldDelete(file, ruleId, scope);
            } else {
                audit.logWouldDelete(file, ruleId, scope, cutoffMillis);
            }
            return true;
        }
        remoteFsOpRateLimiter.acquire();
        try {
            boolean ok = fs.delete(file.path(), false);
            if (ok) {
                if (cutoffMillis == null) {
                    audit.logDeleted(file, ruleId, scope);
                } else {
                    audit.logDeleted(file, ruleId, scope, cutoffMillis);
                }
            } else {
                logDeleteFailed(file, ruleId, "filesystem_returned_false", cutoffMillis);
            }
            return ok;
        } catch (IOException e) {
            LOG.warn("Failed to delete file: {}", file.path(), e);
            logDeleteFailed(file, ruleId, "io_error", cutoffMillis);
            return false;
        }
    }

    private void logDeleteFailed(
            FileMeta file, RuleId ruleId, String reasonCode, Long cutoffMillis) {
        if (cutoffMillis == null) {
            audit.logDeleteFailed(file, ruleId, scope, reasonCode, true);
        } else {
            audit.logDeleteFailed(file, ruleId, scope, reasonCode, true, cutoffMillis);
        }
    }

    /**
     * Delete a directory only if it is currently empty.
     *
     * @return {@code true} if the directory was actually deleted (or recorded as would-be-deleted
     *     under {@code dryRun}); {@code false} if the directory was non-empty / unreadable, or if
     *     {@link FileSystem#delete} returned {@code false}. Callers should not increment a "deleted
     *     directory" counter when this returns {@code false}.
     */
    public boolean deleteEmptyDir(FsPath dir) {
        return deleteEmptyDir(dir, -1L);
    }

    public boolean deleteEmptyDir(FsPath dir, long modificationTime) {
        return deleteEmptyDirDetailed(dir, modificationTime).successful();
    }

    public DirectoryDeleteResult deleteEmptyDirDetailed(FsPath dir, long modificationTime) {
        FileStatus[] children = listChildrenSilently(dir);
        if (children == null) {
            audit.logSkippedDirectory(
                    dir, modificationTime, scope, "directory_list_failed", dryRun, true, true);
            return DirectoryDeleteResult.LIST_FAILED;
        }
        if (children.length > 0) {
            audit.logSkippedDirectory(
                    dir, modificationTime, scope, "directory_not_empty", dryRun, false, false);
            return DirectoryDeleteResult.NOT_EMPTY;
        }
        if (dryRun) {
            audit.logWouldDeleteDirectory(dir, modificationTime, scope, true);
            return DirectoryDeleteResult.SUCCESS;
        }
        remoteFsOpRateLimiter.acquire();
        try {
            boolean ok = fs.delete(dir, false);
            if (ok) {
                audit.logDeletedDirectory(dir, modificationTime, scope, false);
                return DirectoryDeleteResult.SUCCESS;
            }
            audit.logDirectoryDeleteFailed(
                    dir, modificationTime, scope, "filesystem_returned_false", false, true);
            return DirectoryDeleteResult.DELETE_FAILED;
        } catch (IOException e) {
            LOG.warn("Failed to delete empty directory: {}", dir, e);
            audit.logDirectoryDeleteFailed(dir, modificationTime, scope, "io_error", false, true);
            return DirectoryDeleteResult.DELETE_FAILED;
        }
    }

    /** Result of the final empty-directory recheck and non-recursive delete attempt. */
    public enum DirectoryDeleteResult {
        SUCCESS,
        NOT_EMPTY,
        LIST_FAILED,
        DELETE_FAILED;

        public boolean successful() {
            return this == SUCCESS;
        }
    }

    private FileStatus[] listChildrenSilently(FsPath dir) {
        try {
            remoteFsOpRateLimiter.acquire();
            return fs.listStatus(dir);
        } catch (IOException e) {
            audit.logFilesystemFailure(
                    AuditStage.SCAN,
                    scope,
                    CleanupObjectType.DIRECTORY,
                    AuditFailureDetail.builder("list_directory", "directory_list_failed")
                            .targetPath(dir)
                            .exceptionClass(e.getClass())
                            .retryable(true)
                            .actionRequired(true)
                            .consistencyRacePossible(true)
                            .build());
            return null;
        }
    }
}
