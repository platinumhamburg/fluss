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

package org.apache.fluss.flink.action.orphan.audit;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.fs.FsPath;

import javax.annotation.Nullable;

/** Stable structured failure context that deliberately excludes raw exception messages. */
@Internal
public final class AuditFailureDetail {

    private final String operation;
    private final String failureCategory;
    private final @Nullable String targetPath;
    private final @Nullable String metadataPath;
    private final String exceptionClass;
    private final @Nullable String errno;
    private final int attempts;
    private final boolean retryable;
    private final boolean actionRequired;
    private final boolean consistencyRacePossible;

    private AuditFailureDetail(Builder builder) {
        this.operation = required(builder.operation, "operation");
        this.failureCategory = required(builder.failureCategory, "failureCategory");
        this.targetPath = builder.targetPath;
        this.metadataPath = builder.metadataPath;
        this.exceptionClass = required(builder.exceptionClass, "exceptionClass");
        this.errno = builder.errno;
        if (builder.attempts < 1) {
            throw new IllegalArgumentException("attempts");
        }
        this.attempts = builder.attempts;
        this.retryable = builder.retryable;
        this.actionRequired = builder.actionRequired;
        this.consistencyRacePossible = builder.consistencyRacePossible;
    }

    public static Builder builder(String operation, String failureCategory) {
        return new Builder(operation, failureCategory);
    }

    public String operation() {
        return operation;
    }

    public String failureCategory() {
        return failureCategory;
    }

    @Nullable
    public String targetPath() {
        return targetPath;
    }

    @Nullable
    public String metadataPath() {
        return metadataPath;
    }

    public String exceptionClass() {
        return exceptionClass;
    }

    @Nullable
    public String errno() {
        return errno;
    }

    public int attempts() {
        return attempts;
    }

    public boolean retryable() {
        return retryable;
    }

    public boolean actionRequired() {
        return actionRequired;
    }

    public boolean consistencyRacePossible() {
        return consistencyRacePossible;
    }

    private static String required(@Nullable String value, String field) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(field);
        }
        return value;
    }

    /** Builder for stable failure fields. No method accepts a raw exception message. */
    public static final class Builder {
        private final String operation;
        private final String failureCategory;
        private @Nullable String targetPath;
        private @Nullable String metadataPath;
        private @Nullable String exceptionClass;
        private @Nullable String errno;
        private int attempts = 1;
        private boolean retryable;
        private boolean actionRequired = true;
        private boolean consistencyRacePossible;

        private Builder(String operation, String failureCategory) {
            this.operation = operation;
            this.failureCategory = failureCategory;
        }

        public Builder targetPath(FsPath path) {
            this.targetPath = path.toString();
            return this;
        }

        public Builder metadataPath(FsPath path) {
            this.metadataPath = path.toString();
            return this;
        }

        public Builder exceptionClass(Class<? extends Throwable> exceptionClass) {
            return exceptionClass(exceptionClass.getName());
        }

        public Builder exceptionClass(String exceptionClass) {
            this.exceptionClass = exceptionClass;
            return this;
        }

        public Builder errno(String errno) {
            this.errno = errno;
            return this;
        }

        public Builder attempts(int attempts) {
            this.attempts = attempts;
            return this;
        }

        public Builder retryable(boolean retryable) {
            this.retryable = retryable;
            return this;
        }

        public Builder actionRequired(boolean actionRequired) {
            this.actionRequired = actionRequired;
            return this;
        }

        public Builder consistencyRacePossible(boolean consistencyRacePossible) {
            this.consistencyRacePossible = consistencyRacePossible;
            return this;
        }

        public AuditFailureDetail build() {
            return new AuditFailureDetail(this);
        }
    }
}
