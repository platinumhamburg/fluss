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

package org.apache.fluss.rpc.protocol;

import org.apache.fluss.annotation.Internal;

/** Operations available only in the filesystem test build. */
@Internal
public enum TestFilesystemOperation {
    LIST(0),
    STAT(1),
    READ(2),
    WRITE(3),
    DELETE(4),
    COPY(5);

    public static final int MAX_CONTENT_LENGTH = 1024 * 1024;
    private final int id;

    TestFilesystemOperation(int id) {
        this.id = id;
    }

    /** Returns the wire identifier. */
    public int id() {
        return id;
    }

    /** Resolves a wire identifier, rejecting unsupported operations. */
    public static TestFilesystemOperation forId(int id) {
        for (TestFilesystemOperation operation : values()) {
            if (operation.id == id) {
                return operation;
            }
        }
        throw new IllegalArgumentException("Unknown filesystem operation: " + id);
    }
}
