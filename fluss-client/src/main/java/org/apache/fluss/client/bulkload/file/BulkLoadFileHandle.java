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

package org.apache.fluss.client.bulkload.file;

import org.apache.fluss.annotation.Internal;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Immutable handle identifying one BulkLoad file by path, length, and SHA-256. */
@Internal
public final class BulkLoadFileHandle implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String path;
    private final long length;
    private final String sha256;

    /** Creates an immutable BulkLoad file handle. */
    public BulkLoadFileHandle(String path, long length, String sha256) {
        this.path = checkNotNull(path, "BulkLoad file path must not be null.");
        checkArgument(!path.isEmpty(), "BulkLoad file path must not be empty.");
        checkArgument(length > 0L, "BulkLoad file length must be positive.");
        this.length = length;
        this.sha256 = checkNotNull(sha256, "BulkLoad file SHA-256 must not be null.");
        checkArgument(
                sha256.matches("[0-9a-f]{64}"),
                "BulkLoad file SHA-256 must contain exactly 64 lowercase hexadecimal characters.");
    }

    /** Returns the file path. */
    public String getPath() {
        return path;
    }

    /** Returns the file length in bytes. */
    public long getLength() {
        return length;
    }

    /** Returns the lowercase hexadecimal SHA-256. */
    public String getSha256() {
        return sha256;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BulkLoadFileHandle that = (BulkLoadFileHandle) o;
        return length == that.length
                && Objects.equals(path, that.path)
                && Objects.equals(sha256, that.sha256);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, length, sha256);
    }

    @Override
    public String toString() {
        return "BulkLoadFileHandle{"
                + "path='"
                + path
                + '\''
                + ", length="
                + length
                + ", sha256='"
                + sha256
                + '\''
                + '}';
    }
}
