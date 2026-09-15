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

package org.apache.fluss.client;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.rpc.messages.PbTestFileStatus;

import java.util.Objects;

/** Immutable file attributes returned by the filesystem test endpoint. */
@Internal
public final class TestFileStatus {
    private final String path;
    private final boolean directory;
    private final long length;
    private final long modificationTime;

    TestFileStatus(PbTestFileStatus status) {
        path = status.getPath();
        directory = status.isDirectory();
        length = status.getLength();
        modificationTime = status.getModificationTime();
    }

    /** Returns the complete filesystem URI. */
    public String getPath() {
        return path;
    }

    /** Returns whether this entry is a directory. */
    public boolean isDirectory() {
        return directory;
    }

    /** Returns the file length in bytes. */
    public long getLength() {
        return length;
    }

    /** Returns the unmodified filesystem mtime value. */
    public long getModificationTime() {
        return modificationTime;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof TestFileStatus)) {
            return false;
        }
        TestFileStatus that = (TestFileStatus) other;
        return directory == that.directory
                && length == that.length
                && modificationTime == that.modificationTime
                && path.equals(that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, directory, length, modificationTime);
    }

    @Override
    public String toString() {
        return "TestFileStatus{path='"
                + path
                + "', directory="
                + directory
                + ", length="
                + length
                + ", modificationTime="
                + modificationTime
                + '}';
    }
}
