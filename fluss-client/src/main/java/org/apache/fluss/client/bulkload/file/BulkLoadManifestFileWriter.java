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
import org.apache.fluss.fs.FsPath;

import java.io.IOException;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Publishes preassembled BulkLoad manifest bytes with exact create-or-reuse semantics. */
@Internal
public final class BulkLoadManifestFileWriter {

    private BulkLoadManifestFileWriter() {}

    /** Publishes the given bytes and returns their immutable file handle. */
    public static BulkLoadFileHandle write(FsPath path, byte[] manifest) throws IOException {
        checkNotNull(path, "BulkLoad manifest path must not be null.");
        checkNotNull(manifest, "BulkLoad manifest bytes must not be null.");
        BulkLoadFileIO.writeBytesExact(path, manifest);
        return new BulkLoadFileHandle(
                path.toString(), manifest.length, BulkLoadDigests.sha256Hex(manifest));
    }
}
