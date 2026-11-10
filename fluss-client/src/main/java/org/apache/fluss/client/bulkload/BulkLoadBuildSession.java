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

package org.apache.fluss.client.bulkload;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.utils.IOUtils;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.File;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Managed build session for producing the bucket files of one BulkLoad transaction.
 *
 * <p>A session is opened through {@link BulkLoadClient#openBuildSession(BulkLoadBuildContext)} and
 * owns all bucket writers created from it. Closing the session closes any writers that remain open.
 * The session does not commit or abort the transaction.
 *
 * <p>This class is not thread-safe. A caller must not create or use bucket writers concurrently
 * with closing their session.
 */
@PublicEvolving
@NotThreadSafe
public final class BulkLoadBuildSession implements AutoCloseable {

    private final BulkLoadBuildContext context;
    private final List<File> localRootDirs;
    private final Set<BulkLoadBucketWriter> openWriters =
            Collections.newSetFromMap(new IdentityHashMap<>());

    private boolean closed;

    BulkLoadBuildSession(BulkLoadBuildContext context, List<File> localRootDirs) {
        this.context = checkNotNull(context, "BulkLoad build context must not be null.");
        this.localRootDirs =
                checkNotNull(localRootDirs, "BulkLoad local root directories must not be null.");
        checkArgument(
                !localRootDirs.isEmpty(), "BulkLoad requires at least one local root directory.");
    }

    /**
     * Opens a writer for one bucket.
     *
     * @param target the physical target owned by the writer
     * @param bucketId the bucket owned by the writer
     * @return the opened bucket writer
     */
    public BulkLoadBucketWriter createBucketWriter(PhysicalTablePath target, int bucketId) {
        checkState(!closed, "BulkLoad build session is already closed.");
        BulkLoadBucketWriter writer =
                new BulkLoadBucketWriter(this, context.getTarget(target), bucketId);
        openWriters.add(writer);
        return writer;
    }

    /** Returns the configured local root that a bucket writer builds under, spread by bucket id. */
    File localRootDir(int bucketId) {
        return localRootDirs.get(Math.floorMod(bucketId, localRootDirs.size()));
    }

    /**
     * Closes every writer still owned by this session and prevents new writers from being opened.
     */
    @Override
    public void close() throws Exception {
        closed = true;
        IOUtils.closeAll(openWriters.toArray(new BulkLoadBucketWriter[0]));
    }

    void release(BulkLoadBucketWriter writer) {
        openWriters.remove(writer);
    }
}
