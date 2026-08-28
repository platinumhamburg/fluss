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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.client.bulkload.file.BulkLoadFileHandle;
import org.apache.fluss.client.bulkload.protocol.BeginBulkLoadResult;
import org.apache.fluss.client.bulkload.protocol.BulkLoadTransactionDriver;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.BulkLoadStatus;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.rpc.RpcClient;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.time.Duration;
import java.util.Collection;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Entry point for building and atomically installing a primary-key table BulkLoad.
 *
 * <p>A single process may call {@link #begin}, build every bucket with {@link
 * BulkLoadBucketWriter}, and call {@link #commit}. Distributed engines may serialize the returned
 * {@link BulkLoadBuildContext}, build buckets independently, and send the opaque {@link
 * BulkLoadBucketFiles}s to one committer. Manifest assembly and transaction recovery remain
 * internal to this client.
 *
 * <p>This client is thread-safe. Concurrent calls keep all transaction-specific state in their
 * arguments and local variables and share the connection's thread-safe RPC and metadata resources.
 *
 * <p>See the <a href="package-summary.html">BulkLoad package documentation</a> for complete
 * single-process and Flink usage examples.
 */
@PublicEvolving
@ThreadSafe
public final class BulkLoadClient {

    private final BulkLoadTransactionDriver driver;

    /** Internal constructor; obtain a client from a Fluss connection. */
    @Internal
    public BulkLoadClient(RpcClient rpcClient, MetadataUpdater metadataUpdater) {
        this.driver =
                new BulkLoadTransactionDriver(
                        checkNotNull(rpcClient, "RPC client must not be null."),
                        checkNotNull(metadataUpdater, "Metadata updater must not be null."));
    }

    /**
     * Begins or recovers this submission.
     *
     * <p>A result requiring a build contains the frozen context for producing bucket files. A
     * result that does not require a build represents a previously decided transaction recovered to
     * the returned persisted status.
     *
     * @return the persisted status and, when building is required, its frozen build context
     */
    public BulkLoadBeginResult begin(
            PhysicalTablePath target,
            String submissionId,
            @Nullable Duration buildTimeout,
            Duration awaitTimeout)
            throws Exception {
        BeginBulkLoadResult result =
                driver.beginOrRecover(target, submissionId, buildTimeout, awaitTimeout);
        if (result.getTargetInfo() != null) {
            return new BulkLoadBeginResult(
                    result.getStatus(), new BulkLoadBuildContext(result.getTargetInfo()));
        }
        BulkLoadStatus status =
                driver.commitUntilReady(result.getStatus().getHandle(), awaitTimeout);
        return new BulkLoadBeginResult(status, null);
    }

    /** Publishes the manifest internally and commits all bucket results atomically. */
    public BulkLoadStatus commit(
            BulkLoadBuildContext context,
            Collection<BulkLoadBucketFiles> bucketFiles,
            Duration awaitTimeout)
            throws Exception {
        checkNotNull(context, "BulkLoad build context must not be null.");
        checkNotNull(bucketFiles, "BulkLoad bucket files must not be null.");
        BulkLoadFileHandle manifest = BulkLoadManifestWriter.write(context, bucketFiles);
        return driver.commitUntilReady(context.getHandle(), manifest, awaitTimeout);
    }

    /** Aborts the transaction and returns its persisted terminal status. */
    public BulkLoadStatus abort(BulkLoadHandle handle) throws Exception {
        return driver.abort(handle);
    }
}
