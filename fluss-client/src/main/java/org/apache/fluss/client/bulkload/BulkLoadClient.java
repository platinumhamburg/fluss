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
import org.apache.fluss.client.bulkload.protocol.BulkLoadTransactionDriver;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.BulkLoadStatus;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.shaded.guava32.com.google.common.hash.HashCode;
import org.apache.fluss.shaded.guava32.com.google.common.hash.Hasher;
import org.apache.fluss.shaded.guava32.com.google.common.hash.Hashing;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Entry point for building and atomically installing a primary-key table BulkLoad.
 *
 * <p>A single process may call {@link #begin}, build every bucket with {@link
 * BulkLoadBucketWriter}, and call {@link #commit}. Distributed engines may serialize the returned
 * {@link BulkLoadBuildContext}, build buckets independently, and send the opaque {@link
 * BulkLoadBucketFiles}s to one committer. Input digest assembly and exact Commit retries remain
 * internal to this client.
 *
 * <p>This client is thread-safe. Concurrent calls keep all transaction-specific state in their
 * arguments and local variables and share the connection's thread-safe RPC and metadata resources.
 *
 * <p>See the <a href="package-summary.html">BulkLoad package documentation</a> for complete
 * single-process and distributed usage examples.
 */
@PublicEvolving
@ThreadSafe
public final class BulkLoadClient {

    private final Configuration conf;
    private final BulkLoadTransactionDriver driver;

    /** Internal constructor; obtain a client from a Fluss connection. */
    @Internal
    public BulkLoadClient(
            Configuration conf, RpcClient rpcClient, MetadataUpdater metadataUpdater) {
        this.conf = checkNotNull(conf, "Client configuration must not be null.");
        this.driver =
                new BulkLoadTransactionDriver(
                        checkNotNull(rpcClient, "RPC client must not be null."),
                        checkNotNull(metadataUpdater, "Metadata updater must not be null."));
    }

    /**
     * Begins a new BulkLoad transaction.
     *
     * <p>A successful call always returns the frozen context for producing bucket files. If the
     * request outcome is uncertain, use {@link #getInProgressBulkLoad} before deciding whether to
     * retry the workflow.
     *
     * @return the frozen build context of the newly begun transaction
     */
    public BulkLoadBuildContext begin(
            PhysicalTablePath target, @Nullable Duration buildTimeout, Duration awaitTimeout)
            throws Exception {
        return begin(Collections.singletonList(target), buildTimeout, awaitTimeout);
    }

    /** Begins one transaction covering a fixed set of partitions of the same table. */
    public BulkLoadBuildContext begin(
            Collection<PhysicalTablePath> targets,
            @Nullable Duration buildTimeout,
            Duration awaitTimeout)
            throws Exception {
        return driver.begin(targets, buildTimeout, awaitTimeout);
    }

    /**
     * Returns the accessible in-progress transaction for the target.
     *
     * <p>This query does not change transaction state. Terminal transactions are not returned.
     */
    public Optional<BulkLoadStatus> getInProgressBulkLoad(PhysicalTablePath target)
            throws Exception {
        return driver.getInProgressBulkLoad(target);
    }

    /**
     * Opens the managed build session for a context returned by {@link #begin}.
     *
     * @throws Exception if the build session cannot be opened
     */
    public BulkLoadBuildSession openBuildSession(BulkLoadBuildContext context) throws Exception {
        List<String> configuredDirs = conf.get(ConfigOptions.CLIENT_BULK_LOAD_LOCAL_DIRS);
        List<File> localRootDirs = new ArrayList<>(configuredDirs.size());
        for (String dir : configuredDirs) {
            localRootDirs.add(new File(dir));
        }
        return new BulkLoadBuildSession(context, localRootDirs);
    }

    /** Commits the complete set of bucket results atomically. */
    public BulkLoadStatus commit(
            BulkLoadBuildContext context,
            Collection<BulkLoadBucketFiles> bucketFiles,
            Duration awaitTimeout)
            throws Exception {
        return driver.commitUntilReady(
                context.getHandle(), inputDigest(context, bucketFiles), awaitTimeout);
    }

    static String inputDigest(
            BulkLoadBuildContext context, Collection<BulkLoadBucketFiles> bucketFiles) {
        checkNotNull(context, "BulkLoad build context must not be null.");
        Map<TableBucket, BulkLoadBucketFiles> byBucket = new HashMap<>();
        for (BulkLoadBucketFiles files : bucketFiles) {
            checkArgument(
                    context.getHandle().getBulkLoadId().equals(files.getBulkLoadId()),
                    "BulkLoad bucket belongs to another transaction.");
            TableBucket bucket =
                    new TableBucket(
                            context.getHandle().getTableId(),
                            files.getPartitionId(),
                            files.getBucketId());
            checkArgument(
                    byBucket.put(bucket, files) == null,
                    "BulkLoad must cover every bucket without duplicates.");
        }
        Hasher digest = Hashing.sha256().newHasher();
        for (BulkLoadBuildContext.Target target : context.getTargets()) {
            for (int bucketId = 0; bucketId < target.getTableInfo().getNumBuckets(); bucketId++) {
                BulkLoadBucketFiles files =
                        byBucket.remove(
                                new TableBucket(
                                        context.getHandle().getTableId(),
                                        target.getPartitionId(),
                                        bucketId));
                checkArgument(files != null, "BulkLoad must cover every target bucket.");
                digest.putBytes(HashCode.fromString(files.getMetadataSha256()).asBytes());
            }
        }
        checkArgument(byBucket.isEmpty(), "BulkLoad contains unexpected buckets.");
        return digest.hash().toString();
    }

    /** Aborts the transaction and returns its persisted terminal status. */
    public BulkLoadStatus abort(BulkLoadHandle handle) throws Exception {
        return driver.abort(handle);
    }
}
