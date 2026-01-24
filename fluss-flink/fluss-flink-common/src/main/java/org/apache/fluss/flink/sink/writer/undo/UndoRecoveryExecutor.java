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

package org.apache.fluss.flink.sink.writer.undo;

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.ByteArrayWrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Executes undo recovery for multiple buckets using streaming execution.
 *
 * <p>This executor manages:
 *
 * <ul>
 *   <li>Single LogScanner with multiple bucket subscriptions
 *   <li>Changelog reading with immediate undo operation execution
 *   <li>Async write operations via UpsertWriter
 * </ul>
 *
 * <p>The execution flow (streaming):
 *
 * <ol>
 *   <li>Subscribe all buckets to the LogScanner
 *   <li>Poll changelog records and execute undo operations immediately
 *   <li>Collect CompletableFutures for completion tracking
 *   <li>Wait for all futures and flush to ensure all writes complete
 * </ol>
 */
public class UndoRecoveryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(UndoRecoveryExecutor.class);

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);
    private static final int MAX_EMPTY_POLLS = 10;

    private final LogScanner scanner;
    private final UpsertWriter writer;
    private final UndoComputer undoComputer;

    public UndoRecoveryExecutor(
            LogScanner scanner, UpsertWriter writer, UndoComputer undoComputer) {
        this.scanner = scanner;
        this.writer = writer;
        this.undoComputer = undoComputer;
    }

    /**
     * Executes undo recovery for the given bucket contexts.
     *
     * @param contexts the bucket recovery contexts (must have target offsets set)
     * @throws Exception if recovery fails
     */
    public void execute(List<BucketRecoveryContext> contexts) throws Exception {
        // Filter contexts that need recovery
        List<BucketRecoveryContext> toRecover = filterContextsNeedingRecovery(contexts);
        if (toRecover.isEmpty()) {
            LOG.info("No buckets need recovery");
            return;
        }

        LOG.info("Starting undo recovery for {} bucket(s)", toRecover.size());

        // Subscribe and read changelog with streaming execution
        subscribeAll(toRecover);
        List<CompletableFuture<?>> allFutures = readChangelogAndExecute(toRecover);

        // Wait for all async writes to complete
        if (!allFutures.isEmpty()) {
            CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[0])).get();
        }
        writer.flush();

        // Log summary
        int totalUndoOps = 0;
        for (BucketRecoveryContext ctx : toRecover) {
            totalUndoOps += ctx.getProcessedKeys().size();
        }

        LOG.info(
                "Completed undo recovery: {} bucket(s), {} total undo operation(s)",
                toRecover.size(),
                totalUndoOps);
    }

    private List<BucketRecoveryContext> filterContextsNeedingRecovery(
            List<BucketRecoveryContext> contexts) {
        List<BucketRecoveryContext> result = new ArrayList<>();
        for (BucketRecoveryContext ctx : contexts) {
            if (ctx.needsRecovery()) {
                LOG.debug(
                        "Bucket {} needs recovery: checkpoint={}, target={}",
                        ctx.getBucket(),
                        ctx.getCheckpointOffset(),
                        ctx.getTargetOffset());
                result.add(ctx);
            } else {
                LOG.debug("Bucket {} already up-to-date, no recovery needed", ctx.getBucket());
            }
        }
        return result;
    }

    private void subscribeAll(List<BucketRecoveryContext> contexts) {
        for (BucketRecoveryContext ctx : contexts) {
            TableBucket bucket = ctx.getBucket();
            if (bucket.getPartitionId() != null) {
                scanner.subscribe(
                        bucket.getPartitionId(), bucket.getBucket(), ctx.getCheckpointOffset());
            } else {
                scanner.subscribe(bucket.getBucket(), ctx.getCheckpointOffset());
            }
        }
    }

    private List<CompletableFuture<?>> readChangelogAndExecute(
            List<BucketRecoveryContext> contexts) {
        List<CompletableFuture<?>> allFutures = new ArrayList<>();

        // Early exit if all contexts are already complete (no records to read)
        if (allComplete(contexts)) {
            LOG.debug("All buckets already complete, no changelog reading needed");
            return allFutures;
        }

        int emptyPollCount = 0;

        while (!allComplete(contexts)) {
            ScanRecords records = scanner.poll(POLL_TIMEOUT);

            if (records.isEmpty()) {
                emptyPollCount++;
                if (emptyPollCount >= MAX_EMPTY_POLLS) {
                    LOG.warn(
                            "Stopping scan after {} consecutive empty polls, {} bucket(s) may be incomplete",
                            MAX_EMPTY_POLLS,
                            countIncomplete(contexts));
                    break;
                }
                continue;
            }

            emptyPollCount = 0;

            // Process records for each bucket
            for (BucketRecoveryContext ctx : contexts) {
                if (ctx.isComplete()) {
                    continue;
                }
                List<ScanRecord> bucketRecords = records.records(ctx.getBucket());
                if (!bucketRecords.isEmpty()) {
                    processRecords(ctx, bucketRecords, allFutures);
                }
            }
        }

        // Log summary
        for (BucketRecoveryContext ctx : contexts) {
            LOG.debug(
                    "Bucket {} read {} records, executed {} undo operations",
                    ctx.getBucket(),
                    ctx.getTotalRecordsProcessed(),
                    ctx.getProcessedKeys().size());
        }

        return allFutures;
    }

    private void processRecords(
            BucketRecoveryContext ctx,
            List<ScanRecord> records,
            List<CompletableFuture<?>> futures) {
        Set<ByteArrayWrapper> processedKeys = ctx.getProcessedKeys();
        for (ScanRecord record : records) {
            CompletableFuture<?> future = undoComputer.processRecord(record, processedKeys);
            if (future != null) {
                futures.add(future);
            }
            ctx.recordProcessed(record.logOffset());
            if (ctx.isComplete()) {
                break;
            }
        }
    }

    private boolean allComplete(List<BucketRecoveryContext> contexts) {
        for (BucketRecoveryContext ctx : contexts) {
            if (!ctx.isComplete()) {
                return false;
            }
        }
        return true;
    }

    private int countIncomplete(List<BucketRecoveryContext> contexts) {
        int count = 0;
        for (BucketRecoveryContext ctx : contexts) {
            if (!ctx.isComplete()) {
                count++;
            }
        }
        return count;
    }
}
