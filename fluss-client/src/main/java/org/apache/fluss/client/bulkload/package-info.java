/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Public client API for atomically loading the final contents of an empty primary-key table or
 * partition.
 *
 * <h2>Entry point</h2>
 *
 * <p>Obtain the single entry point from {@link org.apache.fluss.client.Connection}:
 *
 * <pre>{@code
 * BulkLoadClient client = connection.getBulkLoadClient();
 * }</pre>
 *
 * <p>The package contains four public concepts:
 *
 * <ul>
 *   <li>{@link org.apache.fluss.client.bulkload.BulkLoadClient} begins, commits, and aborts a load;
 *   <li>{@link org.apache.fluss.client.bulkload.BulkLoadBuildContext} is the immutable,
 *       serializable build contract returned by Begin;
 *   <li>{@link org.apache.fluss.client.bulkload.BulkLoadBucketWriter} builds the final files for
 *       exactly one bucket; and
 *   <li>{@link org.apache.fluss.client.bulkload.BulkLoadBucketFiles} is the opaque, serializable
 *       file description returned by a finished bucket writer and consumed by Commit.
 * </ul>
 *
 * <h2>Single-process client</h2>
 *
 * <p>A direct client begins one transaction, builds every bucket, and submits all bucket files in
 * one Commit. The application must route each row with {@link
 * org.apache.fluss.client.bulkload.BulkLoadBuildContext#bucketOf}; each bucket writer validates
 * that contract and rejects a row for another bucket instead of repartitioning it.
 *
 * <pre>{@code
 * BulkLoadClient client = connection.getBulkLoadClient();
 * BulkLoadBuildContext context = client.begin(target, buildTimeout, awaitTimeout);
 * List<BulkLoadBucketFiles> buckets = new ArrayList<>();
 * try {
 *     for (int bucketId = 0;
 *             bucketId < context.getTableInfo().getNumBuckets();
 *             bucketId++) {
 *         try (BulkLoadBucketWriter writer =
 *                 new BulkLoadBucketWriter(context, bucketId, workDir)) {
 *             for (InternalRow row : rowsByBucket.get(bucketId)) {
 *                 writer.add(row);
 *             }
 *             buckets.add(writer.finish());
 *         }
 *     }
 * } catch (Exception buildFailure) {
 *     try {
 *         client.abort(context.getHandle());
 *     } catch (Exception abortFailure) {
 *         buildFailure.addSuppressed(abortFailure);
 *     }
 *     throw buildFailure;
 * }
 * BulkLoadStatus status = client.commit(context, buckets, awaitTimeout);
 * }</pre>
 *
 * <p>Begin is a one-shot operation. When its outcome is uncertain, call {@link
 * org.apache.fluss.client.bulkload.BulkLoadClient#getInProgressBulkLoad} before starting another
 * transaction. A visible {@code BEGUN} transaction may be aborted with its exact handle before a
 * fresh Begin. A visible {@code COMMITTING} transaction has a durable Commit decision and must not
 * be aborted. Commit already handles an unknown or retriable Commit result by joining that durable
 * decision.
 *
 * <p>When an upstream system supplies each bucket's non-negative log end boundary {@code E_b}, the
 * same public lifecycle uses {@link
 * org.apache.fluss.client.bulkload.BulkLoadBucketWriter#finishAtLogEndOffset(long)}:
 *
 * <pre>{@code
 * BulkLoadBuildContext context = client.begin(target, buildTimeout, awaitTimeout);
 * List<BulkLoadBucketFiles> buckets = new ArrayList<>();
 * for (int bucketId = 0; bucketId < context.getTableInfo().getNumBuckets(); bucketId++) {
 *     try (BulkLoadBucketWriter writer =
 *             new BulkLoadBucketWriter(context, bucketId, workDir)) {
 *         for (InternalRow row : rowsByBucket.get(bucketId)) {
 *             writer.add(row);
 *         }
 *         long E_b = logEndOffsets.get(bucketId);
 *         buckets.add(writer.finishAtLogEndOffset(E_b));
 *     }
 * }
 * client.commit(context, buckets, awaitTimeout);
 * }</pre>
 *
 * <h2>Flink jobs and other distributed engines</h2>
 *
 * <p>Distributed engines use the same API without sharing a writer between processes:
 *
 * <ol>
 *   <li>one coordinator task calls {@code begin};
 *   <li>the serializable {@code BulkLoadBuildContext} is broadcast to build tasks;
 *   <li>rows are partitioned by Fluss bucket, with one {@code BulkLoadBucketWriter} owning each
 *       bucket and its local RocksDB state;
 *   <li>each build task emits one serializable {@code BulkLoadBucketFiles}; and
 *   <li>one committer collects the complete bucket set and calls {@code commit}.
 * </ol>
 *
 * <p>Flink SQL users normally do not call these classes directly. In batch mode, the Fluss
 * connector constructs this topology when BulkLoad is enabled:
 *
 * <pre>{@code
 * SET 'execution.runtime-mode' = 'batch';
 *
 * INSERT INTO target_table /*+ OPTIONS('sink.bulk-load.enabled' = 'true') *\/
 * SELECT * FROM source_table;
 * }</pre>
 *
 * <p>The Flink Begin operator first queries the target. It starts a transaction when none is in
 * progress, or aborts a visible {@code BEGUN} transaction before starting a fresh one. It fails on
 * a visible {@code COMMITTING} transaction because that durable decision cannot be aborted. Every
 * successful Begin operator invocation broadcasts exactly one build context.
 */
package org.apache.fluss.client.bulkload;
