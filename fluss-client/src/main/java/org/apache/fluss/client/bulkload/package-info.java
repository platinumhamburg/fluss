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
 * Client support for initializing new, unopened primary-key tables or partitions from KV snapshots.
 *
 * <p>The caller serializes initialization of each physical target and keeps ordinary access closed
 * until Commit succeeds. Begin records the transaction metadata without waiting for replicas. A
 * fixed set of partitions of the same table shares one transaction and one Commit decision.
 *
 * <pre>{@code
 * BulkLoadClient client = connection.getBulkLoadClient();
 * BulkLoadBuildContext context = client.begin(targets, buildTimeout, awaitTimeout);
 * List<BulkLoadBucketFiles> files = new ArrayList<>();
 * try (BulkLoadBuildSession build = client.openBuildSession(context)) {
 *     for (BulkLoadBuildContext.Target target : context.getTargets()) {
 *         for (int bucket = 0; bucket < target.getTableInfo().getNumBuckets(); bucket++) {
 *             try (BulkLoadBucketWriter writer = build.createBucketWriter(target.getPath(), bucket)) {
 *                 for (InternalRow row : rowsByTargetAndBucket.get(target.getPath()).get(bucket)) {
 *                     writer.add(row);
 *                 }
 *                 files.add(writer.finish());
 *             }
 *         }
 *     }
 * }
 * client.commit(context, files, awaitTimeout);
 * }</pre>
 *
 * <p>Route rows using {@link
 * org.apache.fluss.client.bulkload.BulkLoadBuildContext.Target#bucketOf}. Writers reject rows
 * belonging to another bucket. Each writer owns a unique temporary directory below a configured
 * {@code client.bulk-load.local.dirs} root. Closing a writer or its session releases its RocksDB
 * resources and deletes that temporary directory.
 *
 * <p>The build context and bucket results can be serialized between distributed tasks. Only one
 * attempt may upload a target bucket at a time. Stop and close builders before Abort or a
 * replacement attempt. The caller owns cleanup of uploaded files that were not adopted by Commit.
 *
 * <p>Begin is issued once. If its response is lost, {@code getInProgressBulkLoad} discovers the
 * handle and state without reconstructing the build context. Stop old builders before aborting a
 * BEGUN transaction and starting again. A COMMITTING transaction must finish its durable decision.
 * An unknown Commit result may be retried with the same context and frozen bucket results.
 *
 * <p>{@link org.apache.fluss.client.bulkload.BulkLoadBucketWriter#finishAtLogEndOffset(long)}
 * accepts an explicit upstream log boundary. Otherwise {@code finish()} uses the final number of
 * distinct primary keys as the boundary. Commit confirms restoration by each current bucket leader.
 */
package org.apache.fluss.client.bulkload;
