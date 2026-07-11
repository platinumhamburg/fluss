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

package org.apache.fluss.flink.action.orphan.job;

import org.apache.fluss.annotation.Internal;

import java.util.concurrent.atomic.AtomicLong;

/** Compact counters describing the cleanup scope and the tasks emitted by stage 1. */
@Internal
public final class ScopePlanStats {

    private final AtomicLong databases = new AtomicLong();
    private final AtomicLong tables = new AtomicLong();
    private final AtomicLong partitions = new AtomicLong();
    private final AtomicLong discoveredBuckets = new AtomicLong();
    private final AtomicLong bucketTasks = new AtomicLong();
    private final AtomicLong orphanDirTasks = new AtomicLong();
    private final AtomicLong skippedNoRemoteManifest = new AtomicLong();
    private final AtomicLong skippedEmptyKvActiveSet = new AtomicLong();
    private final AtomicLong skippedOutOfScopeRoot = new AtomicLong();
    private final AtomicLong metadataFailures = new AtomicLong();

    public void database() {
        databases.incrementAndGet();
    }

    public void table() {
        tables.incrementAndGet();
    }

    public void partition() {
        partitions.incrementAndGet();
    }

    public void discoveredBucket() {
        discoveredBuckets.incrementAndGet();
    }

    public void bucketTask() {
        bucketTasks.incrementAndGet();
    }

    public void orphanDirTask() {
        orphanDirTasks.incrementAndGet();
    }

    public void skippedNoRemoteManifest() {
        skippedNoRemoteManifest.incrementAndGet();
    }

    public void skippedEmptyKvActiveSet() {
        skippedEmptyKvActiveSet.incrementAndGet();
    }

    public void skippedOutOfScopeRoot() {
        skippedOutOfScopeRoot.incrementAndGet();
    }

    public void metadataFailure() {
        metadataFailures.incrementAndGet();
    }

    public long databases() {
        return databases.get();
    }

    public long tables() {
        return tables.get();
    }

    public long partitions() {
        return partitions.get();
    }

    public long discoveredBuckets() {
        return discoveredBuckets.get();
    }

    public long bucketTasks() {
        return bucketTasks.get();
    }

    public long orphanDirTasks() {
        return orphanDirTasks.get();
    }

    public long skippedNoRemoteManifestCount() {
        return skippedNoRemoteManifest.get();
    }

    public long skippedEmptyKvActiveSetCount() {
        return skippedEmptyKvActiveSet.get();
    }

    public long skippedOutOfScopeRootCount() {
        return skippedOutOfScopeRoot.get();
    }

    public long metadataFailures() {
        return metadataFailures.get();
    }
}
