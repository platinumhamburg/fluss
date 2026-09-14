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

package org.apache.fluss.client.write;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.utils.concurrent.FutureUtils;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.fluss.utils.ExceptionUtils.stripCompletionException;
import static org.apache.fluss.utils.PartitionUtils.validateAutoPartitionTime;

/** Creates missing partitions, sharing each in-flight check and create request among callers. */
@ThreadSafe
@Internal
public class DynamicPartitionCreator {
    private final MetadataUpdater metadataUpdater;
    private final Admin admin;
    private final long timeoutMs;
    private final boolean dynamicPartitionEnabled;
    private final Consumer<Throwable> fatalErrorHandler;
    private final Map<PhysicalTablePath, CompletableFuture<Void>> inflightPartitionsToCreate =
            new ConcurrentHashMap<>();

    /** Creates a partition creator with a timeout for each shared check-and-create operation. */
    public DynamicPartitionCreator(
            MetadataUpdater metadataUpdater,
            Admin admin,
            boolean dynamicPartitionEnabled,
            Consumer<Throwable> fatalErrorHandler,
            long timeoutMs) {
        this.metadataUpdater = metadataUpdater;
        this.admin = admin;
        this.dynamicPartitionEnabled = dynamicPartitionEnabled;
        this.fatalErrorHandler = fatalErrorHandler;
        this.timeoutMs = timeoutMs;
    }

    /**
     * Ensures the partition exists. Success does not guarantee that its routing metadata is
     * available. Failed checks, validation and creation complete the returned future exceptionally.
     */
    public CompletableFuture<Void> checkAndCreatePartitionAsync(
            PhysicalTablePath path, TableInfo tableInfo) {
        if (path.getPartitionName() == null || metadataUpdater.getPartitionId(path).isPresent()) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> result = new CompletableFuture<>();
        while (true) {
            CompletableFuture<Void> existing = inflightPartitionsToCreate.putIfAbsent(path, result);
            if (existing == null) {
                break;
            }
            if (!existing.isDone()) {
                return existing;
            }
            inflightPartitionsToCreate.remove(path, existing);
        }
        FutureUtils.orTimeout(
                result,
                timeoutMs,
                TimeUnit.MILLISECONDS,
                "Timed out checking or creating partition " + path);
        result.whenComplete((ignored, error) -> inflightPartitionsToCreate.remove(path, result));
        try {
            metadataUpdater
                    .ensurePartitionMetadataAsync(path)
                    .handle(
                            (snapshot, error) -> {
                                if (result.isDone()) {
                                    return result;
                                }
                                if (error == null) {
                                    return CompletableFuture.<Void>completedFuture(null);
                                }
                                Throwable cause = stripCompletionException(error);
                                if (!(cause instanceof PartitionNotExistException)
                                        || !dynamicPartitionEnabled) {
                                    CompletableFuture<Void> failed = new CompletableFuture<>();
                                    failed.completeExceptionally(cause);
                                    return failed;
                                }
                                ResolvedPartitionSpec spec =
                                        ResolvedPartitionSpec.fromPartitionName(
                                                tableInfo.getPartitionKeys(),
                                                path.getPartitionName());
                                validateAutoPartitionTime(
                                        spec.toPartitionSpec(),
                                        tableInfo.getPartitionKeys(),
                                        tableInfo.getTableConfig().getAutoPartitionStrategy());
                                return admin.createPartition(
                                                path.getTablePath(), spec.toPartitionSpec(), true)
                                        .whenComplete(
                                                (ignored, failure) -> {
                                                    if (failure != null && !result.isDone()) {
                                                        fatalErrorHandler.accept(
                                                                stripCompletionException(failure));
                                                    }
                                                });
                            })
                    .thenCompose(future -> future)
                    .whenComplete((ignored, error) -> finish(result, error));
        } catch (Throwable error) {
            finish(result, error);
        }
        return result;
    }

    private void finish(CompletableFuture<Void> result, Throwable error) {
        if (error == null) {
            result.complete(null);
        } else {
            Throwable cause = stripCompletionException(error);
            result.completeExceptionally(cause);
        }
    }
}
