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

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Checks and creation must share their result, including the original failure. */
class DynamicPartitionCreatorTest {
    private static final TablePath TABLE_PATH = TablePath.of("db", "table");
    private static final PhysicalTablePath PATH = PhysicalTablePath.of(TABLE_PATH, "p");
    private static final TableInfo TABLE =
            TableInfo.of(
                    TABLE_PATH,
                    1L,
                    1,
                    TableDescriptor.builder()
                            .schema(Schema.newBuilder().column("dt", DataTypes.STRING()).build())
                            .partitionedBy("dt")
                            .distributedBy(1)
                            .build(),
                    "",
                    1L,
                    1L);

    @Test
    void testConcurrentCallersShareCheckAndCreateResult() {
        MetadataUpdater updater = mock(MetadataUpdater.class);
        Admin admin = mock(Admin.class);
        CompletableFuture<Cluster> metadata = new CompletableFuture<>();
        CompletableFuture<Void> creation = new CompletableFuture<>();
        when(updater.getPartitionId(PATH)).thenReturn(Optional.empty());
        when(updater.ensurePartitionMetadataAsync(PATH)).thenReturn(metadata);
        when(admin.createPartition(eq(TABLE_PATH), any(), eq(true))).thenReturn(creation);
        AtomicReference<Throwable> fatal = new AtomicReference<>();
        DynamicPartitionCreator creator =
                new DynamicPartitionCreator(updater, admin, true, fatal::set, 10000);
        CompletableFuture<Void> first = creator.checkAndCreatePartitionAsync(PATH, TABLE);
        assertThat(creator.checkAndCreatePartitionAsync(PATH, TABLE)).isSameAs(first);
        verify(admin, never()).createPartition(any(), any(), eq(true));
        metadata.completeExceptionally(new PartitionNotExistException("missing"));
        assertThat(first).isNotDone();
        verify(admin).createPartition(eq(TABLE_PATH), any(), eq(true));
        IllegalStateException error = new IllegalStateException("creation failed");
        creation.completeExceptionally(error);
        assertThatThrownBy(first::join).hasCause(error);
        assertThat(fatal).hasValue(error);
    }

    @Test
    void testFailureCallbackCanStartAnotherCheck() {
        MetadataUpdater updater = mock(MetadataUpdater.class);
        CompletableFuture<Cluster> metadata = new CompletableFuture<>();
        when(updater.getPartitionId(PATH)).thenReturn(Optional.empty());
        when(updater.ensurePartitionMetadataAsync(PATH))
                .thenReturn(metadata, CompletableFuture.completedFuture(Cluster.empty()));
        DynamicPartitionCreator creator =
                new DynamicPartitionCreator(
                        updater, mock(Admin.class), false, ignored -> {}, 10000);
        CompletableFuture<Void> first = creator.checkAndCreatePartitionAsync(PATH, TABLE);
        AtomicReference<CompletableFuture<Void>> retry = new AtomicReference<>();
        first.whenComplete(
                (ignored, error) -> retry.set(creator.checkAndCreatePartitionAsync(PATH, TABLE)));
        metadata.completeExceptionally(new IllegalStateException("unavailable"));
        assertThat(retry.get()).isNotSameAs(first).isCompleted();
    }

    @Test
    void testTimeoutDoesNotCreatePartitionAfterLateMetadataResponse() throws Exception {
        MetadataUpdater updater = mock(MetadataUpdater.class);
        Admin admin = mock(Admin.class);
        CompletableFuture<Cluster> metadata = new CompletableFuture<>();
        when(updater.getPartitionId(PATH)).thenReturn(Optional.empty());
        when(updater.ensurePartitionMetadataAsync(PATH)).thenReturn(metadata);
        DynamicPartitionCreator creator =
                new DynamicPartitionCreator(updater, admin, true, ignored -> {}, 50);
        CompletableFuture<Void> result = creator.checkAndCreatePartitionAsync(PATH, TABLE);
        assertThatThrownBy(() -> result.get(10, TimeUnit.SECONDS))
                .hasCauseInstanceOf(java.util.concurrent.TimeoutException.class);
        metadata.completeExceptionally(new PartitionNotExistException("missing"));
        verify(admin, never()).createPartition(any(), any(), eq(true));
    }

    @Test
    void testDisabledCreationPropagatesMissingPartition() {
        MetadataUpdater updater = mock(MetadataUpdater.class);
        Admin admin = mock(Admin.class);
        CompletableFuture<Cluster> metadata = new CompletableFuture<>();
        PartitionNotExistException error = new PartitionNotExistException("missing");
        metadata.completeExceptionally(error);
        when(updater.getPartitionId(PATH)).thenReturn(Optional.empty());
        when(updater.ensurePartitionMetadataAsync(PATH)).thenReturn(metadata);
        DynamicPartitionCreator creator =
                new DynamicPartitionCreator(updater, admin, false, ignored -> {}, 10000);
        assertThatThrownBy(() -> creator.checkAndCreatePartitionAsync(PATH, TABLE).join())
                .hasCause(error);
        verify(admin, never()).createPartition(any(), any(), eq(true));
    }
}
