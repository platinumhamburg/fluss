/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */

package org.apache.fluss.server.bulkload;

import org.apache.fluss.metadata.BulkLoadAbortReason;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.BulkLoadState;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Stable tests for the minimal persisted BulkLoad facts. */
class BulkLoadMetadataTest {

    private static final String SHA256 =
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    @Test
    void testTransactionGoldenJsonRoundTrip() {
        BulkLoadTransaction transaction = committingTransaction(null);

        byte[] transactionBytes = ZkData.BulkLoadTableTransactionZNode.encode(transaction);

        assertThat(new String(transactionBytes, StandardCharsets.UTF_8))
                .isEqualTo(
                        "{\"version\":1,\"bulk_load_id\":\"550e8400-e29b-41d4-a716-446655440000\",\"state\":1,\"database_name\":\"db\",\"table_name\":\"table\",\"table_id\":41,\"creator_name\":\"alice\",\"creator_type\":\"USER\",\"remote_data_dir\":\"file:///warehouse\",\"schema_id\":3,\"metadata_path\":\"/metadata/db/table\",\"metadata_version\":7,\"snapshot_ids\":[17],\"created_time_ms\":100,\"updated_time_ms\":200,\"build_deadline_ms\":300,\"commit_decision_deadline_ms\":400,\"manifest_path\":\"file:///warehouse/bulkload/manifest.json\",\"manifest_length\":500,\"manifest_sha256\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"}");
        assertThat(ZkData.BulkLoadTableTransactionZNode.decode(transactionBytes))
                .isEqualTo(transaction);
    }

    @Test
    void testTransactionRejectsCoercedFields() {
        byte[] encoded = ZkData.BulkLoadTableTransactionZNode.encode(committingTransaction(null));
        assertThatThrownBy(
                        () ->
                                ZkData.BulkLoadTableTransactionZNode.decode(
                                        new String(encoded, StandardCharsets.UTF_8)
                                                .replace(
                                                        "\"snapshot_ids\":[17]",
                                                        "\"snapshot_ids\":[]")
                                                .getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("snapshot_ids");
        assertThatThrownBy(
                        () ->
                                ZkData.BulkLoadTableTransactionZNode.decode(
                                        new String(encoded, StandardCharsets.UTF_8)
                                                .replace(
                                                        "\"snapshot_ids\":[17]",
                                                        "\"snapshot_ids\":[-1]")
                                                .getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("snapshot_ids");
        assertThatThrownBy(
                        () ->
                                ZkData.BulkLoadTableTransactionZNode.decode(
                                        new String(encoded, StandardCharsets.UTF_8)
                                                .replace(
                                                        "\"snapshot_ids\":[17]",
                                                        "\"fence_ready\":true")
                                                .getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fence_ready");
    }

    @Test
    void testRejectsIllegalTransactionStateGroups() {
        assertThatThrownBy(
                        () ->
                                transaction(
                                        BulkLoadState.COMMITTING,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                transaction(
                                        BulkLoadState.ABORTED,
                                        null,
                                        null,
                                        null,
                                        null,
                                        500L,
                                        null,
                                        null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                transaction(
                                        BulkLoadState.BEGUN,
                                        new long[0],
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                transaction(
                                        BulkLoadState.BEGUN,
                                        new long[] {-1L},
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testAbortMessageIsBoundedWithoutSplittingUtf8() {
        String error = String.join("", Collections.nCopies(2000, "错误"));
        BulkLoadTransaction transaction =
                transaction(
                        BulkLoadState.ABORTED,
                        null,
                        null,
                        null,
                        null,
                        500L,
                        BulkLoadAbortReason.ABORTED_BY_CALLER,
                        error);

        assertThat(transaction.getAbortMessage()).isNotNull();
        assertThat(transaction.getAbortMessage().getBytes(StandardCharsets.UTF_8).length)
                .isLessThanOrEqualTo(4096);
        assertThat(transaction.getAbortMessage()).doesNotEndWith("�");
    }

    private static BulkLoadTransaction committingTransaction(String error) {
        return transaction(
                BulkLoadState.COMMITTING,
                new long[] {17L},
                "file:///warehouse/bulkload/manifest.json",
                500L,
                SHA256,
                null,
                null,
                error);
    }

    private static BulkLoadTransaction transaction(
            BulkLoadState state,
            long[] snapshotIds,
            String manifestPath,
            Long manifestLength,
            String manifestSha256,
            Long resultExpireTimeMs,
            BulkLoadAbortReason abortReason,
            String abortMessage) {
        return new BulkLoadTransaction(
                new BulkLoadHandle(
                        PhysicalTablePath.of("db", "table", null),
                        41L,
                        null,
                        "550e8400-e29b-41d4-a716-446655440000"),
                state,
                "alice",
                "USER",
                "file:///warehouse",
                3,
                "/metadata/db/table",
                7,
                snapshotIds,
                100L,
                200L,
                300L,
                manifestPath == null ? null : 400L,
                resultExpireTimeMs,
                manifestPath,
                manifestLength,
                manifestSha256,
                abortReason,
                abortMessage);
    }
}
