/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */

package org.apache.fluss.server.zk.data.bulkload;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.BulkLoadAbortReason;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.BulkLoadState;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerializer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Strict deterministic JSON serde for {@link BulkLoadTransaction}. */
@Internal
public final class BulkLoadTransactionJsonSerde
        implements JsonSerializer<BulkLoadTransaction>, JsonDeserializer<BulkLoadTransaction> {

    public static final BulkLoadTransactionJsonSerde INSTANCE = new BulkLoadTransactionJsonSerde();
    private static final int VERSION = 1;

    private static final String VERSION_KEY = "version";
    private static final String BULK_LOAD_ID = "bulk_load_id";
    private static final String STATE = "state";
    private static final String CALLER_TOKEN = "caller_token";
    private static final String DATABASE_NAME = "database_name";
    private static final String TABLE_NAME = "table_name";
    private static final String PARTITION_NAME = "partition_name";
    private static final String TABLE_ID = "table_id";
    private static final String PARTITION_ID = "partition_id";
    private static final String CREATOR_NAME = "creator_name";
    private static final String CREATOR_TYPE = "creator_type";
    private static final String REMOTE_DATA_DIR = "remote_data_dir";
    private static final String SCHEMA_ID = "schema_id";
    private static final String METADATA_PATH = "metadata_path";
    private static final String METADATA_VERSION = "metadata_version";
    private static final String SNAPSHOT_IDS = "snapshot_ids";
    private static final String CREATED_TIME_MS = "created_time_ms";
    private static final String UPDATED_TIME_MS = "updated_time_ms";
    private static final String BUILD_DEADLINE_MS = "build_deadline_ms";
    private static final String COMMIT_DECISION_DEADLINE_MS = "commit_decision_deadline_ms";
    private static final String RESULT_EXPIRE_TIME_MS = "result_expire_time_ms";
    private static final String MANIFEST_PATH = "manifest_path";
    private static final String MANIFEST_LENGTH = "manifest_length";
    private static final String MANIFEST_SHA256 = "manifest_sha256";
    private static final String ABORT_REASON = "abort_reason";
    private static final String ABORT_MESSAGE = "abort_message";

    private static final Set<String> OPTIONAL_FIELDS =
            setOf(
                    PARTITION_NAME,
                    PARTITION_ID,
                    SNAPSHOT_IDS,
                    COMMIT_DECISION_DEADLINE_MS,
                    RESULT_EXPIRE_TIME_MS,
                    MANIFEST_PATH,
                    MANIFEST_LENGTH,
                    MANIFEST_SHA256,
                    ABORT_REASON,
                    ABORT_MESSAGE);
    private static final Set<String> ALL_FIELDS =
            setOf(
                    VERSION_KEY,
                    BULK_LOAD_ID,
                    STATE,
                    CALLER_TOKEN,
                    DATABASE_NAME,
                    TABLE_NAME,
                    PARTITION_NAME,
                    TABLE_ID,
                    PARTITION_ID,
                    CREATOR_NAME,
                    CREATOR_TYPE,
                    REMOTE_DATA_DIR,
                    SCHEMA_ID,
                    METADATA_PATH,
                    METADATA_VERSION,
                    SNAPSHOT_IDS,
                    CREATED_TIME_MS,
                    UPDATED_TIME_MS,
                    BUILD_DEADLINE_MS,
                    COMMIT_DECISION_DEADLINE_MS,
                    RESULT_EXPIRE_TIME_MS,
                    MANIFEST_PATH,
                    MANIFEST_LENGTH,
                    MANIFEST_SHA256,
                    ABORT_REASON,
                    ABORT_MESSAGE);

    @Override
    public void serialize(BulkLoadTransaction transaction, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, VERSION);
        generator.writeStringField(BULK_LOAD_ID, transaction.getBulkLoadId());
        generator.writeNumberField(STATE, transaction.getState().getCode());
        generator.writeStringField(CALLER_TOKEN, transaction.getCallerToken());
        generator.writeStringField(DATABASE_NAME, transaction.getDatabaseName());
        generator.writeStringField(TABLE_NAME, transaction.getTableName());
        if (transaction.getPartitionName() != null) {
            generator.writeStringField(PARTITION_NAME, transaction.getPartitionName());
        }
        generator.writeNumberField(TABLE_ID, transaction.getTableId());
        if (transaction.getPartitionId() != null) {
            generator.writeNumberField(PARTITION_ID, transaction.getPartitionId());
        }
        generator.writeStringField(CREATOR_NAME, transaction.getCreatorName());
        generator.writeStringField(CREATOR_TYPE, transaction.getCreatorType());
        generator.writeStringField(REMOTE_DATA_DIR, transaction.getRemoteDataDir());
        generator.writeNumberField(SCHEMA_ID, transaction.getSchemaId());
        generator.writeStringField(METADATA_PATH, transaction.getMetadataPath());
        generator.writeNumberField(METADATA_VERSION, transaction.getMetadataVersion());
        long[] snapshotIds = transaction.getSnapshotIds();
        if (snapshotIds != null) {
            generator.writeArrayFieldStart(SNAPSHOT_IDS);
            for (long snapshotId : snapshotIds) {
                generator.writeNumber(snapshotId);
            }
            generator.writeEndArray();
        }
        generator.writeNumberField(CREATED_TIME_MS, transaction.getCreatedTimeMs());
        generator.writeNumberField(UPDATED_TIME_MS, transaction.getUpdatedTimeMs());
        generator.writeNumberField(BUILD_DEADLINE_MS, transaction.getBuildDeadlineMs());
        if (transaction.getCommitDecisionDeadlineMs() != null) {
            generator.writeNumberField(
                    COMMIT_DECISION_DEADLINE_MS, transaction.getCommitDecisionDeadlineMs());
        }
        if (transaction.getResultExpireTimeMs() != null) {
            generator.writeNumberField(RESULT_EXPIRE_TIME_MS, transaction.getResultExpireTimeMs());
        }
        if (transaction.getManifestPath() != null) {
            generator.writeStringField(MANIFEST_PATH, transaction.getManifestPath());
            generator.writeNumberField(MANIFEST_LENGTH, transaction.getManifestLength());
            generator.writeStringField(MANIFEST_SHA256, transaction.getManifestSha256());
        }
        if (transaction.getAbortReason() != null) {
            generator.writeNumberField(ABORT_REASON, transaction.getAbortReason().getCode());
        }
        if (transaction.getAbortMessage() != null) {
            generator.writeStringField(ABORT_MESSAGE, transaction.getAbortMessage());
        }
        generator.writeEndObject();
    }

    @Override
    public BulkLoadTransaction deserialize(JsonNode node) {
        requireObject(node, "BulkLoad transaction");
        validateFields(
                node, ALL_FIELDS, requiredFields(ALL_FIELDS, OPTIONAL_FIELDS), "transaction");
        readVersion(node, "BulkLoad transaction", VERSION);
        BulkLoadHandle handle =
                new BulkLoadHandle(
                        PhysicalTablePath.of(
                                requiredString(node, DATABASE_NAME),
                                requiredString(node, TABLE_NAME),
                                optionalString(node, PARTITION_NAME)),
                        requiredLong(node, TABLE_ID),
                        optionalLong(node, PARTITION_ID),
                        requiredString(node, BULK_LOAD_ID));
        return new BulkLoadTransaction(
                handle,
                BulkLoadState.fromCode(requiredInt(node, STATE)),
                requiredString(node, CALLER_TOKEN),
                requiredString(node, CREATOR_NAME),
                requiredString(node, CREATOR_TYPE),
                requiredString(node, REMOTE_DATA_DIR),
                requiredInt(node, SCHEMA_ID),
                requiredString(node, METADATA_PATH),
                requiredInt(node, METADATA_VERSION),
                optionalSnapshotIds(node),
                requiredLong(node, CREATED_TIME_MS),
                requiredLong(node, UPDATED_TIME_MS),
                requiredLong(node, BUILD_DEADLINE_MS),
                optionalLong(node, COMMIT_DECISION_DEADLINE_MS),
                optionalLong(node, RESULT_EXPIRE_TIME_MS),
                optionalString(node, MANIFEST_PATH),
                optionalLong(node, MANIFEST_LENGTH),
                optionalString(node, MANIFEST_SHA256),
                node.has(ABORT_REASON)
                        ? BulkLoadAbortReason.fromCode(requiredInt(node, ABORT_REASON))
                        : null,
                optionalString(node, ABORT_MESSAGE));
    }

    static int readVersion(JsonNode node, String recordName, int expectedVersion) {
        int version = requiredInt(node, VERSION_KEY);
        checkArgument(
                version == expectedVersion, "Unsupported %s version %s.", recordName, version);
        return version;
    }

    static void requireObject(JsonNode node, String recordName) {
        checkArgument(node != null && node.isObject(), "%s must be a JSON object.", recordName);
    }

    static void validateFields(
            JsonNode node, Set<String> allowed, Set<String> required, String recordName) {
        Iterator<String> fields = node.fieldNames();
        while (fields.hasNext()) {
            String field = fields.next();
            checkArgument(
                    allowed.contains(field), "Unknown BulkLoad %s field %s.", recordName, field);
        }
        for (String field : required) {
            checkArgument(
                    node.has(field), "Missing required BulkLoad %s field %s.", recordName, field);
        }
    }

    static JsonNode requiredNode(JsonNode node, String field) {
        JsonNode value = node.get(field);
        checkArgument(value != null, "Missing required BulkLoad field %s.", field);
        return value;
    }

    static String requiredString(JsonNode node, String field) {
        JsonNode value = requiredNode(node, field);
        checkArgument(value.isTextual(), "BulkLoad field %s must be a string.", field);
        return value.textValue();
    }

    @Nullable
    static String optionalString(JsonNode node, String field) {
        return node.has(field) ? requiredString(node, field) : null;
    }

    static int requiredInt(JsonNode node, String field) {
        JsonNode value = requiredNode(node, field);
        checkArgument(
                value.isIntegralNumber() && value.canConvertToInt(),
                "BulkLoad field %s must be an in-range integer.",
                field);
        return value.intValue();
    }

    static long requiredLong(JsonNode node, String field) {
        JsonNode value = requiredNode(node, field);
        checkArgument(
                value.isIntegralNumber() && value.canConvertToLong(),
                "BulkLoad field %s must be an in-range long.",
                field);
        return value.longValue();
    }

    @Nullable
    static Long optionalLong(JsonNode node, String field) {
        return node.has(field) ? requiredLong(node, field) : null;
    }

    static boolean requiredBoolean(JsonNode node, String field) {
        JsonNode value = requiredNode(node, field);
        checkArgument(value.isBoolean(), "BulkLoad field %s must be a boolean.", field);
        return value.booleanValue();
    }

    @Nullable
    static long[] optionalSnapshotIds(JsonNode node) {
        if (!node.has(SNAPSHOT_IDS)) {
            return null;
        }
        JsonNode value = requiredNode(node, SNAPSHOT_IDS);
        checkArgument(value.isArray(), "BulkLoad field %s must be an array.", SNAPSHOT_IDS);
        checkArgument(value.size() > 0, "BulkLoad field %s must not be empty.", SNAPSHOT_IDS);
        long[] snapshotIds = new long[value.size()];
        for (int i = 0; i < value.size(); i++) {
            JsonNode snapshotId = value.get(i);
            checkArgument(
                    snapshotId.isIntegralNumber() && snapshotId.canConvertToLong(),
                    "BulkLoad field %s must contain in-range long values.",
                    SNAPSHOT_IDS);
            checkArgument(
                    snapshotId.longValue() >= 0,
                    "BulkLoad field %s must contain non-negative values.",
                    SNAPSHOT_IDS);
            snapshotIds[i] = snapshotId.longValue();
        }
        return snapshotIds;
    }

    static Set<String> setOf(String... fields) {
        return new HashSet<>(Arrays.asList(fields));
    }

    private static Set<String> requiredFields(Set<String> all, Set<String> optional) {
        Set<String> required = new HashSet<>(all);
        required.removeAll(optional);
        return required;
    }
}
