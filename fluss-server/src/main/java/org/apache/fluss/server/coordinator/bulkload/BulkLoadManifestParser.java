/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */

package org.apache.fluss.server.coordinator.bulkload;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.exception.InvalidBulkLoadRequestException;
import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.KvSnapshotFileMetadata;
import org.apache.fluss.metadata.KvSnapshotFileMetadataJsonSerde;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotJsonSerde;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonToken;
import org.apache.fluss.utils.FlussPaths;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/** Strict parser and small-metadata validator for the final outer manifest. */
@Internal
public final class BulkLoadManifestParser {

    private static final int VERSION = 1;
    private static final Pattern SHA256 = Pattern.compile("[0-9a-f]{64}");
    private final JsonFactory jsonFactory;

    public BulkLoadManifestParser() {
        jsonFactory = new JsonFactory();
        jsonFactory.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
    }

    /** Reads the exact manifest and validates all referenced standard metadata and file lengths. */
    public List<ValidatedBucket> parse(
            BulkLoadHandle handle,
            String remoteDataDir,
            String manifestPath,
            long manifestLength,
            String manifestSha256,
            long[] snapshotIds,
            ChangelogImage changelogImage,
            long maxManifestBytes,
            long maxInputBytes) {
        try {
            require(snapshotIds != null && snapshotIds.length > 0, "Snapshot IDs are required.");
            long[] plannedSnapshotIds = snapshotIds.clone();
            for (long snapshotId : plannedSnapshotIds) {
                require(snapshotId >= 0, "Snapshot IDs must be non-negative.");
            }
            require(manifestLength > 0, "BulkLoad manifest length must be positive.");
            require(
                    manifestLength <= maxManifestBytes,
                    "BulkLoad manifest exceeds its byte limit.");
            requireDigest(manifestSha256, "BulkLoad manifest SHA-256");
            FsPath expectedManifest = FlussPaths.bulkLoadManifestPath(remoteDataDir, handle);
            require(
                    expectedManifest.toString().equals(manifestPath),
                    "BulkLoad manifest path differs from the planned path.");
            byte[] raw = readExact(expectedManifest, manifestLength);
            require(
                    sha256(raw).equals(manifestSha256),
                    "BulkLoad manifest SHA-256 does not match.");
            List<ManifestBucket> buckets =
                    parseOuter(raw, handle.getBulkLoadId(), plannedSnapshotIds.length);
            List<ValidatedBucket> validatedBuckets = new ArrayList<>(plannedSnapshotIds.length);
            long inputBytes = manifestLength;
            for (int i = 0; i < plannedSnapshotIds.length; i++) {
                ManifestBucket bucket = buckets.get(i);
                require(bucket.bucketId == i, "BulkLoad buckets must be ordered and complete.");
                BucketValidation validation =
                        validateBucket(
                                handle,
                                remoteDataDir,
                                plannedSnapshotIds[i],
                                bucket,
                                changelogImage,
                                maxManifestBytes);
                inputBytes = Math.addExact(inputBytes, validation.totalBytes);
                require(inputBytes <= maxInputBytes, "BulkLoad input exceeds its byte limit.");
                validatedBuckets.add(validation.bucket);
            }
            return Collections.unmodifiableList(validatedBuckets);
        } catch (InvalidBulkLoadRequestException e) {
            throw e;
        } catch (Exception e) {
            throw invalid("Invalid BulkLoad manifest or standard metadata.", e);
        }
    }

    private List<ManifestBucket> parseOuter(byte[] raw, String expectedId, int bucketCount)
            throws IOException {
        Integer version = null;
        String bulkLoadId = null;
        List<ManifestBucket> buckets = null;
        try (JsonParser parser = jsonFactory.createParser(raw)) {
            expect(parser.nextToken(), JsonToken.START_OBJECT, "Manifest root must be an object.");
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                expect(parser.currentToken(), JsonToken.FIELD_NAME, "Expected a manifest field.");
                String field = parser.currentName();
                JsonToken token = parser.nextToken();
                if ("version".equals(field)) {
                    version = integer(parser, token, field);
                } else if ("bulk_load_id".equals(field)) {
                    bulkLoadId = string(parser, token, field);
                } else if ("buckets".equals(field)) {
                    buckets = parseBuckets(parser, token, bucketCount);
                } else {
                    throw invalid("Unknown BulkLoad manifest field " + field + '.', null);
                }
            }
            require(parser.nextToken() == null, "Manifest must contain one JSON value.");
        }
        require(version != null && version == VERSION, "Unsupported BulkLoad manifest version.");
        require(expectedId.equals(bulkLoadId), "BulkLoad manifest ID does not match.");
        require(
                buckets != null && buckets.size() == bucketCount,
                "Manifest must cover every bucket.");
        return buckets;
    }

    private List<ManifestBucket> parseBuckets(JsonParser parser, JsonToken token, int bucketCount)
            throws IOException {
        expect(token, JsonToken.START_ARRAY, "buckets must be an array.");
        List<ManifestBucket> buckets = new ArrayList<>();
        Set<Integer> ids = new HashSet<>();
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            expect(parser.currentToken(), JsonToken.START_OBJECT, "Bucket must be an object.");
            Integer id = null;
            FileReference snapshot = null;
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                expect(parser.currentToken(), JsonToken.FIELD_NAME, "Expected a bucket field.");
                String field = parser.currentName();
                JsonToken value = parser.nextToken();
                if ("bucket_id".equals(field)) {
                    id = integer(parser, value, field);
                } else if ("snapshot_metadata".equals(field)) {
                    snapshot = parseFileReference(parser, value);
                } else {
                    throw invalid("Unknown BulkLoad bucket field " + field + '.', null);
                }
            }
            require(id != null && id >= 0, "Missing or invalid bucket_id.");
            require(ids.add(id), "Duplicate BulkLoad bucket.");
            require(snapshot != null, "Bucket Snapshot metadata is required.");
            require(
                    id == buckets.size() && id < bucketCount,
                    "Buckets must be ordered and complete.");
            buckets.add(new ManifestBucket(id, snapshot));
        }
        return Collections.unmodifiableList(buckets);
    }

    private FileReference parseFileReference(JsonParser parser, JsonToken token)
            throws IOException {
        expect(token, JsonToken.START_OBJECT, "File reference must be an object.");
        String path = null;
        Long length = null;
        String sha256 = null;
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            expect(parser.currentToken(), JsonToken.FIELD_NAME, "Expected a file-reference field.");
            String field = parser.currentName();
            JsonToken value = parser.nextToken();
            if ("path".equals(field)) {
                path = string(parser, value, field);
            } else if ("length".equals(field)) {
                length = longInteger(parser, value, field);
            } else if ("sha256".equals(field)) {
                sha256 = string(parser, value, field);
            } else {
                throw invalid("Unknown BulkLoad file-reference field " + field + '.', null);
            }
        }
        require(path != null && !path.isEmpty(), "File-reference path is required.");
        require(length != null && length > 0, "File-reference length must be positive.");
        requireDigest(sha256, "File-reference SHA-256");
        return new FileReference(path, length, sha256);
    }

    private BucketValidation validateBucket(
            BulkLoadHandle handle,
            String remoteDataDir,
            long snapshotId,
            ManifestBucket bucket,
            ChangelogImage changelogImage,
            long maxMetadataBytes)
            throws Exception {
        TableBucket tableBucket =
                new TableBucket(handle.getTableId(), handle.getPartitionId(), bucket.bucketId);
        FsPath snapshotDirectory =
                FlussPaths.remoteKvSnapshotDir(
                        FlussPaths.remoteKvTabletDir(
                                new FsPath(remoteDataDir, FlussPaths.REMOTE_KV_DIR_NAME),
                                handle.getTarget(),
                                tableBucket),
                        snapshotId);
        FsPath snapshotMetadataPath = new FsPath(snapshotDirectory, "_METADATA");
        FileReference snapshotReference = bucket.snapshotMetadata;
        require(
                snapshotMetadataPath.toString().equals(snapshotReference.path),
                "Snapshot metadata path differs from the canonical path.");
        byte[] snapshotBytes =
                readAndVerify(
                        snapshotMetadataPath,
                        snapshotReference,
                        "Snapshot metadata",
                        maxMetadataBytes);
        requireMetadataVersion(snapshotBytes, "Snapshot metadata");
        KvSnapshotFileMetadata snapshot = KvSnapshotFileMetadataJsonSerde.fromJson(snapshotBytes);
        require(snapshot.getTableBucket().equals(tableBucket), "Snapshot physical target differs.");
        require(snapshot.getSnapshotId() == snapshotId, "Snapshot ID differs.");
        require(
                snapshot.getSnapshotLocation().equals(snapshotDirectory.toString()),
                "Snapshot location differs from the standard path.");
        require(
                snapshot.getSharedFiles().isEmpty(),
                "BulkLoad Snapshot must not use shared files.");
        require(
                snapshot.getAutoIncrementRanges() == null
                        || snapshot.getAutoIncrementRanges().isEmpty(),
                "BulkLoad Snapshot must not contain auto-increment ranges.");
        require(snapshot.getLogOffset() >= 0, "Snapshot log offset must be non-negative.");
        require(
                changelogImage == ChangelogImage.FULL
                        ? snapshot.getRowCount() != null
                        : snapshot.getRowCount() == null,
                "Snapshot row-count shape differs from the changelog image.");
        long total = snapshotReference.length;
        long privateBytes = 0L;
        Set<String> localPaths = new HashSet<>();
        Set<String> remotePaths = new HashSet<>();
        for (KvSnapshotFileMetadata.FileHandle file : snapshot.getPrivateFiles()) {
            require(file.getSize() >= 0, "Snapshot file length must be non-negative.");
            requireSafeLocalPath(file.getLocalPath());
            require(localPaths.add(file.getLocalPath()), "Duplicate Snapshot local path.");
            require(remotePaths.add(file.getPath()), "Duplicate Snapshot remote path.");
            FsPath path = new FsPath(file.getPath());
            requireWithin(path, snapshotDirectory, "Snapshot file");
            requireFileLength(path, file.getSize(), "Snapshot file");
            privateBytes = Math.addExact(privateBytes, file.getSize());
            total = Math.addExact(total, file.getSize());
        }
        require(
                snapshot.getIncrementalSize() == privateBytes,
                "Snapshot incremental size differs from its private files.");

        CompletedSnapshot completedSnapshot = CompletedSnapshotJsonSerde.fromJson(snapshotBytes);
        return new BucketValidation(new ValidatedBucket(completedSnapshot), total);
    }

    private void requireMetadataVersion(byte[] raw, String description) throws IOException {
        Integer version = null;
        try (JsonParser parser = jsonFactory.createParser(raw)) {
            expect(
                    parser.nextToken(),
                    JsonToken.START_OBJECT,
                    description + " root must be an object.");
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                expect(
                        parser.currentToken(),
                        JsonToken.FIELD_NAME,
                        "Expected a " + description + " field.");
                String field = parser.currentName();
                JsonToken token = parser.nextToken();
                if ("version".equals(field)) {
                    version = integer(parser, token, field);
                } else {
                    parser.skipChildren();
                }
            }
            require(parser.nextToken() == null, description + " must contain one JSON value.");
        }
        require(
                version != null && version == VERSION,
                description + " version must be exactly " + VERSION + '.');
    }

    private static byte[] readAndVerify(
            FsPath path, FileReference reference, String name, long maxMetadataBytes)
            throws Exception {
        require(reference.length <= maxMetadataBytes, name + " exceeds its byte limit.");
        byte[] bytes = readExact(path, reference.length);
        require(sha256(bytes).equals(reference.sha256), name + " SHA-256 differs.");
        return bytes;
    }

    private static byte[] readExact(FsPath path, long expectedLength) throws IOException {
        require(
                expectedLength > 0 && expectedLength <= Integer.MAX_VALUE,
                "Metadata length is invalid.");
        requireFileLength(path, expectedLength, "Metadata file");
        byte[] bytes = new byte[(int) expectedLength];
        try (FSDataInputStream input = path.getFileSystem().open(path)) {
            int offset = 0;
            while (offset < bytes.length) {
                int read = input.read(bytes, offset, bytes.length - offset);
                if (read < 0) {
                    throw new IOException("Unexpected end of metadata file " + path);
                }
                offset += read;
            }
            if (input.read() != -1) {
                throw new IOException("Metadata file grew while reading " + path);
            }
        }
        return bytes;
    }

    private static long fileLength(FsPath path, String name) throws IOException {
        FileStatus status = path.getFileSystem().getFileStatus(path);
        require(!status.isDir(), name + " must be a file.");
        return status.getLen();
    }

    private static long requireFileLength(FsPath path, long expected, String name)
            throws IOException {
        long actual = fileLength(path, name);
        require(actual == expected, name + " length differs.");
        return actual;
    }

    private static void requireWithin(FsPath candidate, FsPath parent, String name) {
        URI child = candidate.toUri().normalize();
        URI root = parent.toUri().normalize();
        String rootPath = root.getPath().endsWith("/") ? root.getPath() : root.getPath() + '/';
        require(
                Objects.equals(child.getScheme(), root.getScheme())
                        && Objects.equals(child.getAuthority(), root.getAuthority())
                        && child.getPath().startsWith(rootPath),
                name + " escapes its planned directory.");
    }

    private static void requireSafeLocalPath(String localPath) {
        require(localPath != null && !localPath.isEmpty(), "Snapshot local path is empty.");
        require(!localPath.contains("\\"), "Snapshot local path is unsafe.");
        String[] segments = localPath.split("/", -1);
        for (String segment : segments) {
            require(
                    !segment.isEmpty() && !segment.equals(".") && !segment.equals(".."),
                    "Snapshot local path is unsafe.");
        }
        java.nio.file.Path normalized = Paths.get(localPath).normalize();
        require(
                !normalized.isAbsolute()
                        && normalized.toString().replace('\\', '/').equals(localPath),
                "Snapshot local path is unsafe.");
    }

    private static int integer(JsonParser parser, JsonToken token, String field)
            throws IOException {
        expect(token, JsonToken.VALUE_NUMBER_INT, field + " must be an integer.");
        return parser.getIntValue();
    }

    private static long longInteger(JsonParser parser, JsonToken token, String field)
            throws IOException {
        expect(token, JsonToken.VALUE_NUMBER_INT, field + " must be an integer.");
        return parser.getLongValue();
    }

    private static String string(JsonParser parser, JsonToken token, String field)
            throws IOException {
        expect(token, JsonToken.VALUE_STRING, field + " must be a string.");
        return parser.getText();
    }

    private static void expect(JsonToken actual, JsonToken expected, String message) {
        require(actual == expected, message);
    }

    private static void requireDigest(String digest, String name) {
        require(digest != null && SHA256.matcher(digest).matches(), name + " is not canonical.");
    }

    private static String sha256(byte[] bytes) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
            char[] result = new char[digest.length * 2];
            char[] alphabet = "0123456789abcdef".toCharArray();
            for (int i = 0; i < digest.length; i++) {
                int value = digest[i] & 0xff;
                result[i * 2] = alphabet[value >>> 4];
                result[i * 2 + 1] = alphabet[value & 0xf];
            }
            return new String(result);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable.", e);
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw invalid(message, null);
        }
    }

    private static InvalidBulkLoadRequestException invalid(String message, Throwable cause) {
        return cause == null
                ? new InvalidBulkLoadRequestException(message)
                : new InvalidBulkLoadRequestException(message, cause);
    }

    private static final class ManifestBucket {
        private final int bucketId;
        private final FileReference snapshotMetadata;

        private ManifestBucket(int bucketId, FileReference snapshotMetadata) {
            this.bucketId = bucketId;
            this.snapshotMetadata = snapshotMetadata;
        }
    }

    private static final class FileReference {
        private final String path;
        private final long length;
        private final String sha256;

        private FileReference(String path, long length, String sha256) {
            this.path = path;
            this.length = length;
            this.sha256 = sha256;
        }
    }

    private static final class BucketValidation {
        private final ValidatedBucket bucket;
        private final long totalBytes;

        private BucketValidation(ValidatedBucket bucket, long totalBytes) {
            this.bucket = bucket;
            this.totalBytes = totalBytes;
        }
    }

    /** Standard metadata objects validated for one bucket and ready for ordinary adoption. */
    static final class ValidatedBucket {
        private final CompletedSnapshot completedSnapshot;

        private ValidatedBucket(CompletedSnapshot completedSnapshot) {
            this.completedSnapshot = completedSnapshot;
        }

        CompletedSnapshot getCompletedSnapshot() {
            return completedSnapshot;
        }
    }
}
