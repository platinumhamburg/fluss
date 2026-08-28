/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */

package org.apache.fluss.server.coordinator.bulkload;

import org.apache.fluss.exception.InvalidBulkLoadRequestException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.KvSnapshotFileMetadata;
import org.apache.fluss.metadata.KvSnapshotFileMetadataJsonSerde;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.utils.FlussPaths;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Stable validation tests for the minimal outer manifest and ordinary metadata. */
class BulkLoadManifestParserTest {

    private static final String ID = "550e8400-e29b-41d4-a716-446655440000";
    @TempDir private Path tempDir;

    @Test
    void testValidEmptyAndNonEmptyBuckets() throws Exception {
        Fixture fixture = fixture();

        List<BulkLoadManifestParser.ValidatedBucket> parsed = fixture.parse(fixture.manifestBytes);

        assertThat(parsed)
                .extracting(BulkLoadManifestParser.ValidatedBucket::getCompletedSnapshot)
                .extracting(CompletedSnapshot::getTableBucket)
                .extracting(TableBucket::getBucket)
                .containsExactly(0, 1);
        assertThat(parsed)
                .extracting(BulkLoadManifestParser.ValidatedBucket::getCompletedSnapshot)
                .extracting(CompletedSnapshot::getSnapshotID)
                .containsExactly(17L, 18L);
        assertThat(parsed)
                .extracting(BulkLoadManifestParser.ValidatedBucket::getCompletedSnapshot)
                .extracting(CompletedSnapshot::getLogOffset)
                .containsExactly(0L, 2L);
    }

    @Test
    void testRejectsUnknownFieldAndPathEscape() throws Exception {
        Fixture fixture = fixture();
        byte[] unknown =
                new String(fixture.manifestBytes, StandardCharsets.UTF_8)
                        .replace("\"version\":1", "\"version\":1,\"unknown\":true")
                        .getBytes(StandardCharsets.UTF_8);
        byte[] unknownBucketField =
                new String(fixture.manifestBytes, StandardCharsets.UTF_8)
                        .replace("\"bucket_id\":0,", "\"bucket_id\":0,\"unknown\":{},")
                        .getBytes(StandardCharsets.UTF_8);
        byte[] escaped =
                new String(fixture.manifestBytes, StandardCharsets.UTF_8)
                        .replace(
                                fixture.snapshotMetadata.get(0).toString(),
                                tempDir.resolve("outside/_METADATA").toUri().toString())
                        .getBytes(StandardCharsets.UTF_8);

        assertThatThrownBy(() -> fixture.parse(unknown))
                .isInstanceOf(InvalidBulkLoadRequestException.class);
        assertThatThrownBy(() -> fixture.parse(unknownBucketField))
                .isInstanceOf(InvalidBulkLoadRequestException.class);
        assertThatThrownBy(() -> fixture.parse(escaped))
                .isInstanceOf(InvalidBulkLoadRequestException.class);

        Path firstMetadata = java.nio.file.Paths.get(fixture.snapshotMetadata.get(0).toUri());
        String metadataJson = new String(Files.readAllBytes(firstMetadata), StandardCharsets.UTF_8);
        assertThat(metadataJson).contains("\"local_path\":\"file.sst\"");
        Files.write(
                firstMetadata,
                metadataJson
                        .replace("\"local_path\":\"file.sst\"", "\"local_path\":\"a//b\"")
                        .getBytes(StandardCharsets.UTF_8));
        String unsafeLocalPath =
                "{\"version\":1,\"bulk_load_id\":\""
                        + ID
                        + "\",\"buckets\":["
                        + bucketJson(0, fixture.snapshotMetadata.get(0))
                        + ','
                        + bucketJson(1, fixture.snapshotMetadata.get(1))
                        + "]}";
        assertThatThrownBy(() -> fixture.parse(unsafeLocalPath.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(InvalidBulkLoadRequestException.class);
    }

    @Test
    void testRejectsMissingSnapshotMetadataVersion() throws Exception {
        Fixture fixture = fixture();
        Path metadata = java.nio.file.Paths.get(fixture.snapshotMetadata.get(0).toUri());
        String json = new String(Files.readAllBytes(metadata), StandardCharsets.UTF_8);
        Files.write(metadata, json.replace("\"version\":1,", "").getBytes(StandardCharsets.UTF_8));
        String outer =
                "{\"version\":1,\"bulk_load_id\":\""
                        + ID
                        + "\",\"buckets\":["
                        + bucketJson(0, fixture.snapshotMetadata.get(0))
                        + ','
                        + bucketJson(1, fixture.snapshotMetadata.get(1))
                        + "]}";

        assertThatThrownBy(() -> fixture.parse(outer.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(InvalidBulkLoadRequestException.class)
                .hasMessageContaining("Snapshot metadata version");
    }

    @Test
    void testRejectsFutureSnapshotMetadataVersion() throws Exception {
        Fixture fixture = fixture();
        Path metadata = java.nio.file.Paths.get(fixture.snapshotMetadata.get(0).toUri());
        String json = new String(Files.readAllBytes(metadata), StandardCharsets.UTF_8);
        Files.write(
                metadata,
                json.replace("\"version\":1", "\"version\":2").getBytes(StandardCharsets.UTF_8));
        String outer =
                "{\"version\":1,\"bulk_load_id\":\""
                        + ID
                        + "\",\"buckets\":["
                        + bucketJson(0, fixture.snapshotMetadata.get(0))
                        + ','
                        + bucketJson(1, fixture.snapshotMetadata.get(1))
                        + "]}";

        assertThatThrownBy(() -> fixture.parse(outer.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(InvalidBulkLoadRequestException.class)
                .hasMessageContaining("Snapshot metadata version");
    }

    @Test
    void testRejectsOversizedSnapshotMetadataBeforeReadingContent() throws Exception {
        Fixture fixture = fixture();
        Path metadata = java.nio.file.Paths.get(fixture.snapshotMetadata.get(0).toUri());
        byte[] invalidMetadata = new byte[1025];
        Arrays.fill(invalidMetadata, (byte) 'x');
        Files.write(metadata, invalidMetadata);
        String outer =
                "{\"version\":1,\"bulk_load_id\":\""
                        + ID
                        + "\",\"buckets\":["
                        + bucketJson(0, fixture.snapshotMetadata.get(0))
                        + ','
                        + bucketJson(1, fixture.snapshotMetadata.get(1))
                        + "]}";
        byte[] outerBytes = outer.getBytes(StandardCharsets.UTF_8);

        assertThatThrownBy(() -> fixture.parseWithLimits(outerBytes, 1024L, Long.MAX_VALUE))
                .isInstanceOf(InvalidBulkLoadRequestException.class)
                .hasMessageContaining("Snapshot metadata exceeds its byte limit");
    }

    private Fixture fixture() throws Exception {
        String remoteDataDir = tempDir.toUri().toString();
        BulkLoadHandle handle =
                new BulkLoadHandle(PhysicalTablePath.of("db", "table", null), 41L, null, ID);
        long[] snapshotIds = {17L, 18L};
        List<FsPath> snapshotMetadata =
                Arrays.asList(
                        writeSnapshot(remoteDataDir, handle, 0, 17L, 0L, 0L),
                        writeSnapshot(remoteDataDir, handle, 1, 18L, 2L, 2L));
        String json =
                "{\"version\":1,\"bulk_load_id\":\""
                        + ID
                        + "\",\"buckets\":["
                        + bucketJson(0, snapshotMetadata.get(0))
                        + ','
                        + bucketJson(1, snapshotMetadata.get(1))
                        + "]}";
        return new Fixture(
                remoteDataDir,
                handle,
                snapshotIds,
                snapshotMetadata,
                json.getBytes(StandardCharsets.UTF_8));
    }

    private FsPath writeSnapshot(
            String remoteDataDir,
            BulkLoadHandle handle,
            int bucketId,
            long snapshotId,
            long endOffset,
            Long rowCount)
            throws Exception {
        TableBucket tableBucket = new TableBucket(handle.getTableId(), bucketId);
        FsPath snapshotDir =
                FlussPaths.remoteKvSnapshotDir(
                        FlussPaths.remoteKvTabletDir(
                                new FsPath(remoteDataDir, FlussPaths.REMOTE_KV_DIR_NAME),
                                handle.getTarget(),
                                tableBucket),
                        snapshotId);
        Path data = tempDir.resolve("snapshot-" + bucketId + ".sst");
        Files.write(data, new byte[] {(byte) bucketId});
        String remoteFile = new FsPath(snapshotDir, "file.sst").toString();
        Path remoteFilePath = java.nio.file.Paths.get(new java.net.URI(remoteFile));
        Files.createDirectories(remoteFilePath.getParent());
        Files.copy(data, remoteFilePath);
        KvSnapshotFileMetadata metadata =
                new KvSnapshotFileMetadata(
                        tableBucket,
                        snapshotId,
                        snapshotDir.toString(),
                        Collections.emptyList(),
                        Collections.singletonList(
                                new KvSnapshotFileMetadata.FileHandle(remoteFile, 1L, "file.sst")),
                        1L,
                        endOffset,
                        rowCount,
                        null);
        FsPath metadataPath = new FsPath(snapshotDir, "_METADATA");
        Files.write(
                java.nio.file.Paths.get(metadataPath.toUri()),
                KvSnapshotFileMetadataJsonSerde.toJson(metadata));
        return metadataPath;
    }

    private static void write(FsPath path, byte[] bytes) throws Exception {
        Path local = java.nio.file.Paths.get(path.toUri());
        Files.createDirectories(local.getParent());
        Files.write(local, bytes);
    }

    private static String bucketJson(int bucketId, FsPath snapshotMetadata) throws Exception {
        byte[] snapshot = Files.readAllBytes(java.nio.file.Paths.get(snapshotMetadata.toUri()));
        return "{\"bucket_id\":"
                + bucketId
                + ",\"snapshot_metadata\":{\"path\":\""
                + snapshotMetadata
                + "\",\"length\":"
                + snapshot.length
                + ",\"sha256\":\""
                + sha256(snapshot)
                + "\"}}";
    }

    private static String sha256(byte[] bytes) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
        StringBuilder result = new StringBuilder(64);
        for (byte value : digest) {
            result.append(String.format("%02x", value & 0xff));
        }
        return result.toString();
    }

    private static final class Fixture {
        private final String remoteDataDir;
        private final BulkLoadHandle handle;
        private final long[] snapshotIds;
        private final List<FsPath> snapshotMetadata;
        private final byte[] manifestBytes;

        private Fixture(
                String remoteDataDir,
                BulkLoadHandle handle,
                long[] snapshotIds,
                List<FsPath> snapshotMetadata,
                byte[] manifestBytes) {
            this.remoteDataDir = remoteDataDir;
            this.handle = handle;
            this.snapshotIds = snapshotIds;
            this.snapshotMetadata = snapshotMetadata;
            this.manifestBytes = manifestBytes;
        }

        private List<BulkLoadManifestParser.ValidatedBucket> parse(byte[] bytes) throws Exception {
            return parseWithLimits(bytes, 1024 * 1024, 1024 * 1024);
        }

        private List<BulkLoadManifestParser.ValidatedBucket> parseWithLimits(
                byte[] bytes, long maxManifestBytes, long maxInputBytes) throws Exception {
            FsPath path = FlussPaths.bulkLoadManifestPath(remoteDataDir, handle);
            write(path, bytes);
            return new BulkLoadManifestParser()
                    .parse(
                            handle,
                            remoteDataDir,
                            path.toString(),
                            bytes.length,
                            sha256(bytes),
                            snapshotIds,
                            ChangelogImage.FULL,
                            maxManifestBytes,
                            maxInputBytes);
        }
    }
}
