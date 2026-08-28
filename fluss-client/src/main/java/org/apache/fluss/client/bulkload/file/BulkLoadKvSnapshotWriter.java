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

package org.apache.fluss.client.bulkload.file;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.BulkLoadTargetInfo;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.KvSnapshotFileMetadata;
import org.apache.fluss.metadata.KvSnapshotFileMetadataJsonSerde;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.rocksdb.RocksIteratorWrapper;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.FlussPaths;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Writes one immutable standard KV Snapshot directly into its final remote directory. */
@Internal
public final class BulkLoadKvSnapshotWriter {

    private static final String METADATA_FILE_NAME = "_METADATA";
    private static final String PRIVATE_FILE_PREFIX = "private-";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final BulkLoadTargetInfo targetInfo;
    private final TableBucket tableBucket;
    private final long snapshotId;
    private final FsPath snapshotDirectory;
    private final Path localStagingDirectory;

    public BulkLoadKvSnapshotWriter(
            BulkLoadTargetInfo targetInfo, int bucketId, Path localStagingDirectory) {
        this.targetInfo = checkNotNull(targetInfo, "BulkLoad target info must not be null.");
        this.localStagingDirectory =
                checkNotNull(
                                localStagingDirectory,
                                "BulkLoad Snapshot staging directory must not be null.")
                        .toAbsolutePath()
                        .normalize();
        BulkLoadHandle handle = targetInfo.getHandle();
        this.tableBucket = new TableBucket(handle.getTableId(), handle.getPartitionId(), bucketId);
        this.snapshotId = targetInfo.getSnapshotId(bucketId);
        TableInfo tableInfo = targetInfo.getTableInfo();
        String remoteDataDir =
                checkNotNull(
                        tableInfo.getRemoteDataDir(),
                        "BulkLoad target remote data directory must not be null.");
        FsPath remoteKvTabletDirectory =
                FlussPaths.remoteKvTabletDir(
                        new FsPath(remoteDataDir, FlussPaths.REMOTE_KV_DIR_NAME),
                        handle.getTarget(),
                        tableBucket);
        FsPath expectedSnapshotDirectory =
                FlussPaths.remoteKvSnapshotDir(remoteKvTabletDirectory, snapshotId);
        this.snapshotDirectory = expectedSnapshotDirectory;
    }

    public BulkLoadFileHandle write(
            Path localCheckpoint, long logEndOffset, @Nullable Long rowCount) throws Exception {
        checkNotNull(localCheckpoint, "BulkLoad local checkpoint must not be null.");
        checkArgument(logEndOffset >= 0, "BulkLoad log end offset must be non-negative.");
        boolean fullImage =
                targetInfo.getTableInfo().getTableConfig().getChangelogImage()
                        == ChangelogImage.FULL;
        checkArgument(
                fullImage ? rowCount != null : rowCount == null,
                "BulkLoad Snapshot row count differs from the frozen changelog image.");

        Path checkpointRoot = localCheckpoint.toAbsolutePath().normalize();
        requireCheckpointOpens(checkpointRoot);
        List<CheckpointFile> checkpointFiles = checkpointFiles(checkpointRoot);
        FsPath metadataPath = new FsPath(snapshotDirectory, METADATA_FILE_NAME);
        IOException incompleteMetadataFailure = null;
        BulkLoadFileHandle existing = null;
        try {
            existing = adoptIfPresent(metadataPath, checkpointRoot, logEndOffset, rowCount);
        } catch (IOException failure) {
            // Classification is deferred until the complete expected metadata bytes exist below.
            incompleteMetadataFailure = failure;
        }
        if (existing != null) {
            return existing;
        }

        List<KvSnapshotFileMetadata.FileHandle> privateFiles = new ArrayList<>();
        Set<String> remotePaths = new HashSet<>();
        long incrementalSize = 0L;
        for (CheckpointFile checkpointFile : checkpointFiles) {
            String contentSha256 = BulkLoadDigests.sha256Hex(checkpointFile.path);
            FsPath remoteFile =
                    contentAddressedPrivateFile(
                            snapshotDirectory, checkpointFile.localPath, contentSha256);
            checkState(
                    remotePaths.add(remoteFile.toString()),
                    "BulkLoad checkpoint contains a duplicate remote object identity.");
            BulkLoadFileIO.writeFileExact(remoteFile, checkpointFile.path, checkpointFile.length);
            privateFiles.add(
                    new KvSnapshotFileMetadata.FileHandle(
                            remoteFile.toString(),
                            checkpointFile.length,
                            checkpointFile.localPath));
            incrementalSize = Math.addExact(incrementalSize, checkpointFile.length);
        }

        KvSnapshotFileMetadata metadata =
                new KvSnapshotFileMetadata(
                        tableBucket,
                        snapshotId,
                        snapshotDirectory.toString(),
                        Collections.<KvSnapshotFileMetadata.FileHandle>emptyList(),
                        privateFiles,
                        incrementalSize,
                        logEndOffset,
                        rowCount,
                        null);
        byte[] metadataBytes = KvSnapshotFileMetadataJsonSerde.toJson(metadata);
        try {
            BulkLoadFileIO.writeBytesExact(metadataPath, metadataBytes);
        } catch (IOException failure) {
            if (incompleteMetadataFailure != null) {
                failure.addSuppressed(incompleteMetadataFailure);
            }
            throw failure;
        }
        return metadataHandle(metadataPath, metadataBytes);
    }

    private static FsPath contentAddressedPrivateFile(
            FsPath snapshotDirectory, String localPath, String contentSha256) {
        checkArgument(
                isSafeLocalPath(localPath), "Unsafe BulkLoad Snapshot local path: %s.", localPath);
        checkArgument(
                contentSha256 != null && contentSha256.matches("[0-9a-f]{64}"),
                "BulkLoad Snapshot content SHA-256 is not canonical.");
        String localPathSha256 =
                BulkLoadDigests.sha256Hex(localPath.getBytes(StandardCharsets.UTF_8));
        return new FsPath(
                snapshotDirectory, PRIVATE_FILE_PREFIX + contentSha256 + "-" + localPathSha256);
    }

    private static boolean isSafeLocalPath(String localPath) {
        if (localPath == null
                || localPath.isEmpty()
                || localPath.startsWith("/")
                || localPath.endsWith("/")
                || localPath.indexOf('\\') >= 0
                || localPath.contains("//")) {
            return false;
        }
        String[] components = localPath.split("/", -1);
        for (String component : components) {
            if (component.isEmpty() || ".".equals(component) || "..".equals(component)) {
                return false;
            }
        }
        try {
            Path normalized = Paths.get(localPath).normalize();
            return !normalized.isAbsolute()
                    && localPath.equals(normalized.toString().replace(File.separatorChar, '/'));
        } catch (RuntimeException invalid) {
            return false;
        }
    }

    private BulkLoadFileHandle adoptIfPresent(
            FsPath metadataPath, Path localCheckpoint, long logEndOffset, @Nullable Long rowCount)
            throws Exception {
        if (!metadataPath.getFileSystem().exists(metadataPath)) {
            return null;
        }
        return adoptCompletedMetadata(metadataPath, localCheckpoint, logEndOffset, rowCount);
    }

    private BulkLoadFileHandle adoptCompletedMetadata(
            FsPath metadataPath, Path localCheckpoint, long logEndOffset, @Nullable Long rowCount)
            throws Exception {
        byte[] metadataBytes = BulkLoadFileIO.readExact(metadataPath);
        KvSnapshotFileMetadata metadata = parseMetadata(metadataBytes);
        checkState(
                tableBucket.equals(metadata.getTableBucket())
                        && snapshotId == metadata.getSnapshotId()
                        && snapshotDirectory.toString().equals(metadata.getSnapshotLocation())
                        && metadata.getLogOffset() == logEndOffset
                        && Objects.equals(metadata.getRowCount(), rowCount)
                        && metadata.getSharedFiles().isEmpty()
                        && metadata.getAutoIncrementRanges() == null,
                "Existing BulkLoad Snapshot metadata identity differs.");

        Set<String> localPaths = new HashSet<>();
        Set<String> remotePaths = new HashSet<>();
        Files.createDirectories(localStagingDirectory);
        Path reconstructed =
                Files.createTempDirectory(localStagingDirectory, "fluss-bulkload-snapshot-adopt-");
        long incrementalSize = 0L;
        try {
            for (KvSnapshotFileMetadata.FileHandle file : metadata.getPrivateFiles()) {
                checkState(
                        file.getSize() >= 0L && isSafeLocalPath(file.getLocalPath()),
                        "Existing BulkLoad Snapshot local path or size is invalid.");
                checkState(
                        localPaths.add(file.getLocalPath()) && remotePaths.add(file.getPath()),
                        "Existing BulkLoad Snapshot contains duplicate handles.");
                Path localFile = resolveLocalPath(reconstructed, file.getLocalPath());
                Files.createDirectories(checkNotNull(localFile.getParent()));
                String contentSha256 = copyRemoteFile(file, localFile);
                checkState(
                        contentAddressedPrivateFile(
                                        snapshotDirectory, file.getLocalPath(), contentSha256)
                                .toString()
                                .equals(file.getPath()),
                        "Existing BulkLoad Snapshot private object identity differs.");
                incrementalSize = Math.addExact(incrementalSize, file.getSize());
            }
            checkState(
                    incrementalSize == metadata.getIncrementalSize(),
                    "Existing BulkLoad Snapshot incremental size differs.");
            requireCheckpointsEqual(localCheckpoint, reconstructed);
        } finally {
            FileUtils.deleteDirectoryQuietly(reconstructed.toFile());
        }
        return metadataHandle(metadataPath, metadataBytes);
    }

    private static List<CheckpointFile> checkpointFiles(Path checkpointRoot) throws IOException {
        checkState(
                !Files.isSymbolicLink(checkpointRoot)
                        && Files.isDirectory(checkpointRoot, LinkOption.NOFOLLOW_LINKS),
                "BulkLoad checkpoint root must be a non-symlink directory.");
        List<CheckpointFile> files = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(checkpointRoot)) {
            java.util.Iterator<Path> iterator = paths.iterator();
            while (iterator.hasNext()) {
                Path path = iterator.next().toAbsolutePath().normalize();
                checkState(
                        path.startsWith(checkpointRoot),
                        "BulkLoad checkpoint entry escapes the checkpoint root.");
                checkState(
                        !Files.isSymbolicLink(path),
                        "BulkLoad checkpoint symlink is not allowed: %s.",
                        path);
                BasicFileAttributes attributes =
                        Files.readAttributes(
                                path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                if (attributes.isDirectory()) {
                    continue;
                }
                checkState(
                        attributes.isRegularFile(),
                        "BulkLoad checkpoint entry is not a regular file: %s.",
                        path);
                Path relative = checkpointRoot.relativize(path).normalize();
                String localPath = relative.toString().replace(File.separatorChar, '/');
                checkState(
                        isSafeLocalPath(localPath),
                        "BulkLoad checkpoint local path is unsafe: %s.",
                        localPath);
                files.add(new CheckpointFile(path, localPath, attributes.size()));
            }
        }
        files.sort(Comparator.comparing(file -> file.localPath));
        return files;
    }

    private static void requireCheckpointOpens(Path checkpoint) throws Exception {
        RocksDB.loadLibrary();
        try (Options options = new Options().setCreateIfMissing(false);
                RocksDB ignored = RocksDB.openReadOnly(options, checkpoint.toString())) {
            // Opening read-only proves that the checkpoint is complete.
        }
    }

    private static void requireCheckpointsEqual(Path expectedPath, Path actualPath)
            throws Exception {
        try (Options options = new Options().setCreateIfMissing(false);
                RocksDB expected = RocksDB.openReadOnly(options, expectedPath.toString());
                RocksDB actual = RocksDB.openReadOnly(options, actualPath.toString());
                RocksIteratorWrapper expectedIterator =
                        new RocksIteratorWrapper(expected.newIterator());
                RocksIteratorWrapper actualIterator =
                        new RocksIteratorWrapper(actual.newIterator())) {
            expectedIterator.seekToFirst();
            actualIterator.seekToFirst();
            while (true) {
                boolean expectedValid = expectedIterator.isValid();
                boolean actualValid = actualIterator.isValid();
                checkState(
                        expectedValid == actualValid,
                        "Existing BulkLoad Snapshot RocksDB key count differs.");
                if (!expectedValid) {
                    return;
                }
                checkState(
                        Arrays.equals(expectedIterator.key(), actualIterator.key())
                                && Arrays.equals(expectedIterator.value(), actualIterator.value()),
                        "Existing BulkLoad Snapshot RocksDB contents differ.");
                expectedIterator.next();
                actualIterator.next();
            }
        }
    }

    private static KvSnapshotFileMetadata parseMetadata(byte[] bytes) throws IOException {
        JsonNode root = MAPPER.readTree(bytes);
        JsonNode version = root == null ? null : root.get("version");
        if (root == null
                || !root.isObject()
                || version == null
                || !version.isIntegralNumber()
                || version.asInt() != 1) {
            throw new IOException("BulkLoad Snapshot metadata version differs.");
        }
        try {
            return KvSnapshotFileMetadataJsonSerde.fromJson(bytes);
        } catch (RuntimeException malformed) {
            throw new IOException("BulkLoad Snapshot metadata is malformed.", malformed);
        }
    }

    private static Path resolveLocalPath(Path root, String localPath) {
        checkState(
                isSafeLocalPath(localPath), "Unsafe BulkLoad Snapshot local path: %s.", localPath);
        Path resolved = root.resolve(localPath).normalize();
        checkState(
                resolved.startsWith(root),
                "BulkLoad Snapshot local path escapes the reconstruction root.");
        return resolved;
    }

    private static String copyRemoteFile(KvSnapshotFileMetadata.FileHandle file, Path localFile)
            throws IOException {
        FsPath remote = new FsPath(file.getPath());
        FileStatus status = remote.getFileSystem().getFileStatus(remote);
        if (status.isDir() || status.getLen() != file.getSize()) {
            throw new IOException("BulkLoad Snapshot referenced file length differs: " + remote);
        }
        MessageDigest digest = BulkLoadDigests.newDigest();
        long copied = 0L;
        try (FSDataInputStream input = remote.getFileSystem().open(remote);
                OutputStream output =
                        Files.newOutputStream(localFile, StandardOpenOption.CREATE_NEW)) {
            byte[] buffer = new byte[64 * 1024];
            int read;
            while ((read = input.read(buffer)) != -1) {
                copied = Math.addExact(copied, read);
                if (copied > file.getSize()) {
                    throw new IOException(
                            "BulkLoad Snapshot referenced file is longer than declared: " + remote);
                }
                digest.update(buffer, 0, read);
                output.write(buffer, 0, read);
            }
        }
        if (copied != file.getSize()) {
            throw new IOException("BulkLoad Snapshot referenced file ended early: " + remote);
        }
        return BulkLoadDigests.toHex(digest.digest());
    }

    private static BulkLoadFileHandle metadataHandle(FsPath metadataPath, byte[] bytes) {
        return new BulkLoadFileHandle(
                metadataPath.toString(), bytes.length, BulkLoadDigests.sha256Hex(bytes));
    }

    private static final class CheckpointFile {
        private final Path path;
        private final String localPath;
        private final long length;

        private CheckpointFile(Path path, String localPath, long length) {
            this.path = path;
            this.localPath = localPath;
            this.length = length;
        }
    }
}
