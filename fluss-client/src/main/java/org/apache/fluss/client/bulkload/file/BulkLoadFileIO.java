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

import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Exact create-or-reuse file operations used by BulkLoad output writers. */
final class BulkLoadFileIO {

    private static final int BUFFER_SIZE = 64 * 1024;

    private BulkLoadFileIO() {}

    /** Opens the bytes to upload; reopened once per attempt so contents can be re-read. */
    private interface ByteSource {
        InputStream open() throws IOException;
    }

    static void writeBytesExact(FsPath remote, byte[] expected) throws IOException {
        writeExact(remote, () -> new ByteArrayInputStream(expected), expected.length, remote);
    }

    static void writeFileExact(FsPath remote, Path local, long expectedLength) throws IOException {
        requireCompleteLocalFile(local, expectedLength);
        writeExact(remote, () -> openLocal(local), expectedLength, local);
    }

    static byte[] readExact(FsPath path) throws IOException {
        FileStatus status = path.getFileSystem().getFileStatus(path);
        if (status.isDir() || status.getLen() <= 0L || status.getLen() > Integer.MAX_VALUE) {
            throw new IOException("BulkLoad metadata read-back length differs: " + path);
        }
        byte[] bytes = new byte[Math.toIntExact(status.getLen())];
        try (FSDataInputStream input = path.getFileSystem().open(path)) {
            if (readUpTo(input, bytes, bytes.length) != bytes.length) {
                throw new IOException("BulkLoad file read-back ended early: " + path);
            }
            if (input.read() != -1) {
                throw new IOException("BulkLoad file read-back length differs: " + path);
            }
        }
        return bytes;
    }

    private static void writeExact(
            FsPath remote, ByteSource source, long expectedLength, Object sourceName)
            throws IOException {
        FileSystem fileSystem = remote.getFileSystem();
        fileSystem.mkdirs(remote.getParent());
        FSDataOutputStream output;
        try {
            output = fileSystem.create(remote, FileSystem.WriteMode.NO_OVERWRITE);
        } catch (IOException createFailure) {
            try {
                if (adoptExactOrDeleteStrictPrefix(remote, source, expectedLength, sourceName)) {
                    return;
                }
            } catch (IOException mismatch) {
                mismatch.addSuppressed(createFailure);
                throw mismatch;
            }
            try {
                output = fileSystem.create(remote, FileSystem.WriteMode.NO_OVERWRITE);
            } catch (IOException retryFailure) {
                retryFailure.addSuppressed(createFailure);
                throw retryFailure;
            }
        }
        try (FSDataOutputStream ownedOutput = output;
                InputStream input = source.open()) {
            byte[] buffer = new byte[BUFFER_SIZE];
            long written = 0L;
            int read;
            while ((read = input.read(buffer)) != -1) {
                ownedOutput.write(buffer, 0, read);
                written = Math.addExact(written, read);
            }
            if (written != expectedLength) {
                throw new IOException(
                        "BulkLoad local file length changed while writing: " + sourceName);
            }
        } catch (IOException | RuntimeException failure) {
            deleteOwnedFileBestEffort(fileSystem, remote, failure);
            throw failure;
        }
    }

    private static boolean adoptExactOrDeleteStrictPrefix(
            FsPath remote, ByteSource source, long expectedLength, Object sourceName)
            throws IOException {
        FileSystem fileSystem = remote.getFileSystem();
        FileStatus status = fileSystem.getFileStatus(remote);
        if (status.isDir() || status.getLen() > expectedLength) {
            throw new IOException("BulkLoad file read-back length differs: " + remote);
        }
        long existingLength = status.getLen();
        try (InputStream expected = source.open();
                FSDataInputStream actual = fileSystem.open(remote)) {
            byte[] expectedBuffer = new byte[BUFFER_SIZE];
            byte[] actualBuffer = new byte[BUFFER_SIZE];
            long compared = 0L;
            while (compared < existingLength) {
                int requested =
                        (int) Math.min((long) expectedBuffer.length, existingLength - compared);
                int expectedRead = readUpTo(expected, expectedBuffer, requested);
                int actualRead = readUpTo(actual, actualBuffer, requested);
                if (expectedRead != requested || actualRead != requested) {
                    throw new IOException("BulkLoad file read-back ended early: " + remote);
                }
                for (int index = 0; index < requested; index++) {
                    if (expectedBuffer[index] != actualBuffer[index]) {
                        throw new IOException("BulkLoad file read-back bytes differ: " + remote);
                    }
                }
                compared += requested;
            }
            if (actual.read() != -1) {
                throw new IOException("BulkLoad file read-back length differs: " + remote);
            }
            if (existingLength == expectedLength && expected.read() != -1) {
                throw new IOException(
                        "BulkLoad local file length changed while reading: " + sourceName);
            }
        }
        if (existingLength == expectedLength) {
            return true;
        }
        deleteStrictPrefix(fileSystem, remote);
        return false;
    }

    private static int readUpTo(InputStream input, byte[] buffer, int requested)
            throws IOException {
        int position = 0;
        while (position < requested) {
            int read = input.read(buffer, position, requested - position);
            if (read == -1) {
                break;
            }
            position += read;
        }
        return position;
    }

    private static void requireCompleteLocalFile(Path local, long expectedLength)
            throws IOException {
        try (InputStream ignored = openLocal(local)) {
            if (Files.size(local) != expectedLength) {
                throw new IOException("BulkLoad local file length differs: " + local);
            }
        }
    }

    private static void deleteStrictPrefix(FileSystem fileSystem, FsPath remote)
            throws IOException {
        if (!fileSystem.delete(remote, false)) {
            throw new IOException("BulkLoad strict-prefix file could not be deleted: " + remote);
        }
    }

    private static void deleteOwnedFileBestEffort(
            FileSystem fileSystem, FsPath remote, Throwable failure) {
        try {
            if (!fileSystem.delete(remote, false)) {
                failure.addSuppressed(
                        new IOException(
                                "BulkLoad call-owned file could not be deleted: " + remote));
            }
        } catch (IOException cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
    }

    private static InputStream openLocal(Path local) throws IOException {
        return Channels.newInputStream(
                Files.newByteChannel(local, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS));
    }
}
