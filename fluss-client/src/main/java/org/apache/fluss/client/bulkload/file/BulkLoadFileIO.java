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

    static void writeBytesExact(FsPath remote, byte[] expected) throws IOException {
        FileSystem fileSystem = remote.getFileSystem();
        fileSystem.mkdirs(remote.getParent());
        FSDataOutputStream output;
        try {
            output = fileSystem.create(remote, FileSystem.WriteMode.NO_OVERWRITE);
        } catch (IOException createFailure) {
            try {
                if (adoptExactOrDeleteStrictPrefix(remote, expected)) {
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
        writeOwnedBytes(fileSystem, remote, expected, output);
    }

    static void writeFileExact(FsPath remote, Path local, long expectedLength) throws IOException {
        requireCompleteLocalFile(local, expectedLength);
        FileSystem fileSystem = remote.getFileSystem();
        fileSystem.mkdirs(remote.getParent());
        FSDataOutputStream output;
        try {
            output = fileSystem.create(remote, FileSystem.WriteMode.NO_OVERWRITE);
        } catch (IOException createFailure) {
            try {
                if (adoptExactOrDeleteStrictPrefix(remote, local, expectedLength)) {
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
        writeOwnedFile(fileSystem, remote, local, expectedLength, output);
    }

    private static void writeOwnedBytes(
            FileSystem fileSystem, FsPath remote, byte[] expected, FSDataOutputStream output)
            throws IOException {
        try (FSDataOutputStream ownedOutput = output) {
            ownedOutput.write(expected);
        } catch (IOException | RuntimeException failure) {
            deleteOwnedFileBestEffort(fileSystem, remote, failure);
            throw failure;
        }
    }

    private static void writeOwnedFile(
            FileSystem fileSystem,
            FsPath remote,
            Path local,
            long expectedLength,
            FSDataOutputStream output)
            throws IOException {
        try (InputStream input = openLocal(local);
                FSDataOutputStream ownedOutput = output) {
            byte[] buffer = new byte[BUFFER_SIZE];
            long written = 0L;
            int read;
            while ((read = input.read(buffer)) != -1) {
                ownedOutput.write(buffer, 0, read);
                written = Math.addExact(written, read);
            }
            if (written != expectedLength) {
                throw new IOException("BulkLoad local file length changed while writing: " + local);
            }
        } catch (IOException | RuntimeException failure) {
            deleteOwnedFileBestEffort(fileSystem, remote, failure);
            throw failure;
        }
    }

    private static byte[] readExact(FsPath path, int expectedLength) throws IOException {
        FileStatus status = path.getFileSystem().getFileStatus(path);
        if (status.isDir() || status.getLen() != expectedLength) {
            throw new IOException("BulkLoad file read-back length differs: " + path);
        }
        byte[] bytes = new byte[expectedLength];
        try (FSDataInputStream input = path.getFileSystem().open(path)) {
            int position = 0;
            while (position < bytes.length) {
                int read = input.read(bytes, position, bytes.length - position);
                if (read == -1) {
                    throw new IOException("BulkLoad file read-back ended early: " + path);
                }
                position += read;
            }
            if (input.read() != -1) {
                throw new IOException("BulkLoad file read-back length differs: " + path);
            }
        }
        return bytes;
    }

    static byte[] readExact(FsPath path) throws IOException {
        FileStatus status = path.getFileSystem().getFileStatus(path);
        if (status.isDir() || status.getLen() <= 0L || status.getLen() > Integer.MAX_VALUE) {
            throw new IOException("BulkLoad metadata read-back length differs: " + path);
        }
        return readExact(path, Math.toIntExact(status.getLen()));
    }

    private static boolean adoptExactOrDeleteStrictPrefix(FsPath remote, byte[] expected)
            throws IOException {
        FileSystem fileSystem = remote.getFileSystem();
        FileStatus status = fileSystem.getFileStatus(remote);
        if (status.isDir() || status.getLen() > expected.length) {
            throw new IOException("BulkLoad file read-back length differs: " + remote);
        }
        int existingLength = Math.toIntExact(status.getLen());
        try (FSDataInputStream actual = fileSystem.open(remote)) {
            byte[] buffer = new byte[BUFFER_SIZE];
            int position = 0;
            while (position < existingLength) {
                int read =
                        actual.read(buffer, 0, Math.min(buffer.length, existingLength - position));
                if (read == -1) {
                    throw new IOException("BulkLoad file read-back ended early: " + remote);
                }
                for (int index = 0; index < read; index++) {
                    if (buffer[index] != expected[position + index]) {
                        throw new IOException("BulkLoad file read-back bytes differ: " + remote);
                    }
                }
                position += read;
            }
            if (actual.read() != -1) {
                throw new IOException("BulkLoad file read-back length differs: " + remote);
            }
        }
        if (existingLength == expected.length) {
            return true;
        }
        deleteStrictPrefix(fileSystem, remote);
        return false;
    }

    private static boolean adoptExactOrDeleteStrictPrefix(
            FsPath remote, Path local, long expectedLength) throws IOException {
        FileSystem fileSystem = remote.getFileSystem();
        FileStatus status = fileSystem.getFileStatus(remote);
        if (status.isDir() || status.getLen() > expectedLength) {
            throw new IOException("BulkLoad file read-back length differs: " + remote);
        }
        long existingLength = status.getLen();
        try (InputStream expected = openLocal(local);
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
                throw new IOException("BulkLoad local file length changed while reading: " + local);
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
