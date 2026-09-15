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

package org.apache.fluss.server.coordinator.remote;

import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.rpc.messages.PbTestFileStatus;
import org.apache.fluss.rpc.messages.TestFilesystemRequest;
import org.apache.fluss.rpc.messages.TestFilesystemResponse;
import org.apache.fluss.rpc.protocol.TestFilesystemOperation;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Direct filesystem access confined to the configured roots of a dedicated test server. */
public final class TestFilesystemHandler {
    private final List<String> rootPaths;

    /** Creates a handler using the current configured roots. */
    public TestFilesystemHandler(List<String> rootPaths) {
        this.rootPaths = rootPaths;
    }

    /** Executes one operation; I/O failures are returned separately from RPC failures. */
    public TestFilesystemResponse execute(TestFilesystemRequest request) {
        try {
            return executeChecked(request);
        } catch (IllegalArgumentException e) {
            return failure("INVALID_ARGUMENT", e.getMessage());
        } catch (FileNotFoundException e) {
            return failure("NOT_FOUND", "File or directory does not exist");
        } catch (AccessDeniedException | SecurityException e) {
            return failure("ACCESS_DENIED", "Filesystem access denied");
        } catch (IOException e) {
            // Hadoop's AccessControlException is optional and cannot be linked from this module.
            if (e.getClass().getSimpleName().equals("AccessControlException")) {
                return failure("ACCESS_DENIED", "Filesystem access denied");
            }
            return failure(
                    "IO_ERROR", "Filesystem operation failed: " + e.getClass().getSimpleName());
        }
    }

    private TestFilesystemResponse executeChecked(TestFilesystemRequest request)
            throws IOException {
        TestFilesystemOperation operation = TestFilesystemOperation.forId(request.getOperation());
        List<FsPath> roots = new ArrayList<>();
        for (String root : rootPaths) {
            roots.add(canonicalPath(root));
        }
        TestFilesystemResponse response = new TestFilesystemResponse();
        if (operation == TestFilesystemOperation.LIST && request.getPath().isEmpty()) {
            for (FsPath root : roots) {
                // Configured object-store prefixes need not have a directory marker yet.
                response.addFile()
                        .setPath(root.toUri().toASCIIString())
                        .setDirectory(true)
                        .setLength(0)
                        .setModificationTime(Long.MAX_VALUE);
            }
            return response;
        }
        FsPath path =
                checkedPath(
                        request.getPath(),
                        roots,
                        operation == TestFilesystemOperation.WRITE
                                || operation == TestFilesystemOperation.DELETE);
        FileSystem fs = path.getFileSystem();
        switch (operation) {
            case LIST:
                FileStatus[] children = fs.listStatus(path);
                if (children == null) {
                    // Some adapters return null for both a missing directory and a listing error.
                    fs.getFileStatus(path);
                    throw new IOException("Filesystem returned no directory listing");
                }
                for (FileStatus status : children) {
                    addStatus(response, status);
                }
                break;
            case STAT:
                addStatus(response, fs.getFileStatus(path));
                break;
            case READ:
                checkArgument(
                        request.hasOffset() && request.getOffset() >= 0,
                        "offset must be nonnegative");
                checkArgument(
                        request.hasLength()
                                && request.getLength() >= 0
                                && request.getLength()
                                        <= TestFilesystemOperation.MAX_CONTENT_LENGTH,
                        "length must be between 0 and 1 MiB");
                try (FSDataInputStream input = fs.open(path)) {
                    byte[] data = new byte[request.getLength()];
                    int count = 0;
                    if (request.getOffset() < fs.getFileStatus(path).getLen()) {
                        input.seek(request.getOffset());
                        while (count < data.length) {
                            int read = input.read(data, count, data.length - count);
                            if (read < 0) {
                                break;
                            }
                            if (read == 0) {
                                int value = input.read();
                                if (value < 0) {
                                    break;
                                }
                                data[count++] = (byte) value;
                            } else {
                                count += read;
                            }
                        }
                    }
                    response.setContent(Arrays.copyOf(data, count));
                }
                break;
            case WRITE:
                checkArgument(
                        request.hasContent()
                                && request.getContentSize()
                                        <= TestFilesystemOperation.MAX_CONTENT_LENGTH,
                        "content is required and must not exceed 1 MiB");
                checkArgument(request.hasOverwrite(), "overwrite is required");
                try (FSDataOutputStream output = fs.create(path, writeMode(request))) {
                    output.write(request.getContent());
                }
                break;
            case DELETE:
                checkArgument(request.hasRecursive(), "recursive is required");
                response.setDeleted(fs.delete(path, request.isRecursive()));
                break;
            case COPY:
                checkArgument(request.hasTargetPath(), "targetPath is required");
                checkArgument(request.hasOverwrite(), "overwrite is required");
                FsPath target = checkedPath(request.getTargetPath(), roots, true);
                checkArgument(!path.equals(target), "Copy source and target must differ");
                checkArgument(!fs.getFileStatus(path).isDir(), "Copy source must be a file");
                try (FSDataInputStream input = fs.open(path);
                        FSDataOutputStream output =
                                target.getFileSystem().create(target, writeMode(request))) {
                    byte[] buffer = new byte[64 * 1024];
                    int count;
                    while ((count = input.read(buffer)) != -1) {
                        if (count == 0) {
                            int value = input.read();
                            if (value < 0) {
                                break;
                            }
                            output.write(value);
                        } else {
                            output.write(buffer, 0, count);
                        }
                    }
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported filesystem operation");
        }
        return response;
    }

    private static FileSystem.WriteMode writeMode(TestFilesystemRequest request) {
        return request.isOverwrite()
                ? FileSystem.WriteMode.OVERWRITE
                : FileSystem.WriteMode.NO_OVERWRITE;
    }

    private static void addStatus(TestFilesystemResponse response, FileStatus status) {
        PbTestFileStatus file = response.addFile();
        file.setPath(status.getPath().toUri().toASCIIString())
                .setDirectory(status.isDir())
                .setLength(status.getLen())
                .setModificationTime(status.getModificationTime());
    }

    private static TestFilesystemResponse failure(String type, String message) {
        return new TestFilesystemResponse().setErrorType(type).setErrorMessage(message);
    }

    private static FsPath checkedPath(String value, List<FsPath> roots, boolean mutation)
            throws IOException {
        FsPath path = canonicalPath(value);
        URI uri = path.toUri();
        for (FsPath root : roots) {
            if (path.equals(root)) {
                checkArgument(!mutation, "Cannot modify a configured storage root");
                return path;
            }
        }
        for (FsPath root : roots) {
            URI rootUri = root.toUri();
            String prefix = rootUri.getPath();
            if (!prefix.endsWith("/")) {
                prefix += "/";
            }
            if (Objects.equals(uri.getScheme(), rootUri.getScheme())
                    && Objects.equals(uri.getAuthority(), rootUri.getAuthority())
                    && uri.getPath().startsWith(prefix)) {
                return path;
            }
        }
        throw new IllegalArgumentException("Path is outside configured storage roots");
    }

    private static FsPath canonicalPath(String value) throws IOException {
        checkArgument(value != null && !value.isEmpty(), "A complete filesystem URI is required");
        URI uri;
        try {
            uri = URI.create(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid filesystem URI");
        }
        checkArgument(
                uri.isAbsolute()
                        && !uri.isOpaque()
                        && uri.getPath() != null
                        && uri.getPath().startsWith("/")
                        && uri.getUserInfo() == null
                        && uri.getQuery() == null
                        && uri.getFragment() == null,
                "A filesystem URI without credentials, query or fragment is required");
        checkArgument(uri.getPath().indexOf('\\') < 0, "Backslashes are not supported");
        for (String segment : uri.getPath().split("/")) {
            checkArgument(
                    !segment.equals(".") && !segment.equals(".."),
                    "Relative path segments are not allowed");
        }
        if ("file".equals(uri.getScheme())) {
            return new FsPath("file", null, new File(uri).getCanonicalFile().toURI().getPath());
        }
        return new FsPath(uri.getScheme(), uri.getAuthority(), uri.getPath());
    }
}
