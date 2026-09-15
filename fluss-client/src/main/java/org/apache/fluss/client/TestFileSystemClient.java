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

package org.apache.fluss.client;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.TestFilesystemRequest;
import org.apache.fluss.rpc.messages.TestFilesystemResponse;
import org.apache.fluss.rpc.protocol.TestFilesystemOperation;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * File operations on a dedicated filesystem test server. The supplied connection owns all RPC
 * resources. Mutations are not automatically retried; after a timeout, inspect the target first.
 */
@Internal
public final class TestFileSystemClient {
    private final CoordinatorGateway gateway;

    /** Creates a client sharing an existing Fluss connection and its authentication. */
    public TestFileSystemClient(Connection connection) {
        checkArgument(connection instanceof FlussConnection, "A FlussConnection is required");
        this.gateway = ((FlussConnection) connection).testFilesystemGateway();
    }

    /** Lists direct children, or configured roots when path is empty. */
    public CompletableFuture<List<TestFileStatus>> list(String path) {
        return send(request(TestFilesystemOperation.LIST, path))
                .thenApply(
                        r ->
                                r.getFilesList().stream()
                                        .map(TestFileStatus::new)
                                        .collect(Collectors.toList()));
    }

    /** Reads file or directory attributes. */
    public CompletableFuture<TestFileStatus> stat(String path) {
        return send(request(TestFilesystemOperation.STAT, path))
                .thenApply(r -> new TestFileStatus(r.getFilesList().get(0)));
    }

    /** Reads at most length bytes from offset; each read is limited to 1 MiB. */
    public CompletableFuture<byte[]> read(String path, long offset, int length) {
        checkArgument(offset >= 0, "offset must be nonnegative");
        checkArgument(
                length >= 0 && length <= TestFilesystemOperation.MAX_CONTENT_LENGTH,
                "length must be between 0 and 1 MiB");
        return send(request(TestFilesystemOperation.READ, path).setOffset(offset).setLength(length))
                .thenApply(TestFilesystemResponse::getContent);
    }

    /** Creates or replaces an entire file of at most 1 MiB. */
    public CompletableFuture<Void> write(String path, byte[] content, boolean overwrite) {
        checkNotNull(content, "content");
        checkArgument(
                content.length <= TestFilesystemOperation.MAX_CONTENT_LENGTH,
                "content exceeds 1 MiB; use copy for larger existing files");
        return send(request(TestFilesystemOperation.WRITE, path)
                        .setContent(content)
                        .setOverwrite(overwrite))
                .thenApply(r -> null);
    }

    /** Deletes a file or directory and returns the filesystem result. */
    public CompletableFuture<Boolean> delete(String path, boolean recursive) {
        return send(request(TestFilesystemOperation.DELETE, path).setRecursive(recursive))
                .thenApply(TestFilesystemResponse::isDeleted);
    }

    /** Streams a file between configured roots on the server, without preserving mtime. */
    public CompletableFuture<Void> copy(String sourcePath, String targetPath, boolean overwrite) {
        return send(request(TestFilesystemOperation.COPY, sourcePath)
                        .setTargetPath(checkNotNull(targetPath, "targetPath"))
                        .setOverwrite(overwrite))
                .thenApply(r -> null);
    }

    private static TestFilesystemRequest request(TestFilesystemOperation operation, String path) {
        return new TestFilesystemRequest()
                .setOperation(operation.id())
                .setPath(checkNotNull(path, "path"));
    }

    private CompletableFuture<TestFilesystemResponse> send(TestFilesystemRequest request) {
        return gateway.testFilesystem(request)
                .thenApply(
                        response -> {
                            if (response.hasErrorType()) {
                                String message = response.getErrorMessage();
                                switch (response.getErrorType()) {
                                    case "NOT_FOUND":
                                        throw new CompletionException(
                                                new FileNotFoundException(message));
                                    case "ACCESS_DENIED":
                                        throw new CompletionException(
                                                new AccessDeniedException(message));
                                    case "INVALID_ARGUMENT":
                                        throw new IllegalArgumentException(message);
                                    default:
                                        throw new CompletionException(new IOException(message));
                                }
                            }
                            return response;
                        });
    }
}
