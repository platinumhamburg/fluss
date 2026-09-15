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

import org.apache.fluss.rpc.messages.TestFilesystemRequest;
import org.apache.fluss.rpc.protocol.TestFilesystemOperation;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies filesystem operations across the public test client and real RPC transport. */
class TestFileSystemClientITCase {
    @RegisterExtension
    public static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setRemoteDirNames(Arrays.asList("remote-a", "remote-b"))
                    .build();

    private Connection connection;
    private TestFileSystemClient client;
    private String root;
    private String directory;

    @BeforeEach
    void setup() throws Exception {
        connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
        client = new TestFileSystemClient(connection);
        List<TestFileStatus> roots = client.list("").get();
        assertThat(roots).hasSize(3);
        root = roots.get(0).getPath();
        directory = root + "/filesystem-" + UUID.randomUUID();
    }

    @AfterEach
    void close() throws Exception {
        try {
            if (directory != null) {
                client.delete(directory, true).get();
            }
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    void testCrudAndReadBoundaries() throws Exception {
        String path = directory + "/nested/data";
        byte[] content = new byte[] {1, 2, 3, 4};
        client.write(path, content, false).get();
        TestFileStatus status = client.stat(path).get();
        assertThat(status.isDirectory()).isFalse();
        assertThat(status.getLength()).isEqualTo(4);
        assertThat(status.getModificationTime()).isPositive();
        assertThat(client.list(directory).get()).hasSize(1).allMatch(TestFileStatus::isDirectory);
        assertThat(client.read(path, 1, 2).get()).containsExactly((byte) 2, (byte) 3);
        assertThat(client.read(path, 3, 10).get()).containsExactly((byte) 4);
        assertThat(client.read(path, 4, 10).get()).isEmpty();
        assertThat(client.read(path, Long.MAX_VALUE, 10).get()).isEmpty();
        assertThat(client.read(path, 0, 0).get()).isEmpty();
        assertThatThrownBy(() -> client.write(path, new byte[] {9}, false).get())
                .hasRootCauseInstanceOf(IOException.class);
        assertThat(client.read(path, 0, 4).get()).isEqualTo(content);
        client.write(path, new byte[0], true).get();
        assertThat(client.stat(path).get().getLength()).isZero();
        assertThat(client.delete(path, false).get()).isTrue();
        assertThat(client.list(directory + "/nested").get()).isEmpty();
        assertThatThrownBy(() -> client.stat(path).get())
                .hasRootCauseInstanceOf(FileNotFoundException.class);
        assertThatThrownBy(() -> client.read(path, 0, 1).get())
                .hasRootCauseInstanceOf(FileNotFoundException.class);
        assertThatThrownBy(() -> client.list(path).get())
                .hasRootCauseInstanceOf(FileNotFoundException.class);
    }

    @Test
    void testReturnedUrisCanBeUsedForEncodedFileNames() throws Exception {
        String path = directory + "/space%20and%23hash";
        client.write(path, new byte[] {7}, false).get();
        String listed = client.list(directory).get().get(0).getPath();
        assertThat(listed).contains("space%20and%23hash");
        assertThat(client.read(listed, 0, 1).get()).containsExactly((byte) 7);
        assertThat(client.stat(listed).get().getLength()).isEqualTo(1);
        assertThat(client.delete(listed, false).get()).isTrue();
    }

    @Test
    void testCopyStreamsFileLargerThanRpcLimit() throws Exception {
        String source = directory + "/source";
        String target = directory + "/target";
        byte[] data = new byte[TestFilesystemOperation.MAX_CONTENT_LENGTH * 2 + 17];
        Arrays.fill(data, (byte) 37);
        // Seed a large server-side file; writes through the test RPC stay bounded.
        Files.createDirectories(Paths.get(URI.create(directory)));
        Files.write(Paths.get(URI.create(source)), data);
        client.copy(source, target, false).get();
        assertThat(client.stat(target).get().getLength()).isEqualTo(data.length);
        assertThat(Files.readAllBytes(Paths.get(URI.create(target)))).isEqualTo(data);
        assertThatThrownBy(() -> client.copy(source, target, false).get())
                .hasRootCauseInstanceOf(IOException.class);
        assertThatThrownBy(() -> client.copy(source, source, true).get())
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
        assertThat(client.stat(source).get().getLength()).isEqualTo(data.length);
        client.write(source, new byte[] {5}, true).get();
        client.copy(source, target, true).get();
        assertThat(client.read(target, 0, 10).get()).containsExactly((byte) 5);
    }

    @Test
    void testCopyBetweenConfiguredRoots() throws Exception {
        String otherDirectory =
                client.list("").get().get(1).getPath() + "/copy-" + UUID.randomUUID();
        try {
            client.write(directory + "/data", new byte[] {4, 5}, false).get();
            client.copy(directory + "/data", otherDirectory + "/data", false).get();
            assertThat(client.read(otherDirectory + "/data", 0, 10).get())
                    .containsExactly((byte) 4, (byte) 5);
        } finally {
            client.delete(otherDirectory, true).get();
        }
    }

    @Test
    void testRecursiveDeleteAndIoFailure() throws Exception {
        client.write(directory + "/nested/data", new byte[] {1}, false).get();
        assertThatThrownBy(() -> client.delete(directory, false).get())
                .hasRootCauseInstanceOf(IOException.class);
        assertThat(client.stat(directory + "/nested/data").get().getLength()).isEqualTo(1);
        assertThat(client.delete(directory, true).get()).isTrue();
        assertThat(client.delete(directory, true).get()).isFalse();
    }

    @Test
    void testRejectsRootMutationsAndEscapingPaths() throws Exception {
        client.write(directory + "/data", new byte[] {1}, false).get();
        for (String path :
                Arrays.asList(
                        root,
                        root + "/",
                        root + "-sibling/data",
                        root + "/../outside",
                        root + "/%2e%2e/outside",
                        "file:///",
                        "relative",
                        "file:///tmp/data?query=value")) {
            assertThatThrownBy(() -> client.write(path, new byte[0], true).get())
                    .hasRootCauseInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> client.delete(path, true).get())
                    .hasRootCauseInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> client.copy(directory + "/data", path, true).get())
                    .hasRootCauseInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> client.copy("file:///outside", directory + "/copy", true).get())
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
        assertThat(client.read(directory + "/data", 0, 1).get()).containsExactly((byte) 1);
    }

    @Test
    void testServerRejectsInvalidRequestsEvenWithoutClientValidation() throws Exception {
        TestFilesystemRequest request =
                new TestFilesystemRequest()
                        .setOperation(TestFilesystemOperation.WRITE.id())
                        .setPath(directory + "/large")
                        .setOverwrite(false)
                        .setContent(new byte[TestFilesystemOperation.MAX_CONTENT_LENGTH + 1]);
        assertThat(CLUSTER.newCoordinatorClient().testFilesystem(request).get().getErrorType())
                .isEqualTo("INVALID_ARGUMENT");
        request =
                new TestFilesystemRequest()
                        .setOperation(TestFilesystemOperation.READ.id())
                        .setPath(directory + "/large")
                        .setOffset(-1)
                        .setLength(1);
        assertThat(CLUSTER.newCoordinatorClient().testFilesystem(request).get().getErrorType())
                .isEqualTo("INVALID_ARGUMENT");
        request = new TestFilesystemRequest().setOperation(999).setPath(directory);
        assertThat(CLUSTER.newCoordinatorClient().testFilesystem(request).get().getErrorType())
                .isEqualTo("INVALID_ARGUMENT");
    }
}
