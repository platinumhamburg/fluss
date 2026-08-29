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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.rpc.messages.ApiVersionsRequest;
import org.apache.fluss.rpc.messages.ApiVersionsResponse;
import org.apache.fluss.rpc.messages.PbApiVersion;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.server.ServerBase;
import org.apache.fluss.server.ServerTestBase;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.server.zk.data.ServerApiVersion;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CoordinatorServer} . */
class CoordinatorServerTest extends ServerTestBase {

    private CoordinatorServer coordinatorServer;

    @BeforeEach
    void beforeEach() throws Exception {
        coordinatorServer = startCoordinatorServer(createConfiguration());
        waitUntilCoordinatorServerElected();
    }

    @AfterEach
    void after() throws Exception {
        if (coordinatorServer != null) {
            coordinatorServer.close();
        }
    }

    @Override
    protected ServerBase getServer() {
        return coordinatorServer;
    }

    @Override
    protected ServerBase getStartFailServer() {
        Configuration configuration = createConfiguration();
        // CoordinatorServer starts leader services asynchronously in a separate election
        // thread. An invalid port wouldn't cause start() to throw because the port binding
        // happens asynchronously. Instead, use an empty ZK address to cause a synchronous
        // failure in startZookeeperClient() during start().
        configuration.setString(ConfigOptions.ZOOKEEPER_ADDRESS, "");
        return new CoordinatorServer(configuration);
    }

    @Override
    protected void checkAfterStartServer() throws Exception {
        assertThat(coordinatorServer.getRpcServer()).isNotNull();
        // check the data put in zk after coordinator server start
        Optional<CoordinatorAddress> optCoordinatorAddr =
                zookeeperClient.getCoordinatorLeaderAddress();
        assertThat(optCoordinatorAddr).isNotEmpty();
        verifyEndpoint(
                optCoordinatorAddr.get().getEndpoints(),
                coordinatorServer.getRpcServer().getBindEndpoints());

        List<ServerApiVersion> persistedApiVersions = optCoordinatorAddr.get().getApiVersions();
        ApiVersionsResponse wireResponse =
                coordinatorServer
                        .getCoordinatorService()
                        .apiVersions(new ApiVersionsRequest())
                        .get();
        assertThat(toVersionString(wireResponse)).isEqualTo(toVersionString(persistedApiVersions));
        assertThat(persistedApiVersions)
                .extracting(ServerApiVersion::getApiKey)
                .contains(
                        ApiKeys.BEGIN_BULK_LOAD.id,
                        ApiKeys.COMMIT_BULK_LOAD.id,
                        ApiKeys.ABORT_BULK_LOAD.id)
                .doesNotContain(
                        ApiKeys.UPDATE_METADATA.id,
                        ApiKeys.NOTIFY_LEADER_AND_ISR.id,
                        ApiKeys.STOP_REPLICA.id);
    }

    public void waitUntilCoordinatorServerElected() {
        waitUntil(
                () -> zookeeperClient.getCoordinatorLeaderAddress().isPresent(),
                Duration.ofSeconds(5),
                "Fail to wait coordinator server elected");
    }

    private static String toVersionString(List<ServerApiVersion> versions) {
        return versions.stream()
                .map(
                        version ->
                                version.getApiKey()
                                        + ":"
                                        + version.getMinVersion()
                                        + ":"
                                        + version.getMaxVersion())
                .collect(Collectors.joining(","));
    }

    private static String toVersionString(ApiVersionsResponse response) {
        return response.getApiVersionsList().stream()
                .map(CoordinatorServerTest::toVersionString)
                .collect(Collectors.joining(","));
    }

    private static String toVersionString(PbApiVersion version) {
        return version.getApiKey() + ":" + version.getMinVersion() + ":" + version.getMaxVersion();
    }
}
