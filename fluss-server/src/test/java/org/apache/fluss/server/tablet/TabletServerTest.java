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

package org.apache.fluss.server.tablet;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.rpc.messages.ApiVersionsRequest;
import org.apache.fluss.rpc.messages.ApiVersionsResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.server.ServerBase;
import org.apache.fluss.server.ServerTestBase;
import org.apache.fluss.server.zk.data.ServerApiVersion;
import org.apache.fluss.server.zk.data.TabletServerRegistration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TabletServer}. */
class TabletServerTest extends ServerTestBase {

    private static final int SERVER_ID = 0;
    private static final String RACK = "cn-hangzhou-server10";
    private static @TempDir File tempDirForLog;

    private TabletServer server;

    @BeforeEach
    void before() throws Exception {
        Configuration conf = createTabletServerConfiguration();
        server = new TabletServer(conf);
        server.start();
    }

    @AfterEach
    void after() throws Exception {
        if (server != null) {
            server.close();
        }
    }

    @Override
    protected ServerBase getServer() {
        return server;
    }

    @Override
    protected ServerBase getStartFailServer() {
        Configuration configuration = createTabletServerConfiguration();
        // configure with a invalid port, the server should fail to start
        configuration.set(ConfigOptions.BIND_LISTENERS, "FLUSS://localhost:-12");
        return new TabletServer(configuration);
    }

    private static Configuration createTabletServerConfiguration() {
        Configuration configuration = createConfiguration();
        configuration.set(ConfigOptions.TABLET_SERVER_ID, SERVER_ID);
        configuration.set(ConfigOptions.TABLET_SERVER_RACK, RACK);
        configuration.setString(ConfigOptions.DATA_DIR, tempDirForLog.getAbsolutePath());
        return configuration;
    }

    @Override
    protected void checkAfterStartServer() throws Exception {
        // check the data put in zk after tablet server start
        Optional<TabletServerRegistration> optionalTabletServerRegistration =
                zookeeperClient.getTabletServer(SERVER_ID);
        assertThat(optionalTabletServerRegistration).isPresent();

        TabletServerRegistration tabletServerRegistration = optionalTabletServerRegistration.get();
        assertThat(tabletServerRegistration.getRack()).isEqualTo(RACK);
        verifyEndpoint(
                tabletServerRegistration.getEndpoints(), server.getRpcServer().getBindEndpoints());
        ApiVersionsResponse wireResponse =
                server.getTabletService().apiVersions(new ApiVersionsRequest()).get();
        assertThat(toVersionString(wireResponse))
                .isEqualTo(toVersionString(tabletServerRegistration.getApiVersions()));
        assertThat(tabletServerRegistration.getApiVersions())
                .extracting(ServerApiVersion::getApiKey)
                .contains(
                        ApiKeys.UPDATE_METADATA.id,
                        ApiKeys.NOTIFY_LEADER_AND_ISR.id,
                        ApiKeys.STOP_REPLICA.id)
                .doesNotContain(
                        ApiKeys.BEGIN_BULK_LOAD.id,
                        ApiKeys.COMMIT_BULK_LOAD.id,
                        ApiKeys.ABORT_BULK_LOAD.id,
                        ApiKeys.GET_BULK_LOAD_STATUS.id,
                        ApiKeys.COMMIT_KV_SNAPSHOT.id,
                        ApiKeys.COMMIT_REMOTE_LOG_MANIFEST.id);
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
                .map(
                        version ->
                                version.getApiKey()
                                        + ":"
                                        + version.getMinVersion()
                                        + ":"
                                        + version.getMaxVersion())
                .collect(Collectors.joining(","));
    }
}
