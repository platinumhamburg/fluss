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

package org.apache.fluss.server;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.ApiManager;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.server.zk.data.ServerApiVersion;
import org.apache.fluss.server.zk.data.TabletServerRegistration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/** Defines exact role-specific API capability ranges for server registration and discovery. */
@Internal
public final class ServerApiVersionSupport {

    private static final Set<ApiKeys> REQUIRED_COORDINATOR_BULK_LOAD_APIS =
            EnumSet.of(
                    ApiKeys.BEGIN_BULK_LOAD,
                    ApiKeys.COMMIT_BULK_LOAD,
                    ApiKeys.ABORT_BULK_LOAD,
                    ApiKeys.GET_IN_PROGRESS_BULK_LOAD);

    private static final Set<ApiKeys> REQUIRED_TABLET_BULK_LOAD_APIS =
            EnumSet.of(
                    ApiKeys.UPDATE_METADATA, ApiKeys.NOTIFY_LEADER_AND_ISR, ApiKeys.STOP_REPLICA);

    private static final List<ServerApiVersion> COORDINATOR_API_VERSIONS =
            toApiVersions(ServerType.COORDINATOR);

    private static final List<ServerApiVersion> TABLET_SERVER_API_VERSIONS =
            toApiVersions(ServerType.TABLET_SERVER);

    private ServerApiVersionSupport() {}

    /** Returns true only when every current Coordinator covers the complete required release. */
    public static boolean coordinatorReady(Collection<CoordinatorAddress> registrations) {
        if (registrations == null || registrations.isEmpty()) {
            return false;
        }
        for (CoordinatorAddress registration : registrations) {
            if (registration == null
                    || !covers(
                            registration.getApiVersions(),
                            REQUIRED_COORDINATOR_BULK_LOAD_APIS,
                            0)) {
                return false;
            }
        }
        return true;
    }

    /** Returns true only when every current TabletServer covers the complete required release. */
    public static boolean tabletServersReady(Collection<TabletServerRegistration> registrations) {
        if (registrations == null || registrations.isEmpty()) {
            return false;
        }
        for (TabletServerRegistration registration : registrations) {
            if (registration == null
                    || !covers(registration.getApiVersions(), REQUIRED_TABLET_BULK_LOAD_APIS, 1)) {
                return false;
            }
        }
        return true;
    }

    private static boolean covers(
            List<ServerApiVersion> actual, Set<ApiKeys> required, int requiredVersion) {
        if (actual == null || actual.isEmpty()) {
            return false;
        }
        for (ApiKeys key : required) {
            boolean covered = false;
            for (ServerApiVersion range : actual) {
                if (range.getApiKey() == key.id
                        && range.getMinVersion() <= requiredVersion
                        && range.getMaxVersion() >= requiredVersion) {
                    covered = true;
                    break;
                }
            }
            if (!covered) {
                return false;
            }
        }
        return true;
    }

    /** Returns the sorted API ranges handled by the given server role. */
    public static List<ServerApiVersion> apiVersions(ServerType provider) {
        if (provider == ServerType.COORDINATOR) {
            return COORDINATOR_API_VERSIONS;
        } else if (provider == ServerType.TABLET_SERVER) {
            return TABLET_SERVER_API_VERSIONS;
        }
        throw new IllegalArgumentException("Unsupported server type: " + provider);
    }

    private static List<ServerApiVersion> toApiVersions(ServerType provider) {
        List<ApiKeys> sortedApiKeys = new ArrayList<>(new ApiManager(provider).enabledApis());
        sortedApiKeys.sort(Comparator.comparingInt(api -> api.id));
        List<ServerApiVersion> apiVersions = new ArrayList<>(sortedApiKeys.size());
        for (ApiKeys apiKey : sortedApiKeys) {
            apiVersions.add(
                    new ServerApiVersion(
                            apiKey.id,
                            apiKey.lowestSupportedVersion,
                            apiKey.highestSupportedVersion));
        }
        return Collections.unmodifiableList(apiVersions);
    }
}
