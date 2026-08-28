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

package org.apache.fluss.server.zk.data;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.server.metadata.TabletServerResource;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * The register information of tablet server stored in {@link ZkData.ServerIdZNode}.
 *
 * @see TabletServerRegistrationJsonSerde for json serialization and deserialization.
 */
public class TabletServerRegistration {
    private final @Nullable String rack;
    private final List<Endpoint> endpoints;
    private final long registerTimestamp;
    private final TabletServerResource resource;
    private final List<ServerApiVersion> apiVersions;

    /** Creates a tablet-server registration without resource or API capability information. */
    public TabletServerRegistration(
            @Nullable String rack, List<Endpoint> endpoints, long registerTimestamp) {
        this(
                rack,
                endpoints,
                registerTimestamp,
                TabletServerResource.unknown(),
                Collections.emptyList());
    }

    public TabletServerRegistration(
            @Nullable String rack,
            List<Endpoint> endpoints,
            long registerTimestamp,
            TabletServerResource resource) {
        this(rack, endpoints, registerTimestamp, resource, Collections.emptyList());
    }

    /** Creates a tablet-server registration with resource and API capability information. */
    public TabletServerRegistration(
            @Nullable String rack,
            List<Endpoint> endpoints,
            long registerTimestamp,
            TabletServerResource resource,
            List<ServerApiVersion> apiVersions) {
        this.rack = rack;
        this.endpoints = Collections.unmodifiableList(new ArrayList<>(endpoints));
        this.registerTimestamp = registerTimestamp;
        this.resource = resource;
        this.apiVersions = ServerApiVersion.copyAndValidate(apiVersions);
    }

    public List<Endpoint> getEndpoints() {
        return endpoints;
    }

    public long getRegisterTimestamp() {
        return registerTimestamp;
    }

    public @Nullable String getRack() {
        return rack;
    }

    public TabletServerResource getResource() {
        return resource;
    }

    /** Returns the immutable advertised API capabilities. */
    public List<ServerApiVersion> getApiVersions() {
        return apiVersions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TabletServerRegistration that = (TabletServerRegistration) o;
        return registerTimestamp == that.registerTimestamp
                && Objects.equals(endpoints, that.endpoints)
                && Objects.equals(rack, that.rack)
                && Objects.equals(resource, that.resource)
                && Objects.equals(apiVersions, that.apiVersions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(endpoints, registerTimestamp, rack, resource, apiVersions);
    }

    @Override
    public String toString() {
        return "TabletServerRegistration{"
                + "endpoints="
                + endpoints
                + ", registerTimestamp="
                + registerTimestamp
                + ", rack='"
                + rack
                + "', resource="
                + resource
                + ", apiVersions="
                + apiVersions
                + '}';
    }
}
