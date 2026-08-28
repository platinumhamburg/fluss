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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Json serializer and deserializer for {@link CoordinatorAddress}. */
@Internal
public class CoordinatorAddressJsonSerde
        implements JsonSerializer<CoordinatorAddress>, JsonDeserializer<CoordinatorAddress> {

    public static final CoordinatorAddressJsonSerde INSTANCE = new CoordinatorAddressJsonSerde();
    private static final String VERSION_KEY = "version";
    static final int VERSION = 3;

    private static final String ID = "id";
    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String LISTENERS = "listeners";
    private static final String API_VERSIONS = "api_versions";
    private static final String API_KEY = "api_key";
    private static final String MIN_VERSION = "min_version";
    private static final String MAX_VERSION = "max_version";

    private static void writeVersion(JsonGenerator generator) throws IOException {
        generator.writeNumberField(VERSION_KEY, VERSION);
    }

    @Override
    public void serialize(CoordinatorAddress coordinatorAddress, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();
        writeVersion(generator);
        generator.writeStringField(ID, coordinatorAddress.getId());
        generator.writeStringField(
                LISTENERS, Endpoint.toListenersString(coordinatorAddress.getEndpoints()));
        writeApiVersions(generator, coordinatorAddress.getApiVersions());
        generator.writeEndObject();
    }

    @Override
    public CoordinatorAddress deserialize(JsonNode node) {
        int version = readVersion(node);
        String id = node.get(ID).asText();
        List<Endpoint> endpoints;
        if (version == 1) {
            String host = node.get(HOST).asText();
            int port = node.get(PORT).asInt();
            endpoints = Collections.singletonList(new Endpoint(host, port, "CLIENT"));
        } else {
            endpoints = Endpoint.fromListenersString(node.get(LISTENERS).asText());
        }
        List<ServerApiVersion> apiVersions =
                version >= 3 ? readApiVersions(node) : Collections.emptyList();
        return new CoordinatorAddress(id, endpoints, apiVersions);
    }

    private static int readVersion(JsonNode node) {
        JsonNode versionNode = node.get(VERSION_KEY);
        String version = versionNode == null ? "missing" : versionNode.toString();
        checkArgument(
                node.has(VERSION_KEY)
                        && versionNode.isIntegralNumber()
                        && versionNode.canConvertToInt(),
                "CoordinatorAddress version must be an integer: %s.",
                version);
        int versionValue = versionNode.intValue();
        checkArgument(
                versionValue > 0 && versionValue <= VERSION,
                "Unsupported CoordinatorAddress version %s.",
                versionValue);
        return versionValue;
    }

    private static void writeApiVersions(
            JsonGenerator generator, List<ServerApiVersion> apiVersions) throws IOException {
        generator.writeArrayFieldStart(API_VERSIONS);
        for (ServerApiVersion apiVersion : apiVersions) {
            generator.writeStartObject();
            generator.writeNumberField(API_KEY, apiVersion.getApiKey());
            generator.writeNumberField(MIN_VERSION, apiVersion.getMinVersion());
            generator.writeNumberField(MAX_VERSION, apiVersion.getMaxVersion());
            generator.writeEndObject();
        }
        generator.writeEndArray();
    }

    private static List<ServerApiVersion> readApiVersions(JsonNode node) {
        JsonNode apiVersionsNode = node.get(API_VERSIONS);
        checkArgument(
                apiVersionsNode != null && apiVersionsNode.isArray(),
                "CoordinatorAddress api_versions must be an array.");
        List<ServerApiVersion> apiVersions = new ArrayList<>(apiVersionsNode.size());
        for (JsonNode apiVersionNode : apiVersionsNode) {
            int apiKey = readInt(apiVersionNode, API_KEY);
            int minVersion = readInt(apiVersionNode, MIN_VERSION);
            int maxVersion = readInt(apiVersionNode, MAX_VERSION);
            checkArgument(
                    apiKey >= 0 && apiKey <= Short.MAX_VALUE,
                    "API key must be in range 0..32767: %s.",
                    apiKey);
            checkArgument(
                    minVersion >= 0 && minVersion <= maxVersion && maxVersion <= Short.MAX_VALUE,
                    "API version range must satisfy 0 <= min <= max <= 32767: %s..%s.",
                    minVersion,
                    maxVersion);
            apiVersions.add(
                    new ServerApiVersion((short) apiKey, (short) minVersion, (short) maxVersion));
        }
        return apiVersions;
    }

    private static int readInt(JsonNode node, String field) {
        JsonNode value = node.get(field);
        checkArgument(
                value != null && value.isIntegralNumber() && value.canConvertToInt(),
                "API version field %s must be an integer.",
                field);
        return value.intValue();
    }
}
