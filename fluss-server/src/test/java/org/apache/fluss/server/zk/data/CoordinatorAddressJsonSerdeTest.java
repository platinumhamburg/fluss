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
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.utils.json.JsonSerdeTestBase;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link org.apache.fluss.server.zk.data.CoordinatorAddressJsonSerde}. */
public class CoordinatorAddressJsonSerdeTest extends JsonSerdeTestBase<CoordinatorAddress> {

    CoordinatorAddressJsonSerdeTest() {
        super(CoordinatorAddressJsonSerde.INSTANCE);
    }

    @Override
    protected CoordinatorAddress[] createObjects() {
        CoordinatorAddress coordinatorAddress =
                new CoordinatorAddress(
                        "1",
                        Arrays.asList(
                                new Endpoint("localhost", 1001, "CLIENT"),
                                new Endpoint("127.0.0.1", 9124, "FLUSS")),
                        Arrays.asList(
                                new ServerApiVersion((short) 1000, (short) 0, (short) 0),
                                new ServerApiVersion((short) 1016, (short) 0, (short) 1)));
        return new CoordinatorAddress[] {coordinatorAddress};
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":3,\"id\":\"1\",\"listeners\":\"CLIENT://localhost:1001,FLUSS://127.0.0.1:9124\",\"api_versions\":[{\"api_key\":1000,\"min_version\":0,\"max_version\":0},{\"api_key\":1016,\"min_version\":0,\"max_version\":1}]}"
        };
    }

    @Test
    void testCompatibility() throws IOException {
        JsonNode jsonInVersion1 =
                new ObjectMapper()
                        .readTree(
                                "{\"version\":1,\"id\":\"1\",\"host\":\"localhost\",\"port\":1001}"
                                        .getBytes(StandardCharsets.UTF_8));

        CoordinatorAddress coordinatorAddress =
                CoordinatorAddressJsonSerde.INSTANCE.deserialize(jsonInVersion1);
        CoordinatorAddress expectedCoordinator =
                new CoordinatorAddress(
                        "1", Endpoint.fromListenersString("CLIENT://localhost:1001"));
        assertEquals(coordinatorAddress, expectedCoordinator);

        JsonNode jsonInVersion2 =
                new ObjectMapper()
                        .readTree(
                                "{\"version\":2,\"id\":\"1\",\"listeners\":\"CLIENT://localhost:1001\"}"
                                        .getBytes(StandardCharsets.UTF_8));
        assertThat(
                        CoordinatorAddressJsonSerde.INSTANCE
                                .deserialize(jsonInVersion2)
                                .getApiVersions())
                .isEmpty();
    }

    @Test
    void testApiVersionsAreDefensivelyCopiedAndImmutable() {
        List<ServerApiVersion> versions = new ArrayList<>();
        versions.add(new ServerApiVersion((short) 1000, (short) 0, (short) 0));
        CoordinatorAddress address =
                new CoordinatorAddress(
                        "1",
                        Collections.singletonList(new Endpoint("localhost", 1001, "CLIENT")),
                        versions);

        versions.clear();

        assertThat(address.getApiVersions())
                .containsExactly(new ServerApiVersion((short) 1000, (short) 0, (short) 0));
        assertThatThrownBy(
                        () ->
                                address.getApiVersions()
                                        .add(
                                                new ServerApiVersion(
                                                        (short) 1001, (short) 0, (short) 0)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testServerApiVersionValueAndRangeContract() {
        ServerApiVersion version = new ServerApiVersion((short) 1000, (short) 0, (short) 1);

        assertThat(version)
                .isEqualTo(new ServerApiVersion((short) 1000, (short) 0, (short) 1))
                .hasSameHashCodeAs(new ServerApiVersion((short) 1000, (short) 0, (short) 1));
        assertThat(version).isLessThan(new ServerApiVersion((short) 1001, (short) 0, (short) 0));
        assertThatThrownBy(() -> new ServerApiVersion((short) 1000, (short) -1, (short) 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ServerApiVersion((short) 1000, (short) 1, (short) 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("malformedApiVersionLists")
    void testDeserializeRejectsMalformedApiVersionList(String description, String apiVersions)
            throws IOException {
        JsonNode node =
                new ObjectMapper()
                        .readTree(
                                ("{\"version\":3,\"id\":\"1\",\"listeners\":\"CLIENT://localhost:1001\","
                                                + "\"api_versions\":"
                                                + apiVersions
                                                + "}")
                                        .getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(() -> CoordinatorAddressJsonSerde.INSTANCE.deserialize(node))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidVersions")
    void testDeserializeRejectsInvalidVersion(String json, String expectedMessage)
            throws IOException {
        JsonNode node = new ObjectMapper().readTree(json);

        assertThatThrownBy(() -> CoordinatorAddressJsonSerde.INSTANCE.deserialize(node))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(expectedMessage);
    }

    private static Stream<Arguments> invalidVersions() {
        int futureVersion = CoordinatorAddressJsonSerde.VERSION + 1;
        return Stream.of(
                Arguments.of("{}", "CoordinatorAddress version must be an integer: missing."),
                Arguments.of(
                        "{\"version\":\"not-an-integer\"}",
                        "CoordinatorAddress version must be an integer: \"not-an-integer\"."),
                Arguments.of("{\"version\":0}", "Unsupported CoordinatorAddress version 0."),
                Arguments.of("{\"version\":-1}", "Unsupported CoordinatorAddress version -1."),
                Arguments.of(
                        "{\"version\":" + futureVersion + "}",
                        "Unsupported CoordinatorAddress version " + futureVersion + "."));
    }

    private static Stream<Arguments> malformedApiVersionLists() {
        return Stream.of(
                Arguments.of("missing list", "null"),
                Arguments.of(
                        "duplicate key",
                        "[{\"api_key\":1000,\"min_version\":0,\"max_version\":0},{\"api_key\":1000,\"min_version\":0,\"max_version\":0}]"),
                Arguments.of(
                        "descending key",
                        "[{\"api_key\":1001,\"min_version\":0,\"max_version\":0},{\"api_key\":1000,\"min_version\":0,\"max_version\":0}]"),
                Arguments.of(
                        "negative minimum",
                        "[{\"api_key\":1000,\"min_version\":-1,\"max_version\":0}]"),
                Arguments.of(
                        "minimum above maximum",
                        "[{\"api_key\":1000,\"min_version\":1,\"max_version\":0}]"),
                Arguments.of(
                        "maximum above protocol range",
                        "[{\"api_key\":1000,\"min_version\":0,\"max_version\":32768}]"));
    }
}
