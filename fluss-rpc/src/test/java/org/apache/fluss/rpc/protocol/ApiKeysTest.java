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

package org.apache.fluss.rpc.protocol;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.apache.fluss.rpc.protocol.ApiKeys.ApiVisibility.PUBLIC;
import static org.assertj.core.api.Assertions.assertThat;

class ApiKeysTest {

    @Test
    void testAllApiKeys() {
        Set<Short> keys = new HashSet<>();
        for (ApiKeys api : ApiKeys.values()) {
            assertThat(keys.add(api.id)).isTrue();
            // reserve 0~999 for kafka protocol compatibility
            assertThat(api.id).isGreaterThanOrEqualTo((short) 1000);
            assertThat(api.lowestSupportedVersion).isLessThanOrEqualTo(api.highestSupportedVersion);
            assertThat(api.lowestSupportedVersion).isGreaterThanOrEqualTo((short) 0);
            assertThat(api.highestSupportedVersion).isGreaterThanOrEqualTo((short) 0);
        }
    }

    @Test
    void testBulkLoadApiKeysAndVersionRanges() {
        assertApi(ApiKeys.LIST_REMOTE_LOG_MANIFESTS, 1063, 0, 1, PUBLIC);
        assertApi(ApiKeys.LIST_KV_SNAPSHOTS, 1064, 0, 1, PUBLIC);
        assertApi(ApiKeys.BEGIN_BULK_LOAD, 1065, 0, 0, PUBLIC);
        assertApi(ApiKeys.COMMIT_BULK_LOAD, 1066, 0, 0, PUBLIC);
        assertApi(ApiKeys.ABORT_BULK_LOAD, 1067, 0, 0, PUBLIC);
        assertApi(ApiKeys.GET_IN_PROGRESS_BULK_LOAD, 1068, 0, 0, PUBLIC);
    }

    private static void assertApi(
            ApiKeys api,
            int id,
            int lowestVersion,
            int highestVersion,
            ApiKeys.ApiVisibility visibility) {
        assertThat(api.id).isEqualTo((short) id);
        assertThat(api.lowestSupportedVersion).isEqualTo((short) lowestVersion);
        assertThat(api.highestSupportedVersion).isEqualTo((short) highestVersion);
        assertThat(api.visibility).isEqualTo(visibility);
        assertThat(ApiKeys.forId(id)).isSameAs(api);
    }
}
