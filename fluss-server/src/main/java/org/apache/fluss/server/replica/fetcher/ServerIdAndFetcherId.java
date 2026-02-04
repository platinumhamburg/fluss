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

package org.apache.fluss.server.replica.fetcher;

import java.util.Objects;

/** Class to represent server id and fetcher id. */
public class ServerIdAndFetcherId {
    private final int serverId;
    private final int fetcherId;

    public ServerIdAndFetcherId(int serverId, int fetcherId) {
        this.serverId = serverId;
        this.fetcherId = fetcherId;
    }

    public int getServerId() {
        return serverId;
    }

    public int getFetcherId() {
        return fetcherId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ServerIdAndFetcherId that = (ServerIdAndFetcherId) o;
        return serverId == that.serverId && fetcherId == that.fetcherId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverId, fetcherId);
    }

    @Override
    public String toString() {
        return "ServerIdAndFetcherId{serverId=" + serverId + ", fetcherId=" + fetcherId + '}';
    }
}
