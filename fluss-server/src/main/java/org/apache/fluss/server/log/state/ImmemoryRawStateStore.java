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

package org.apache.fluss.server.log.state;

import java.io.IOException;
import java.util.concurrent.ConcurrentSkipListMap;

/** ImmemoryRawStateStore. */
public class ImmemoryRawStateStore implements RawStateStore {

    private final ConcurrentSkipListMap<byte[], byte[]> map = new ConcurrentSkipListMap<>();

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        map.put(key, value);
    }

    @Override
    public byte[] get(byte[] key) throws IOException {
        return map.get(key);
    }

    @Override
    public void delete(byte[] key) throws IOException {
        map.remove(key);
    }

    @Override
    public long checkpoint(long recoverLogOffset) throws IOException {
        throw new UnsupportedOperationException();
    }
}
