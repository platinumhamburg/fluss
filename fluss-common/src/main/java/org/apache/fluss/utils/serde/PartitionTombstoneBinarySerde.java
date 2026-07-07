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

package org.apache.fluss.utils.serde;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.PartitionTombstone;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Binary serializer/deserializer for {@link PartitionTombstone} stored in ZooKeeper.
 *
 * <p>Wire format: {@code formatVersion(1B) + reserved(3B) + inBandVersion(8B) + floor(8B) +
 * explicitCount(4B) + explicit[i](8B * N)}.
 */
@Internal
public final class PartitionTombstoneBinarySerde {

    private static final byte FORMAT_VERSION = 1;
    private static final int HEADER_SIZE = 1 + 3 + 8 + 8 + 4;

    private PartitionTombstoneBinarySerde() {}

    /**
     * Serializes a {@link PartitionTombstone} to the compact binary wire format. Explicit partition
     * IDs are written in strict ascending order.
     *
     * @param tombstone the tombstone to serialize
     * @return the binary representation
     */
    public static byte[] serialize(PartitionTombstone tombstone) {
        Set<Long> explicitSet = tombstone.getExplicitSet();
        int size = HEADER_SIZE + explicitSet.size() * 8;
        ByteBuffer buf = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
        buf.put(FORMAT_VERSION);
        buf.put((byte) 0);
        buf.put((byte) 0);
        buf.put((byte) 0);
        buf.putLong(tombstone.getVersion());
        buf.putLong(tombstone.getFloor());
        buf.putInt(explicitSet.size());
        List<Long> sorted = new ArrayList<>(explicitSet);
        Collections.sort(sorted);
        for (long pid : sorted) {
            buf.putLong(pid);
        }
        return buf.array();
    }

    /**
     * Deserializes a {@link PartitionTombstone} from the compact binary wire format.
     *
     * @param bytes the binary representation produced by {@link #serialize}
     * @return the deserialized tombstone
     * @throws IllegalArgumentException if the format version is unsupported
     */
    public static PartitionTombstone deserialize(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        byte version = buf.get();
        if (version != FORMAT_VERSION) {
            throw new IllegalArgumentException(
                    "Unsupported PartitionTombstone format version: " + version);
        }
        buf.get();
        buf.get();
        buf.get();
        long inBandVersion = buf.getLong();
        long floor = buf.getLong();
        int count = buf.getInt();
        Set<Long> explicitSet = new HashSet<>(count);
        for (int i = 0; i < count; i++) {
            explicitSet.add(buf.getLong());
        }
        return new PartitionTombstone(floor, explicitSet, inBandVersion);
    }
}
