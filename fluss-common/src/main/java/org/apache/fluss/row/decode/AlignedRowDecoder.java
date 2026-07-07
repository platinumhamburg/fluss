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

package org.apache.fluss.row.decode;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.row.aligned.AlignedRow;
import org.apache.fluss.types.DataType;

/** A decoder to decode {@link AlignedRow} from a byte array or memory segment. */
public class AlignedRowDecoder implements RowDecoder {
    private final int arity;

    public AlignedRowDecoder(DataType[] fieldDataTypes) {
        this.arity = fieldDataTypes.length;
    }

    @Override
    public AlignedRow decode(byte[] values) {
        AlignedRow row = new AlignedRow(arity);
        MemorySegment segment = MemorySegment.wrap(values);
        row.pointTo(segment, 0, values.length);
        return row;
    }

    @Override
    public AlignedRow decode(MemorySegment segment, int offset, int sizeInBytes) {
        AlignedRow row = new AlignedRow(arity);
        row.pointTo(segment, offset, sizeInBytes);
        return row;
    }
}
