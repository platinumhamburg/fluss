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

package org.apache.fluss.server.kv;

import org.apache.fluss.record.WriterKey;
import org.apache.fluss.row.BinaryRow;

import javax.annotation.Nullable;

/** Table-owned validation invoked by the generic V1 KV write path under its write lock. */
public interface KvWriteGuard {

    /** Whether the V1 batch should proceed to WriterState or succeed without applying. */
    enum Decision {
        APPLY,
        NO_OP
    }

    Decision beforeWriterState(WriterKey writerKey) throws Exception;

    void validateRecord(WriterKey writerKey, byte[] key, @Nullable BinaryRow value)
            throws Exception;

    KvWriteGuard ACCEPT_ALL =
            new KvWriteGuard() {
                @Override
                public Decision beforeWriterState(WriterKey writerKey) {
                    return Decision.APPLY;
                }

                @Override
                public void validateRecord(
                        WriterKey writerKey, byte[] key, @Nullable BinaryRow value) {}
            };
}
