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

import org.apache.fluss.record.LogRecordBatch;

import java.util.Map;

/** Converter interface for extracting state key-value pairs from LogRecordBatch. */
public interface StateConverter {

    /**
     * Converts a LogRecordBatch into state key-value pairs.
     *
     * @param batch the log record batch to convert
     * @return map of state key-value pairs, both key and value can be any Object type
     */
    Map<Object, Object> convert(LogRecordBatch batch);
}
