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

package org.apache.fluss.metadata;

import org.apache.fluss.annotation.PublicEvolving;

/**
 * Table type identifies what kind of table it is.
 *
 * <p>By default, a user created table is a {@link #DATA_TABLE}. {@link #INDEX_TABLE} is an internal
 * table managed by Fluss.
 *
 * @since 0.9
 */
@PublicEvolving
public enum TableType {
    /** A user-facing normal table. */
    DATA_TABLE,
    /** An internal table created for secondary index. */
    INDEX_TABLE

    // TODO: Future support for SYSTEM_TABLE
    // SYSTEM_TABLE will be used for internal system tables managed by Fluss,
    // such as metadata tables or operational tables.
}
