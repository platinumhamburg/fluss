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

package org.apache.fluss.client.metadata;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TablePath;

/**
 * Factory for creating {@link SchemaGetter} instances for specific tables.
 *
 * <p>This interface is used to decouple the creation of SchemaGetter from the direct dependency on
 * Admin, allowing better testability and encapsulation.
 */
@Internal
@FunctionalInterface
public interface SchemaGetterFactory {

    /**
     * Creates a SchemaGetter for the specified table.
     *
     * @param tablePath the path of the table
     * @param schemaInfo the latest schema info of the table
     * @return a SchemaGetter instance for the table
     */
    SchemaGetter create(TablePath tablePath, SchemaInfo schemaInfo);
}
