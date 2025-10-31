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

package org.apache.fluss.exception;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.metadata.TablePath;

/**
 * Exception for trying to operate on a table that doesn't exist.
 *
 * @since 0.1
 */
@PublicEvolving
public class TableNotExistException extends ApiException {

    public TableNotExistException(TablePath tablePath) {
        this("Table " + tablePath + " does not exist.");
    }

    public TableNotExistException(String message) {
        this(message, null);
    }

    public TableNotExistException(String message, Throwable cause) {
        super(message, cause);
    }
}
