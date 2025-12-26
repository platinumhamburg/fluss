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

import java.util.Locale;

/**
 * Aggregation function for aggregate merge engine.
 *
 * <p>This enum represents all supported aggregation functions that can be applied to non-primary
 * key columns in aggregation merge engine tables.
 */
@PublicEvolving
public enum AggFunction {
    // Numeric aggregation
    SUM("sum"),
    PRODUCT("product"),
    MAX("max"),
    MIN("min"),

    // Value selection
    LAST_VALUE("last_value"),
    LAST_VALUE_IGNORE_NULLS("last_value_ignore_nulls"),
    FIRST_VALUE("first_value"),
    FIRST_VALUE_IGNORE_NULLS("first_value_ignore_nulls"),

    // String aggregation
    LISTAGG("listagg"),
    STRING_AGG("string_agg"), // Alias for LISTAGG - maps to same factory

    // Boolean aggregation
    BOOL_AND("bool_and"),
    BOOL_OR("bool_or");

    private final String identifier;

    AggFunction(String identifier) {
        this.identifier = identifier;
    }

    /**
     * Returns the identifier string for this aggregation function.
     *
     * @return the identifier string
     */
    public String getIdentifier() {
        return identifier;
    }

    /**
     * Converts a string to an AggFunction enum value.
     *
     * <p>This method supports multiple naming formats:
     *
     * <ul>
     *   <li>Underscore format: "last_value_ignore_nulls"
     *   <li>Hyphen format: "last-value-ignore-nulls"
     *   <li>Case insensitive matching
     * </ul>
     *
     * <p>Note: For string_agg, this will return STRING_AGG enum, but the server-side factory will
     * map it to the same implementation as listagg.
     *
     * @param name the aggregation function name
     * @return the AggFunction enum value, or null if not found
     */
    public static AggFunction fromString(String name) {
        if (name == null || name.trim().isEmpty()) {
            return null;
        }

        // Normalize the input: convert hyphens to underscores and lowercase
        String normalized = name.replace('-', '_').toLowerCase(Locale.ROOT).trim();

        // Try direct match with identifier
        for (AggFunction aggFunc : values()) {
            if (aggFunc.identifier.equals(normalized)) {
                return aggFunc;
            }
        }

        return null;
    }

    /**
     * Converts this AggFunction to its string identifier.
     *
     * @return the identifier string
     */
    @Override
    public String toString() {
        return identifier;
    }
}
