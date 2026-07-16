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

package org.apache.fluss.flink.action.orphan.audit;

import org.apache.fluss.annotation.Internal;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;

/** Immutable, serializable configuration for external orphan audit reporters. */
@Internal
public final class AuditReporterSpec implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String runId;
    private final List<ReporterSpec> reporters;

    public AuditReporterSpec(String runId, List<ReporterSpec> reporters) {
        this.runId = validateRunId(Objects.requireNonNull(runId, "runId"));
        this.reporters =
                Collections.unmodifiableList(
                        new ArrayList<>(Objects.requireNonNull(reporters, "reporters")));
    }

    public String runId() {
        return runId;
    }

    public List<ReporterSpec> reporters() {
        return reporters;
    }

    private static String validateRunId(String runId) {
        UUID parsed;
        try {
            parsed = UUID.fromString(runId);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("runId");
        }
        if (!parsed.toString().equalsIgnoreCase(runId)) {
            throw new IllegalArgumentException("runId");
        }
        return runId;
    }

    /** Immutable configuration for one reporter factory. */
    @Internal
    public static final class ReporterSpec implements Serializable {

        private static final long serialVersionUID = 1L;
        private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("[a-z][a-z0-9_-]*");

        private final String identifier;
        private final boolean required;
        private final Map<String, String> options;

        public ReporterSpec(String identifier, boolean required, Map<String, String> options) {
            this.identifier = validateIdentifier(Objects.requireNonNull(identifier, "identifier"));
            this.required = required;
            this.options =
                    Collections.unmodifiableMap(
                            new LinkedHashMap<>(Objects.requireNonNull(options, "options")));
        }

        public String identifier() {
            return identifier;
        }

        public boolean required() {
            return required;
        }

        public Map<String, String> options() {
            return options;
        }

        private static String validateIdentifier(String identifier) {
            if (!IDENTIFIER_PATTERN.matcher(identifier).matches()) {
                throw new IllegalArgumentException("identifier");
            }
            return identifier;
        }
    }
}
