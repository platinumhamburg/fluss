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

import org.apache.fluss.flink.action.orphan.audit.AuditReporterSpec.ReporterSpec;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** Tests for {@link AuditReporterSpec}. */
class AuditReporterSpecTest {

    private static final String RUN_ID = "3b5939f1-9837-49d8-8a02-945273a0d7e2";
    private static final String CLUSTER_ID = "fluss-zjk-log";
    private static final String UPPERCASE_RUN_ID = "3B5939F1-9837-49D8-8A02-945273A0D7E2";
    private static final String NON_CANONICAL_UUID = "1-1-1-1-1";

    @Test
    void rejectsNullAndInvalidConstructorArguments() {
        assertThatThrownBy(() -> new AuditReporterSpec(null, Collections.emptyList()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("runId");
        assertThatThrownBy(() -> new AuditReporterSpec("not-a-uuid", Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("runId");
        assertThatThrownBy(() -> new AuditReporterSpec(RUN_ID, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("reporters");
        assertThatThrownBy(() -> new AuditReporterSpec(RUN_ID, "-cluster", Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("clusterId")
                .hasMessageNotContaining("-cluster");
        assertThatThrownBy(() -> new ReporterSpec(null, true, Collections.emptyMap()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("identifier");
        assertThatThrownBy(() -> new ReporterSpec("Jdbc", true, Collections.emptyMap()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("identifier");
        assertThatThrownBy(() -> new ReporterSpec("jdbc", true, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("options");
    }

    @Test
    void requiresCanonicalRunIdText() {
        assertThatThrownBy(() -> new AuditReporterSpec(NON_CANONICAL_UUID, Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("runId")
                .hasMessageNotContaining(NON_CANONICAL_UUID);
        AuditReporterSpec spec = new AuditReporterSpec(UPPERCASE_RUN_ID, Collections.emptyList());

        assertThat(spec.runId()).isEqualTo(UPPERCASE_RUN_ID);
    }

    @Test
    void defensivelyCopiesCollectionsAndPreservesInsertionOrder() {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("url", "jdbc:postgresql://audit-db:5432/fluss");
        options.put("username", "orphan_auditor");
        ReporterSpec jdbc = new ReporterSpec("jdbc", true, options);
        List<ReporterSpec> reporters = new ArrayList<>();
        reporters.add(jdbc);

        AuditReporterSpec spec = new AuditReporterSpec(RUN_ID, reporters);
        options.put("late-option", "must-not-appear");
        reporters.add(new ReporterSpec("sls", false, Collections.emptyMap()));

        assertThat(spec.runId()).isEqualTo(RUN_ID);
        assertThat(spec.reporters()).containsExactly(jdbc);
        assertThat(jdbc.identifier()).isEqualTo("jdbc");
        assertThat(jdbc.required()).isTrue();
        assertThat(jdbc.options())
                .containsExactly(
                        entry("url", "jdbc:postgresql://audit-db:5432/fluss"),
                        entry("username", "orphan_auditor"));
        assertThatThrownBy(
                        () ->
                                spec.reporters()
                                        .add(
                                                new ReporterSpec(
                                                        "sls", false, Collections.emptyMap())))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> jdbc.options().put("other", "value"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void roundTripsJavaSerializationWithoutReadingSecretFileContents(@TempDir Path tempDir)
            throws Exception {
        String fileContentMarker = "FILE_CONTENT_MUST_NOT_BE_READ";
        Path passwordFile = tempDir.resolve("password").toAbsolutePath();
        Files.write(
                passwordFile, Collections.singletonList(fileContentMarker), StandardCharsets.UTF_8);
        Map<String, String> options = new LinkedHashMap<>();
        options.put("url", "jdbc:postgresql://audit-db:5432/fluss");
        options.put("password-file", passwordFile.toString());
        AuditReporterSpec original =
                new AuditReporterSpec(
                        RUN_ID,
                        CLUSTER_ID,
                        Collections.singletonList(new ReporterSpec("jdbc", false, options)));

        byte[] serialized;
        try (ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                ObjectOutputStream output = new ObjectOutputStream(buffer)) {
            output.writeObject(original);
            output.flush();
            serialized = buffer.toByteArray();
        }

        String serializedText = new String(serialized, StandardCharsets.ISO_8859_1);
        assertThat(serializedText)
                .contains("jdbc")
                .contains("url")
                .contains("jdbc:postgresql://audit-db:5432/fluss")
                .contains(passwordFile.toString())
                .doesNotContain(fileContentMarker);

        AuditReporterSpec restored;
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(serialized))) {
            restored = (AuditReporterSpec) input.readObject();
        }

        assertThat(restored.runId()).isEqualTo(RUN_ID);
        assertThat(restored.clusterId()).isEqualTo(CLUSTER_ID);
        assertThat(restored.reporters()).hasSize(1);
        ReporterSpec restoredJdbc = restored.reporters().get(0);
        assertThat(restoredJdbc.identifier()).isEqualTo("jdbc");
        assertThat(restoredJdbc.required()).isFalse();
        assertThat(restoredJdbc.options())
                .containsExactly(
                        entry("url", "jdbc:postgresql://audit-db:5432/fluss"),
                        entry("password-file", passwordFile.toString()));
        assertThatThrownBy(() -> restored.reporters().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> restoredJdbc.options().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(Serializable.class.isAssignableFrom(AuditReporterSpec.class)).isTrue();
        assertThat(Serializable.class.isAssignableFrom(ReporterSpec.class)).isTrue();
        assertThat(ObjectStreamClass.lookup(AuditReporterSpec.class).getSerialVersionUID())
                .isEqualTo(1L);
        assertThat(ObjectStreamClass.lookup(ReporterSpec.class).getSerialVersionUID())
                .isEqualTo(1L);
    }
}
