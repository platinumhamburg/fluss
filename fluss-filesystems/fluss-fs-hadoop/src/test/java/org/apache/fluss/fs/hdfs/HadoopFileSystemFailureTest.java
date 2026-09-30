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

package org.apache.fluss.fs.hdfs;

import org.apache.fluss.fs.FileSystemFailure;
import org.apache.fluss.fs.FileSystemOperationException;
import org.apache.fluss.fs.FileSystemPathNotFoundException;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.AccessControlException;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;
import java.nio.file.AccessDeniedException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HadoopFileSystemFailureTest {

    @Test
    void hdfsMissingPathIsConfirmed() {
        org.apache.hadoop.fs.FileSystem hadoop = mock(org.apache.hadoop.fs.FileSystem.class);
        when(hadoop.getUri()).thenReturn(URI.create("hdfs://namenode/"));
        FileNotFoundException failure = new FileNotFoundException("missing path");

        IOException normalized =
                filesystem(hadoop).normalize(failure, HadoopFileSystem.Operation.LIST_STATUS);

        assertThat(normalized)
                .isInstanceOf(FileSystemPathNotFoundException.class)
                .hasCause(failure);
        assertThat(((FileSystemFailure) normalized).resource())
                .isEqualTo(FileSystemFailure.Resource.PATH);
    }

    @Test
    void otherHadoopSchemesDoNotInferMissingPathFromExceptionClass() {
        org.apache.hadoop.fs.FileSystem hadoop = mock(org.apache.hadoop.fs.FileSystem.class);
        when(hadoop.getUri()).thenReturn(URI.create("oss://store/"));
        FileNotFoundException failure = new FileNotFoundException("missing path or marker");

        IOException normalized =
                filesystem(hadoop).normalize(failure, HadoopFileSystem.Operation.LIST_STATUS);

        assertThat(normalized).isInstanceOf(FileSystemOperationException.class).hasCause(failure);
        assertThat(((FileSystemFailure) normalized).resource())
                .isEqualTo(FileSystemFailure.Resource.UNKNOWN);
    }

    @Test
    void accessDeniedIsNotReportedAsMissing() {
        org.apache.hadoop.fs.FileSystem hadoop = mock(org.apache.hadoop.fs.FileSystem.class);
        AccessDeniedException failure = new AccessDeniedException("/some/path");

        IOException normalized =
                filesystem(hadoop).normalize(failure, HadoopFileSystem.Operation.LIST_STATUS);

        assertThat(((FileSystemFailure) normalized).kind())
                .isEqualTo(FileSystemFailure.Kind.PERMISSION_DENIED);
    }

    @Test
    void hdfsAccessControlExceptionIsPermissionDenied() {
        org.apache.hadoop.fs.FileSystem hadoop = mock(org.apache.hadoop.fs.FileSystem.class);
        when(hadoop.getUri()).thenReturn(URI.create("hdfs://namenode/"));

        IOException failure = new AccessControlException("access denied");
        IOException normalized =
                filesystem(hadoop).normalize(failure, HadoopFileSystem.Operation.LIST_STATUS);

        assertThat(((FileSystemFailure) normalized).kind())
                .isEqualTo(FileSystemFailure.Kind.PERMISSION_DENIED);
        assertThat(normalized).hasCause(failure);
    }

    @Test
    void wrappedHdfsAccessControlExceptionIsPermissionDenied() {
        org.apache.hadoop.fs.FileSystem hadoop = mock(org.apache.hadoop.fs.FileSystem.class);
        when(hadoop.getUri()).thenReturn(URI.create("hdfs://namenode/"));
        IOException failure =
                new RemoteException(AccessControlException.class.getName(), "access denied");

        IOException normalized =
                filesystem(hadoop).normalize(failure, HadoopFileSystem.Operation.LIST_STATUS);

        assertThat(((FileSystemFailure) normalized).kind())
                .isEqualTo(FileSystemFailure.Kind.PERMISSION_DENIED);
        assertThat(normalized).hasCause(failure);
    }

    @Test
    void hdfsCreateFailureDoesNotClaimThatTheTargetPathWasMissing() {
        org.apache.hadoop.fs.FileSystem hadoop = mock(org.apache.hadoop.fs.FileSystem.class);
        when(hadoop.getUri()).thenReturn(URI.create("hdfs://namenode/"));

        IOException normalized =
                filesystem(hadoop)
                        .normalize(
                                new FileNotFoundException("parent missing"),
                                HadoopFileSystem.Operation.CREATE);

        assertThat(normalized).isInstanceOf(FileSystemOperationException.class);
        assertThat(((FileSystemFailure) normalized).kind())
                .isEqualTo(FileSystemFailure.Kind.NOT_FOUND);
        assertThat(((FileSystemFailure) normalized).resource())
                .isEqualTo(FileSystemFailure.Resource.UNKNOWN);
    }

    @Test
    void interruptionIsNotAutomaticallyMarkedForRetry() {
        org.apache.hadoop.fs.FileSystem hadoop = mock(org.apache.hadoop.fs.FileSystem.class);
        IOException normalized =
                filesystem(hadoop)
                        .normalize(
                                new InterruptedIOException("interrupted"),
                                HadoopFileSystem.Operation.CREATE);

        assertThat(((FileSystemFailure) normalized).isTemporary()).isFalse();
    }

    private static HadoopFileSystem filesystem(org.apache.hadoop.fs.FileSystem hadoop) {
        return new HadoopFileSystem(hadoop) {
            @Override
            public ObtainedSecurityToken obtainSecurityToken() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
