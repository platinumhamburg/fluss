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

package org.apache.fluss.flink.sink.bulkload;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.bulkload.BulkLoadBucketFiles;
import org.apache.fluss.client.bulkload.BulkLoadBuildContext;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.adapter.SinkAdapter;
import org.apache.fluss.utils.InstantiationUtils;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Terminal sink that hands all bucket files to Flink's standard committer at end of input. */
final class BulkLoadCommitSink extends SinkAdapter<BulkLoadCommittable>
        implements SupportsCommitter<BulkLoadCommittable> {

    private static final long serialVersionUID = 1L;

    private final Configuration flussConfig;
    private final Duration awaitTimeout;

    BulkLoadCommitSink(Configuration flussConfig, Duration awaitTimeout) {
        this.flussConfig = checkNotNull(flussConfig, "Fluss configuration must not be null.");
        this.awaitTimeout = checkNotNull(awaitTimeout, "BulkLoad await timeout must not be null.");
        checkArgument(
                !awaitTimeout.isNegative() && !awaitTimeout.isZero(),
                "BulkLoad await timeout must be positive.");
    }

    @Override
    protected SinkWriter<BulkLoadCommittable> createWriter(
            MailboxExecutor mailboxExecutor, SinkWriterMetricGroup metricGroup, int subtaskIndex) {
        return new CommitWriter();
    }

    @Override
    public Committer<BulkLoadCommittable> createCommitter(CommitterInitContext context) {
        return new BulkLoadCommitter(flussConfig, awaitTimeout);
    }

    @Override
    public SimpleVersionedSerializer<BulkLoadCommittable> getCommittableSerializer() {
        return new BulkLoadCommittableSerializer();
    }

    private static final class CommitWriter
            implements CommittingSinkWriter<BulkLoadCommittable, BulkLoadCommittable> {

        private final List<BulkLoadCommittable> committables = new ArrayList<>();
        private boolean endOfInput;

        @Override
        public void write(BulkLoadCommittable committable, Context context) {
            committables.add(committable);
        }

        @Override
        public void flush(boolean endOfInput) {
            this.endOfInput = endOfInput;
        }

        @Override
        public Collection<BulkLoadCommittable> prepareCommit() {
            if (!endOfInput) {
                return Collections.emptyList();
            }
            List<BulkLoadCommittable> prepared = new ArrayList<>(committables);
            committables.clear();
            return prepared;
        }

        @Override
        public void close() {
            committables.clear();
        }
    }

    private static final class BulkLoadCommitter implements Committer<BulkLoadCommittable> {

        private final Configuration flussConfig;
        private final Duration awaitTimeout;

        private BulkLoadCommitter(Configuration flussConfig, Duration awaitTimeout) {
            this.flussConfig = flussConfig;
            this.awaitTimeout = awaitTimeout;
        }

        @Override
        public void commit(Collection<CommitRequest<BulkLoadCommittable>> requests)
                throws IOException, InterruptedException {
            BulkLoadBuildContext buildContext = null;
            List<BulkLoadBucketFiles> bucketFiles = new ArrayList<>(requests.size());
            for (CommitRequest<BulkLoadCommittable> request : requests) {
                BulkLoadCommittable committable = request.getCommittable();
                BulkLoadBuildContext received = committable.getContext();
                if (buildContext == null) {
                    buildContext = received;
                } else {
                    checkState(
                            buildContext.equals(received),
                            "BulkLoad committer received files from different transactions.");
                }
                bucketFiles.add(committable.getBucketFiles());
            }
            if (buildContext == null) {
                return;
            }
            try (Connection connection = ConnectionFactory.createConnection(flussConfig)) {
                connection.getBulkLoadClient().commit(buildContext, bucketFiles, awaitTimeout);
            } catch (InterruptedException failure) {
                throw failure;
            } catch (Exception failure) {
                throw failure instanceof IOException
                        ? (IOException) failure
                        : new IOException("Failed to commit the BulkLoad transaction.", failure);
            }
        }

        @Override
        public void close() {}
    }

    private static final class BulkLoadCommittableSerializer
            implements SimpleVersionedSerializer<BulkLoadCommittable> {

        private static final int VERSION = 1;

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public byte[] serialize(BulkLoadCommittable committable) throws IOException {
            return InstantiationUtils.serializeObject(committable);
        }

        @Override
        public BulkLoadCommittable deserialize(int version, byte[] serialized) throws IOException {
            if (version != VERSION) {
                throw new IOException(
                        "Unsupported BulkLoad committable serializer version " + version + '.');
            }
            try {
                return InstantiationUtils.deserializeObject(
                        serialized, BulkLoadCommittable.class.getClassLoader());
            } catch (ClassNotFoundException failure) {
                throw new IOException("Failed to deserialize a BulkLoad committable.", failure);
            }
        }
    }
}
