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
import org.apache.fluss.client.bulkload.BulkLoadBuildContext;
import org.apache.fluss.client.bulkload.BulkLoadClient;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.BulkLoadState;
import org.apache.fluss.metadata.BulkLoadStatus;
import org.apache.fluss.metadata.PhysicalTablePath;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Optional;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * The operator that begins the BulkLoad transaction of the primary-key bulk-load protocol; it is
 * the single entry point of transaction initiation and leftover-transaction cleanup in the BulkLoad
 * sink topology.
 *
 * <p>The operator consumes a single trigger element and checks the target through the public
 * BulkLoad client before beginning a fresh transaction. The BulkLoad sink topology contract
 * guarantees exactly one trigger element per operator instance; a second trigger element fails fast
 * via a state check:
 *
 * <ul>
 *   <li>when no transaction is in progress, it begins one;
 *   <li>when a {@code BEGUN} transaction remains, it aborts that transaction and begins a fresh
 *       one;
 *   <li>when a {@code COMMITTING} transaction remains, it fails because aborting a durable Commit
 *       decision is not safe.
 * </ul>
 *
 * <p>Every successful invocation emits exactly one frozen build context, which is broadcast to the
 * downstream Build operators. The connection is created lazily on the trigger element and is closed
 * by {@link #close()}, which is idempotent and safe even when no element was ever processed.
 */
final class BulkLoadBeginOperator extends AbstractStreamOperator<BulkLoadBuildContext>
        implements OneInputStreamOperator<Long, BulkLoadBuildContext> {

    private static final long serialVersionUID = 1L;

    private final Configuration flussConfig;
    private final PhysicalTablePath target;
    private final @Nullable Duration buildTimeout;
    private final Duration awaitTimeout;
    private transient @Nullable Connection connection;

    BulkLoadBeginOperator(
            Configuration flussConfig,
            PhysicalTablePath target,
            @Nullable Duration buildTimeout,
            Duration awaitTimeout) {
        this.flussConfig = checkNotNull(flussConfig, "Fluss configuration must not be null.");
        this.target = checkNotNull(target, "BulkLoad target must not be null.");
        this.buildTimeout = buildTimeout;
        this.awaitTimeout = checkNotNull(awaitTimeout, "BulkLoad await timeout must not be null.");
        checkArgument(
                !awaitTimeout.isNegative() && !awaitTimeout.isZero(),
                "BulkLoad await timeout must be positive.");
    }

    @Override
    public void processElement(StreamRecord<Long> element) throws Exception {
        checkState(
                connection == null,
                "The BulkLoad begin operator expects exactly one trigger element per operator"
                        + " instance.");
        Connection newConnection = ConnectionFactory.createConnection(flussConfig);
        this.connection = newConnection;
        try {
            BulkLoadClient client = newConnection.getBulkLoadClient();
            Optional<BulkLoadStatus> inProgress = client.getInProgressBulkLoad(target);
            if (inProgress.isPresent()) {
                BulkLoadStatus status = inProgress.get();
                if (status.getState() == BulkLoadState.BEGUN) {
                    BulkLoadStatus aborted = client.abort(status.getHandle());
                    checkState(
                            aborted.getState() == BulkLoadState.ABORTED,
                            "Aborting the leftover BulkLoad transaction returned state %s.",
                            aborted.getState());
                } else if (status.getState() == BulkLoadState.COMMITTING) {
                    throw new IllegalStateException(
                            "BulkLoad target "
                                    + target
                                    + " has a COMMITTING transaction that cannot be aborted.");
                } else {
                    throw new IllegalStateException(
                            "BulkLoad target "
                                    + target
                                    + " returned unexpected in-progress state "
                                    + status.getState()
                                    + '.');
                }
            }
            BulkLoadBuildContext context = client.begin(target, buildTimeout, awaitTimeout);
            output.collect(new StreamRecord<>(context));
        } catch (Exception e) {
            // Release the connection eagerly on failure; close() stays idempotent. A close failure
            // must never mask the original failure.
            try {
                closeConnection();
            } catch (Exception closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        closeConnection();
    }

    private void closeConnection() throws Exception {
        Connection toClose = connection;
        connection = null;
        if (toClose != null) {
            toClose.close();
        }
    }
}
