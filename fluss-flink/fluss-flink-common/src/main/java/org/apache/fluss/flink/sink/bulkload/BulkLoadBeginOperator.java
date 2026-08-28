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
import org.apache.fluss.client.bulkload.BulkLoadBeginResult;
import org.apache.fluss.client.bulkload.BulkLoadBuildContext;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PhysicalTablePath;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import javax.annotation.Nullable;

import java.time.Duration;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * The operator that begins the BulkLoad transaction of the primary-key bulk-load protocol; it is
 * the single entry point of transaction initiation and leftover-transaction recovery in the
 * BulkLoad sink topology.
 *
 * <p>The operator consumes a single trigger element and begins or recovers the submission through
 * the public BulkLoad client. The BulkLoad sink topology contract guarantees exactly one trigger
 * element per operator instance; a second trigger element fails fast via a state check:
 *
 * <ul>
 *   <li>when this submission creates or recovers a {@code BEGUN} transaction, it emits the frozen
 *       build context, which is broadcast to the downstream Build operators;
 *   <li>when this submission recovers its {@code COMMITTING} transaction, it completes the durable
 *       decision through the BulkLoad client and emits nothing.
 * </ul>
 *
 * <p>The begin-and-recover sequence and the completion of a leftover transaction share the same
 * client-side await budget given by the await timeout. The connection is created lazily on the
 * trigger element and is closed by {@link #close()}, which is idempotent and safe even when no
 * element was ever processed.
 */
final class BulkLoadBeginOperator extends AbstractStreamOperator<BulkLoadBuildContext>
        implements OneInputStreamOperator<Long, BulkLoadBuildContext> {

    private static final long serialVersionUID = 1L;

    private final Configuration flussConfig;
    private final PhysicalTablePath target;
    private final String callerToken;
    private final @Nullable Duration buildTimeout;
    private final Duration awaitTimeout;
    private transient @Nullable Connection connection;

    BulkLoadBeginOperator(
            Configuration flussConfig,
            PhysicalTablePath target,
            String callerToken,
            @Nullable Duration buildTimeout,
            Duration awaitTimeout) {
        this.flussConfig = checkNotNull(flussConfig, "Fluss configuration must not be null.");
        this.target = checkNotNull(target, "BulkLoad target must not be null.");
        this.callerToken = checkNotNull(callerToken, "BulkLoad caller token must not be null.");
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
            BulkLoadBeginResult result =
                    newConnection
                            .getBulkLoadClient()
                            .begin(target, callerToken, buildTimeout, awaitTimeout);
            if (result.isBuildRequired()) {
                output.collect(new StreamRecord<>(result.getBuildContext()));
            }
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
