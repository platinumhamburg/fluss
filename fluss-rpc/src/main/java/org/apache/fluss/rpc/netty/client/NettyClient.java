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

package org.apache.fluss.rpc.netty.client;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;
import org.apache.fluss.rpc.netty.NettyMetrics;
import org.apache.fluss.rpc.netty.NettyUtils;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.security.auth.AuthenticationFactory;
import org.apache.fluss.security.auth.ClientAuthenticator;
import org.apache.fluss.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.fluss.shaded.netty4.io.netty.buffer.PooledByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelOption;
import org.apache.fluss.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.fluss.timer.DefaultTimer;
import org.apache.fluss.timer.Timer;
import org.apache.fluss.utils.MapUtils;
import org.apache.fluss.utils.concurrent.FutureUtils;
import org.apache.fluss.utils.concurrent.ShutdownableThread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * A network client for asynchronous request/response network i/o. This is an internal class used to
 * implement the user-facing reader and writer.
 */
@ThreadSafe
public final class NettyClient implements RpcClient {

    private static final Logger LOG = LoggerFactory.getLogger(NettyClient.class);

    /** Netty's Bootstrap. */
    private final Bootstrap bootstrap;

    /** Netty's event group. */
    private final EventLoopGroup eventGroup;

    /**
     * Managed connections to Netty servers. The key is the server uid (e.g., "cs-2", "ts-3"), the
     * value is the connection.
     */
    private final Map<String, ServerConnection> connections;

    /** Metric groups for client. */
    private final ClientMetricGroup clientMetricGroup;

    private final Supplier<ClientAuthenticator> authenticatorSupplier;

    /**
     * Whether the NettyClient is used as inner network client (Communicating between Fluss's
     * servers).
     */
    private final boolean isInnerClient;

    /** Shared timer for all connections to handle request timeouts. */
    private final Timer sharedTimer;

    /** Background reaper to advance shared timer clock. */
    private final TimerReaper sharedTimerReaper;

    private volatile boolean isClosed = false;

    public NettyClient(
            Configuration conf, ClientMetricGroup clientMetricGroup, boolean isInnerClient) {
        this.connections = MapUtils.newConcurrentHashMap();

        // build bootstrap
        this.eventGroup =
                NettyUtils.newEventLoopGroup(
                        conf.getInt(ConfigOptions.NETTY_CLIENT_NUM_NETWORK_THREADS),
                        "fluss-netty-client");
        int connectTimeoutMs = (int) conf.get(ConfigOptions.CLIENT_CONNECT_TIMEOUT).toMillis();
        int connectionMaxIdle =
                (int) conf.get(ConfigOptions.NETTY_CONNECTION_MAX_IDLE_TIME).getSeconds();
        PooledByteBufAllocator pooledAllocator = PooledByteBufAllocator.DEFAULT;
        this.bootstrap =
                new Bootstrap()
                        .group(eventGroup)
                        .channel(NettyUtils.getClientSocketChannelClass(eventGroup))
                        .option(ChannelOption.ALLOCATOR, pooledAllocator)
                        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMs)
                        .option(ChannelOption.TCP_NODELAY, true)
                        .option(ChannelOption.SO_KEEPALIVE, true)
                        .handler(new ClientChannelInitializer(connectionMaxIdle));
        this.isInnerClient = isInnerClient;
        this.clientMetricGroup = clientMetricGroup;
        this.authenticatorSupplier = AuthenticationFactory.loadClientAuthenticatorSupplier(conf);

        // Initialize shared timer and timer reaper for all connections
        this.sharedTimer = new DefaultTimer("NettyClient-Timer");
        this.sharedTimerReaper = new TimerReaper();
        this.sharedTimerReaper.start();

        NettyMetrics.registerNettyMetrics(clientMetricGroup, pooledAllocator);
    }

    /**
     * Begin connecting to the given node, return true if we are already connected and ready to send
     * to that node.
     *
     * @param node The server node to check
     * @return True if we are ready to send to the given node.
     */
    @Override
    public boolean connect(ServerNode node) {
        checkArgument(!isClosed, "Netty client is closed.");
        return getOrCreateConnection(node).isReady();
    }

    /**
     * Disconnects the connection to the given server node, if there is one. Any inflight/pending
     * requests for this connection will receive disconnections.
     *
     * @param serverUid The uid of the server node
     * @return A future that completes when the connection is fully closed
     */
    @Override
    public CompletableFuture<Void> disconnect(String serverUid) {
        LOG.debug("Disconnecting from server {}.", serverUid);
        checkArgument(!isClosed, "Netty client is closed.");
        ServerConnection connection = connections.remove(serverUid);
        if (connection != null) {
            return connection.close();
        }
        return FutureUtils.completedVoidFuture();
    }

    /**
     * Check if we are currently ready to send another request to the given server but don't attempt
     * to connect if we aren't.
     *
     * @return true if the node is ready
     */
    @Override
    public boolean isReady(String serverUid) {
        checkArgument(!isClosed, "Netty client is closed.");
        ServerConnection connection = connections.get(serverUid);
        if (connection == null) {
            return false;
        }
        return connection.isReady();
    }

    /** Send an RPC request to the given server and return a future for the response. */
    @Override
    public CompletableFuture<ApiMessage> sendRequest(
            ServerNode node, ApiKeys apiKey, ApiMessage request) {
        checkArgument(!isClosed, "Netty client is closed.");
        return getOrCreateConnection(node).send(apiKey, request);
    }

    @Override
    public void close() throws Exception {
        try {
            isClosed = true;
            final List<CompletableFuture<Void>> shutdownFutures = new ArrayList<>();
            for (Map.Entry<String, ServerConnection> conn : connections.entrySet()) {
                if (connections.remove(conn.getKey(), conn.getValue())) {
                    shutdownFutures.add(conn.getValue().close());
                }
            }
            shutdownFutures.add(NettyUtils.shutdownGroup(eventGroup));

            // Shutdown shared timer and timer reaper
            if (sharedTimerReaper != null) {
                sharedTimerReaper.initiateShutdown();
            }
            if (sharedTimer != null) {
                sharedTimer.shutdown();
            }

            CompletableFuture.allOf(shutdownFutures.toArray(new CompletableFuture<?>[0]))
                    .get(10, TimeUnit.SECONDS);
            LOG.info("Netty client was shutdown successfully.");
        } catch (Exception e) {
            LOG.warn("Netty client shutdown failed: ", e);
        }
    }

    private ServerConnection getOrCreateConnection(ServerNode node) {
        String serverId = node.uid();
        return connections.computeIfAbsent(
                serverId,
                ignored -> {
                    LOG.debug("Creating connection to server {}.", node);
                    return new ServerConnection(
                            bootstrap,
                            node,
                            clientMetricGroup,
                            authenticatorSupplier.get(),
                            (con, ignore) -> connections.remove(serverId, con),
                            isInnerClient,
                            sharedTimer);
                });
    }

    @VisibleForTesting
    Map<String, ServerConnection> connections() {
        return connections;
    }

    /** Get shared timer for connections to use. */
    Timer getSharedTimer() {
        return sharedTimer;
    }

    /** A background reaper to advance shared timer clock. */
    private class TimerReaper extends ShutdownableThread {
        TimerReaper() {
            super("NettyClient-TimerReaper", false);
        }

        @Override
        public void doWork() {
            try {
                sharedTimer.advanceClock(200L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // Exit the loop when interrupted
                return;
            }
        }
    }
}
