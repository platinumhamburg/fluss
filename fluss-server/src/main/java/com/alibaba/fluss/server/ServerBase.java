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

package com.alibaba.fluss.server;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.FlussException;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.plugin.PluginManager;
import com.alibaba.fluss.plugin.PluginUtils;
import com.alibaba.fluss.server.authorizer.Authorizer;
import com.alibaba.fluss.server.coordinator.CoordinatorServer;
import com.alibaba.fluss.server.exception.FlussParseException;
import com.alibaba.fluss.server.tablet.TabletServer;
import com.alibaba.fluss.server.utils.ConfigurationParserUtils;
import com.alibaba.fluss.server.utils.FatalErrorHandler;
import com.alibaba.fluss.server.utils.ShutdownHookUtil;
import com.alibaba.fluss.utils.AutoCloseableAsync;
import com.alibaba.fluss.utils.ExceptionUtils;
import com.alibaba.fluss.utils.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.lang.reflect.UndeclaredThrowableException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** An abstract base server class for {@link CoordinatorServer} & {@link TabletServer}. */
public abstract class ServerBase implements AutoCloseableAsync, FatalErrorHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ServerBase.class);

    private static final int SUCCESS_EXIT_CODE = 0;

    protected static final int FAILURE_EXIT_CODE = 1;

    private static final int STARTUP_FAILURE_RETURN_CODE = 2;

    private static final int RUNTIME_FAILURE_RETURN_CODE = 3;

    private static final long FATAL_ERROR_SHUTDOWN_TIMEOUT_MS = 10000L;

    private static final Duration INITIALIZATION_SHUTDOWN_TIMEOUT = Duration.ofSeconds(30L);

    protected static final long ZOOKEEPER_REGISTER_TOTAL_WAIT_TIME_MS = 60 * 1000L;
    protected static final long ZOOKEEPER_REGISTER_RETRY_INTERVAL_MS = 3 * 1000L;

    protected final Configuration conf;

    protected FileSystem remoteFileSystem;
    protected PluginManager pluginManager;

    protected ServerBase(Configuration conf) {
        this.conf = conf;
    }

    private Thread shutDownHook;

    protected static Configuration loadConfiguration(String[] args, String serverClassName) {
        try {
            return ConfigurationParserUtils.loadCommonConfiguration(args, serverClassName);
        } catch (FlussParseException fpe) {
            LOG.error("Could not load the configuration.", fpe);
            System.exit(FAILURE_EXIT_CODE);
            return null;
        }
    }

    protected static void startServer(ServerBase server) {
        String serverName = server.getServerName();
        LOG.info("Starting {}.", server.getServerName());
        try {
            server.start();
        } catch (Exception e) {
            LOG.error("Could not start {}.", serverName, e);
            System.exit(STARTUP_FAILURE_RETURN_CODE);
        }
        int returnCode;
        Throwable throwable = null;

        try {
            returnCode = server.getTerminationFuture().get().getExitCode();
        } catch (Throwable e) {
            throwable = ExceptionUtils.stripExecutionException(e);
            returnCode = RUNTIME_FAILURE_RETURN_CODE;
        }

        LOG.info("Terminating {} process with exit code {}.", serverName, returnCode, throwable);
        System.exit(returnCode);
    }

    public void start() throws Exception {
        try {
            addShutDownHook();

            // at first, we need to initialize the file system
            pluginManager = PluginUtils.createPluginManagerFromRootFolder(conf);
            FileSystem.initialize(conf, pluginManager);

            // get uri for remote data dir
            String remoteDir = conf.get(ConfigOptions.REMOTE_DATA_DIR);
            remoteFileSystem = new FsPath(remoteDir).getFileSystem();

            startServices();
        } catch (Throwable t) {
            final Throwable strippedThrowable =
                    ExceptionUtils.stripException(t, UndeclaredThrowableException.class);
            try {
                // clean up any partial state
                closeAsync(Result.FAILURE)
                        .get(INITIALIZATION_SHUTDOWN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                strippedThrowable.addSuppressed(e);
            }
            LOG.error("Could not start the {}.", getServerName(), strippedThrowable);
            throw new FlussException(
                    String.format("Failed to start the %s.", getServerName()), strippedThrowable);
        }
    }

    private void addShutDownHook() {
        shutDownHook =
                ShutdownHookUtil.addShutdownHook(
                        () -> this.closeAsync(Result.JVM_SHUTDOWN).join(), getServerName(), LOG);
    }

    @Override
    public void onFatalError(Throwable exception) {
        // todo, enrich coordinator server error like Flink
        LOG.error(
                "Fatal error occurred while running the {}. Shutting it down...",
                getServerName(),
                exception);
        if (ExceptionUtils.isJvmFatalError(exception)) {
            System.exit(-1);
        } else {
            closeAsync(Result.FAILURE);
            FutureUtils.orTimeout(
                    getTerminationFuture(),
                    FATAL_ERROR_SHUTDOWN_TIMEOUT_MS,
                    TimeUnit.MILLISECONDS,
                    String.format(
                            "Waiting for %s shutting down timed out after %s ms.",
                            getServerName(), FATAL_ERROR_SHUTDOWN_TIMEOUT_MS));
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        ShutdownHookUtil.removeShutdownHook(shutDownHook, getServerName(), LOG);
        return closeAsync(Result.SUCCESS).thenAccept(ignored -> {});
    }

    protected abstract void startServices() throws Exception;

    protected abstract CompletableFuture<Result> closeAsync(Result result);

    protected abstract CompletableFuture<Result> getTerminationFuture();

    protected abstract String getServerName();

    @VisibleForTesting
    public abstract @Nullable Authorizer getAuthorizer();

    /** Result for run {@link ServerBase}. */
    public enum Result {
        SUCCESS(SUCCESS_EXIT_CODE),
        JVM_SHUTDOWN(FAILURE_EXIT_CODE),
        FAILURE(FAILURE_EXIT_CODE);

        private final int exitCode;

        Result(int exitCode) {
            this.exitCode = exitCode;
        }

        public int getExitCode() {
            return exitCode;
        }
    }
}
