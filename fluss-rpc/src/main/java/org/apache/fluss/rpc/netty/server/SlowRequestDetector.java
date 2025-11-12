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

package org.apache.fluss.rpc.netty.server;

import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.timer.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A timer task that monitors RPC request processing time and logs detailed diagnostic information
 * when requests exceed a configured threshold.
 *
 * <p>This detector captures:
 *
 * <ul>
 *   <li>Request context (API type, request ID, client address)
 *   <li>Timing breakdown (queue time, processing time so far)
 *   <li>Thread stack trace of the processing thread (if enabled)
 * </ul>
 *
 * <p>The detector is designed to be lightweight and only activates when a request is actually slow,
 * minimizing performance impact on normal request processing.
 */
class SlowRequestDetector extends TimerTask {
    private static final Logger LOG = LoggerFactory.getLogger(SlowRequestDetector.class);

    private final FlussRequest request;
    private final Thread processingThread;
    private final boolean dumpStack;
    private final long thresholdMs;
    private final AtomicBoolean executed = new AtomicBoolean(false);

    /**
     * Creates a slow request detector.
     *
     * @param request the request to monitor
     * @param processingThread the thread processing this request
     * @param thresholdMs the threshold in milliseconds after which the request is considered slow
     * @param dumpStack whether to dump the thread stack trace
     * @param delayMs the delay in milliseconds before checking if the request is slow
     */
    SlowRequestDetector(
            FlussRequest request,
            Thread processingThread,
            long thresholdMs,
            boolean dumpStack,
            long delayMs) {
        super(delayMs);
        this.request = request;
        this.processingThread = processingThread;
        this.thresholdMs = thresholdMs;
        this.dumpStack = dumpStack;
    }

    @Override
    public void run() {
        // Ensure this task only executes once even if there are race conditions
        if (!executed.compareAndSet(false, true)) {
            return;
        }

        // Check if request has already completed
        if (request.getResponseFuture().isDone()) {
            return;
        }

        long currentTimeMs = System.currentTimeMillis();
        long elapsedMs = currentTimeMs - request.getStartTimeMs();
        long queueTimeMs = request.getRequestDequeTimeMs() - request.getStartTimeMs();
        long processingTimeMs = currentTimeMs - request.getRequestDequeTimeMs();

        ApiKeys apiKey = ApiKeys.forId(request.getApiKey());
        String requestInfo =
                String.format(
                        "api=%s, requestId=%d, clientAddress=%s",
                        apiKey.name(), request.getRequestId(), request.getAddress());

        StringBuilder logMessage = new StringBuilder();
        logMessage
                .append("Slow RPC request detected: ")
                .append(requestInfo)
                .append("\n")
                .append("  Threshold: ")
                .append(thresholdMs)
                .append(" ms\n")
                .append("  Elapsed time: ")
                .append(elapsedMs)
                .append(" ms\n")
                .append("  Timing breakdown:\n")
                .append("    - Queue time: ")
                .append(queueTimeMs)
                .append(" ms\n")
                .append("    - Processing time (so far): ")
                .append(processingTimeMs)
                .append(" ms\n");

        // Add thread stack trace if enabled
        if (dumpStack && processingThread != null && processingThread.isAlive()) {
            logMessage
                    .append("  Processing thread stack trace:\n")
                    .append(formatThreadStack(processingThread));
        }

        LOG.warn(logMessage.toString());
    }

    /**
     * Formats the thread stack trace into a readable string.
     *
     * @param thread the thread to dump stack trace from
     * @return formatted stack trace string
     */
    private String formatThreadStack(Thread thread) {
        StringBuilder sb = new StringBuilder();
        sb.append("    Thread: ")
                .append(thread.getName())
                .append(" (")
                .append(thread.getState())
                .append(")\n");

        StackTraceElement[] stackTrace = thread.getStackTrace();
        if (stackTrace.length == 0) {
            sb.append("      <no stack trace available>\n");
        } else {
            for (StackTraceElement element : stackTrace) {
                sb.append("      at ").append(element.toString()).append("\n");
            }
        }
        return sb.toString();
    }
}
