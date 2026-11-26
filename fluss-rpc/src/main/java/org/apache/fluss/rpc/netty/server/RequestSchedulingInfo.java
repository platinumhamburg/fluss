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

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Optional server-side scheduling inputs. Deadlines use the monotonic clock of this JVM. */
public final class RequestSchedulingInfo {
    public static final RequestSchedulingInfo DEFAULT =
            new RequestSchedulingInfo(0, Long.MAX_VALUE, 1);
    private final long estimatedNanos;
    private final long deadlineNanos;
    private final double weight;

    /** Creates hints. A zero estimate uses the server estimator; MAX_VALUE means no deadline. */
    public RequestSchedulingInfo(long estimatedNanos, long deadlineNanos, double weight) {
        checkArgument(estimatedNanos >= 0, "Negative service estimate");
        checkArgument(Double.isFinite(weight) && weight > 0, "Weight must be finite and positive");
        this.estimatedNanos = estimatedNanos;
        this.deadlineNanos = deadlineNanos;
        this.weight = weight;
    }

    /** Returns the estimated synchronous service time, or zero to use the server estimator. */
    public long estimatedNanos() {
        return estimatedNanos;
    }

    /** Returns the monotonic deadline, or MAX_VALUE when absent. */
    public long deadlineNanos() {
        return deadlineNanos;
    }

    /** Returns the service weight for this connection's request. */
    public double weight() {
        return weight;
    }
}
