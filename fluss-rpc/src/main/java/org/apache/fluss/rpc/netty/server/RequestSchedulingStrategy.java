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

/** Waiting protection and ordering decisions, independent of queue and resource ownership. */
public interface RequestSchedulingStrategy {

    /** Whether an older waiting request should block later conflicting candidates. */
    boolean protects(long waitingNanos, long protectionNanos);

    /** Whether the first candidate should precede the second in registration order. */
    boolean precedes(long firstSequence, long secondSequence);
}
