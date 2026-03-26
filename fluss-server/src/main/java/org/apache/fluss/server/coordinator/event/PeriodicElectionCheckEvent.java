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

package org.apache.fluss.server.coordinator.event;

/**
 * Event that triggers a periodic election check for offline buckets.
 *
 * <p>This event is posted by a scheduled task and processed by the coordinator event thread. It
 * scans all {@link org.apache.fluss.server.coordinator.statemachine.BucketState#OfflineBucket}
 * buckets and attempts leader election as a safety net for event-driven election.
 */
public class PeriodicElectionCheckEvent implements CoordinatorEvent {

    @Override
    public String toString() {
        return "PeriodicElectionCheckEvent{}";
    }
}
