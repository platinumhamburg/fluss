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

package com.alibaba.fluss.metrics.reporter;

import com.alibaba.fluss.annotation.PublicEvolving;

import java.time.Duration;

/**
 * Interface for reporters that actively send out data periodically.
 *
 * @since 0.2
 */
@PublicEvolving
public interface ScheduledMetricReporter extends MetricReporter {

    /**
     * Report the current measurements. This method is called periodically by the metrics registry
     * that uses the reporter. This method must not block for a significant amount of time, any
     * reporter needing more time should instead run the operation asynchronously.
     */
    void report();

    /**
     * Return the interval at which metrics registry should call this reporter's {@link #report()}.
     */
    Duration scheduleInterval();
}
