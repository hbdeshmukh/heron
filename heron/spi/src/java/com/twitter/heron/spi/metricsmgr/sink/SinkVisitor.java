// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.spi.metricsmgr.sink;

import java.util.Collection;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsInfo;


/**
 * Accesses records from a given sink.
 */
public interface SinkVisitor extends AutoCloseable {


  /**
   * Initialize the sinkVisitor by associating it with a metrics collector and a topology.
   */
  void initialize(Config conf, Config runtime);


  /**
   * Retrieve the result of a metric
   *
   * @return The metric name
   */
  Collection<MetricsInfo> getNextMetric(String metric, String... component);

  /**
   * Retrieve the result of a metric during a specified duration.
   *
   * @param metric The name of the metric.
   * @param startTime The start time stamp of the duration.
   * @param endTime The end time stamp of the duration.
   * @param component
   * @return The name of the metric.
   */
  Collection<MetricsInfo> getNextMetric(String metric, long startTime, long endTime, String... component);

  /**
   * Close the sink visitor
   */
  void close();
}
