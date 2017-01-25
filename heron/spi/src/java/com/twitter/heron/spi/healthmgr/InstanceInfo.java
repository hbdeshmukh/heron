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
package com.twitter.heron.spi.healthmgr;

import java.util.Set;

import com.twitter.heron.spi.metricsmgr.metrics.MetricsInfo;


public class InstanceInfo {
  int containerId;
  int instanceId;
  Set<MetricsInfo> metrics;

  public InstanceInfo(int containerId, int instanceId, Set<MetricsInfo> metricValues) {
    this.containerId = containerId;
    this.instanceId = instanceId;
    this.metrics = metricValues;
  }

  public int getInstanceId() {
    return instanceId;
  }

  public int getContainerId() {
    return containerId;
  }

  public Set<MetricsInfo> getMetrics() {
    return metrics;
  }

  public String toString() {
    return String.format("Instance %d in container %d has metric value %s", instanceId, containerId,
        metrics.toString());
  }

  public boolean contains(String metric, String value) {
    if (metrics.contains(new MetricsInfo(metric, value))) {
      return true;
    }
    return false;
  }

}

