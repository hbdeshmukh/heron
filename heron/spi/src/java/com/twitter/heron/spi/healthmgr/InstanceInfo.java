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
import com.twitter.heron.spi.packing.PackingPlan.InstancePlan;


public class InstanceInfo {
  private final InstancePlan instance;
  private int containerId;
  private Set<MetricsInfo> metrics;

  public InstanceInfo(int containerId, InstancePlan instance, Set<MetricsInfo> metrics) {
    this.containerId = containerId;
    this.instance = instance;
    this.metrics = metrics;
  }

  public int getInstanceId() {
    return instance.getTaskId();
  }

  public String getInstanceNameId() {
    return instance.getComponentName() + ":" + instance.getTaskId();
  }

  public int getContainerId() {
    return containerId;
  }

  public Set<MetricsInfo> getMetrics() {
    return metrics;
  }

  public void updateMetrics(Set<MetricsInfo> newMetrics) {
    metrics.addAll(newMetrics);
  }

  public String toString() {
    return String.format("Instance %s in container %d has metric value %s",
        getInstanceNameId(), containerId, metrics.toString());
  }

  public boolean contains(String metric, String value) {
    if (metrics.contains(new MetricsInfo(metric, value))) {
      return true;
    }
    return false;
  }

  public boolean containsBelow(String metric, String value) {
    for(MetricsInfo current : metrics){
      if(current.getName().equals(metric)){
        if(Double.parseDouble(current.getValue()) <= Double.parseDouble(value)){
          return true;
        }
      }
    }
    return false;
  }

  public String getMetricValue(String metric) throws RuntimeException {
    for (MetricsInfo currentMetric : metrics) {
      if (currentMetric.getName().equals(metric)) {
        return currentMetric.getValue();
      }
    }
    throw new RuntimeException("No metric with name " + metric + "was found");
  }

}

