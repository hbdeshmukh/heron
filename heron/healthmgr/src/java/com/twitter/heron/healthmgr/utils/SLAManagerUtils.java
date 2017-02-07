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
package com.twitter.heron.healthmgr.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import com.twitter.heron.spi.healthmgr.ComponentBottleneck;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsInfo;
import com.twitter.heron.spi.metricsmgr.sink.SinkVisitor;
import com.twitter.heron.spi.packing.PackingPlan;

public class SLAManagerUtils {

  public static HashMap<String, ComponentBottleneck> retrieveMetricValues(String metricName,
                                                                          String component,
                                                                          SinkVisitor visitor,
                                                                          PackingPlan packingPlan) {
    HashMap<String, ComponentBottleneck> results = new HashMap<>();
    for (PackingPlan.ContainerPlan containerPlan : packingPlan.getContainers()) {
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
        String metricValue = getMetricValue(metricName, component, visitor, containerPlan,
            instancePlan);
        if (metricValue == null) {
          continue;
        }
        MetricsInfo metric = new MetricsInfo(metricName, metricValue);
        ComponentBottleneck currentBottleneck;
        if (!results.containsKey(instancePlan.getComponentName())) {
          currentBottleneck = new ComponentBottleneck(instancePlan.getComponentName());
        } else {
          currentBottleneck = results.get(instancePlan.getComponentName());
        }
        Set<MetricsInfo> metrics = new HashSet<>();
        metrics.add(metric);
        currentBottleneck.add(containerPlan.getId(),
            instancePlan.getTaskId(), metrics);
        results.put(instancePlan.getComponentName(), currentBottleneck);
      }
    }
    return results;
  }

  public static String getMetricValue(String metricName, String component, SinkVisitor visitor,
                                      PackingPlan.ContainerPlan containerPlan,
                                      PackingPlan.InstancePlan instancePlan) {
    String name = "container_" + containerPlan.getId()
        + "_" + instancePlan.getComponentName()
        + "_" + instancePlan.getTaskId();
    //System.out.println(BACKPRESSURE_METRIC +"/" + name);
    Collection<MetricsInfo> metricsResults =
        visitor.getNextMetric(metricName + "/" + name, component);
    if (metricsResults.size() > 1) {
      throw new IllegalStateException(
          String.format("More than one metric (%d) received for %s", metricsResults.size(),
              metricName));
    }

    if (metricsResults.isEmpty()) {
      return null;
    }
    return metricsResults.iterator().next().getValue();
  }

  public static void updateComponentBottleneck(ComponentBottleneck currentBottleneck,
                                               String metricName,
                                               MetricsInfo metricsInfo) {
    String[] parts = metricsInfo.getName().split("_");
    Set<MetricsInfo> metrics = new HashSet<>();
    metrics.add(new MetricsInfo(metricName, metricsInfo.getValue()));
    currentBottleneck.add(Integer.parseInt(parts[1]),
        Integer.parseInt(parts[3]), metrics);
  }

  public static Double[] getDoubleDataPoints(Iterable<MetricsInfo> metricsResults) {
    ArrayList<Double> data = new ArrayList<>();
    for (MetricsInfo metricsInfo : metricsResults) {
      data.add(Double.parseDouble(metricsInfo.getValue()));
    }
    Double[] dataPoints = new Double[data.size()];
    data.toArray(dataPoints);
    return dataPoints;
  }

}
