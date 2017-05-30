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

import org.apache.commons.math3.fitting.PolynomialCurveFitter;
import org.apache.commons.math3.fitting.WeightedObservedPoints;

import com.twitter.heron.spi.healthmgr.ComponentBottleneck;
import com.twitter.heron.spi.healthmgr.InstanceBottleneck;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsInfo;
import com.twitter.heron.spi.metricsmgr.sink.SinkVisitor;
import com.twitter.heron.spi.packing.InstanceId;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlan.InstancePlan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class SLAManagerUtils {

  private static final String BACKPRESSURE_METRIC = "__time_spent_back_pressure_by_compid";

  private SLAManagerUtils() {

  }

  public static HashMap<String, ComponentBottleneck> retrieveMetricValues(String metricName,
                                                                          String metricExtension,
                                                                          String component,
                                                                          SinkVisitor visitor,
                                                                          PackingPlan packingPlan) {
    HashMap<String, ComponentBottleneck> results = new HashMap<>();
    for (PackingPlan.ContainerPlan containerPlan : packingPlan.getContainers()) {
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
        String metricValue = getMetricValue(metricName, metricExtension,
            component, visitor, containerPlan,
            instancePlan);
        if (metricValue == null) {
          continue;
        }

        ComponentBottleneck currentBottleneck = results.get(instancePlan.getComponentName());
        if (currentBottleneck == null) {
          currentBottleneck = new ComponentBottleneck(instancePlan.getComponentName());
        }

        Set<MetricsInfo> metrics = new HashSet<>();
        MetricsInfo metric = new MetricsInfo(metricName, metricValue);
        metrics.add(metric);
        currentBottleneck.add(containerPlan.getId(), instancePlan, metrics);
        results.put(instancePlan.getComponentName(), currentBottleneck);
      }
    }
    return results;
  }

  /**
   *
   * @param metricName
   * @param metricExtension
   * @param component
   * @param visitor
   * @param startTime
   * @param endTime
   * @param packingPlan
   * @return A map where the key is the component name and the value is a list of {@link ComponentBottlenecks}.
   *         If the interval is made up of N small subintervals, the length of this list will be N (assuming that
   *         we have N such observations with us).
   */
  public static HashMap<String, List<ComponentBottleneck>> retrieveMetricValuesForInterval(String metricName,
                                                                                           String metricExtension,
                                                                                           String component,
                                                                                           SinkVisitor visitor,
                                                                                           long startTime,
                                                                                           long endTime,
                                                                                           PackingPlan packingPlan) {
    HashMap<String, List<ComponentBottleneck>> results = new HashMap<>();
    for (PackingPlan.ContainerPlan containerPlan : packingPlan.getContainers()) {
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
        List<String> metricValue = getMetricValueForInterval(metricName, metricExtension,
                component, visitor, containerPlan, startTime, endTime,
                instancePlan);
        if (metricValue != null) {
          // For each observation in metricValue, construct a ComponentBottleneck and append it to a list.
          if (!results.containsKey(instancePlan.getComponentName())) {
            // Create a list first.
            results.put(instancePlan.getComponentName(), new ArrayList<ComponentBottleneck>());
          }
          boolean listIsEmpty = results.get(instancePlan.getComponentName()).isEmpty();
          if (listIsEmpty) {
            // The ith entry represents the (i+1)th intervals since the beginning of the topology.
            // Construct a list of ComponentBottlenecks.
            List<ComponentBottleneck> componentBottlenecksList = results.get(instancePlan.getComponentName());
            // Create as many ComponentBottleneck instances in this list as the number of observations.
            for (int i = 0; i < metricValue.size(); i++) {
              componentBottlenecksList.add(new ComponentBottleneck(component));
            }
            for (int i = 0; i < metricValue.size(); i++) {
              Set<MetricsInfo> metricsForNewBottleneck = new HashSet<>();
              metricsForNewBottleneck.add(new MetricsInfo(metricName, metricValue.get(i)));
              componentBottlenecksList.get(i).add(containerPlan.getId(), instancePlan, metricsForNewBottleneck);
            }
          } else {
            // There is already a list of componentBottlenecks in the hash map.
            // Make sure the number of readings are the same.
            List<ComponentBottleneck> existingBottlenecks = results.get(instancePlan.getComponentName());
            assert existingBottlenecks.size() == metricValue.size();
            for (int metricCounter = 0; metricCounter < metricValue.size(); ++metricCounter) {
              // We first need to construct a set of MetricsInfo to construct a ComponentBottleneck object.
              Set<MetricsInfo> metricsForNewBottleneck = new HashSet<>();
              metricsForNewBottleneck.add(new MetricsInfo(metricName, metricValue.get(metricCounter)));
              // With the set of MetricsInfo just constructed, append a ComponentBottleneck in the hash map's value.
              results.get(instancePlan.getComponentName()).get(metricCounter).add(containerPlan.getId(), instancePlan, metricsForNewBottleneck);
            }
          }
        }
      }
    }
    return results;
  }

  public static List<String> getMetricValueForInterval(String metricName, String metricExtension,
                                                       String component, SinkVisitor visitor,
                                                       PackingPlan.ContainerPlan containerPlan,
                                                       long startTime, long endTime,
                                                       PackingPlan.InstancePlan instancePlan) {
    String name = "container_" + containerPlan.getId()
            + "_" + instancePlan.getComponentName()
            + "_" + instancePlan.getTaskId();
    String newMetricName;
    if(metricExtension.equals("")){
      newMetricName = metricName + "/" + name;
    }
    else{
      newMetricName = metricName + "/" + name + "/" + metricExtension;
    }
    Collection<MetricsInfo> metricsResults =
            visitor.getNextMetric(newMetricName, startTime, endTime, component);

    if (metricsResults.isEmpty()) {
      return null;
    }

    List<String> metrics = new ArrayList<>();
    for (MetricsInfo m : metricsResults) {
      metrics.add(m.getValue());
    }
    return metrics;
  }

  public static String getMetricValue(String metricName, String metricExtension,
                                      String component, SinkVisitor visitor,
                                      PackingPlan.ContainerPlan containerPlan,
                                      PackingPlan.InstancePlan instancePlan) {
    String name = "container_" + containerPlan.getId()
        + "_" + instancePlan.getComponentName()
        + "_" + instancePlan.getTaskId();
    String newMetricName;
    if(metricExtension.equals("")){
      newMetricName = metricName + "/" + name;
    }
    else{
      newMetricName = metricName + "/" + name + "/" + metricExtension;
    }
    Collection<MetricsInfo> metricsResults =
        visitor.getNextMetric(newMetricName, component);
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
    InstancePlan instance =
        new InstancePlan(new InstanceId(parts[2], Integer.parseInt(parts[3]), 0), null);
    currentBottleneck.add(Integer.parseInt(parts[1]), instance, metrics);
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

  /**
   * Evaluates whether the instances of the first component are contained
   * in the set of instances of the second component.
   *
   * @param first First component
   * @param second Second Component
   * @return true if the second component contains the instances of the first, false otherwise
   */
  public static boolean containsInstanceIds(ComponentBottleneck first, ComponentBottleneck second) {
    ArrayList<InstanceBottleneck> firstInstances = first.getInstances();
    ArrayList<InstanceBottleneck> secondInstances = second.getInstances();

    for (int i = 0; i < firstInstances.size(); i++) {
      if (!containsInstanceId(secondInstances,
          firstInstances.get(i).getInstanceData().getInstanceId())) {
        return false;
      }
    }
    return true;

  }

  public static boolean sameInstanceIds(ComponentBottleneck first, ComponentBottleneck second) {
    ArrayList<InstanceBottleneck> firstInstances = first.getInstances();
    ArrayList<InstanceBottleneck> secondInstances = second.getInstances();

    if (firstInstances.size() != secondInstances.size()) {
      return false;
    } else {
      for (int i = 0; i < firstInstances.size(); i++) {
        if (!containsInstanceId(secondInstances,
            firstInstances.get(i).getInstanceData().getInstanceId())) {
          return false;
        }
      }
      return true;
    }
  }

  public static boolean containsInstanceId(ArrayList<InstanceBottleneck> instances,
                                           int instanceId) {
    for (int i = 0; i < instances.size(); i++) {
      if (instances.get(i).getInstanceData().getInstanceId() == instanceId) {
        return true;
      }
    }
    return false;
  }

  public static boolean similarSumMetric(ComponentBottleneck first,
                                         ComponentBottleneck second, String metric, int threshold) {
    Double firstMetric = 0.0;
    Double secondMetric = 0.0;
    for (int j = 0; j < first.getInstances().size(); j++) {
      InstanceBottleneck currentInstance = first.getInstances().get(j);
      firstMetric += Double.parseDouble(
          currentInstance.getInstanceData().getMetricValue(metric));
    }

    for (int j = 0; j < second.getInstances().size(); j++) {
      InstanceBottleneck currentInstance = second.getInstances().get(j);
      secondMetric += Double.parseDouble(
          currentInstance.getInstanceData().getMetricValue(metric));
    }
    if (firstMetric / secondMetric > threshold || secondMetric / firstMetric > threshold) {
      return false;
    }
    return true;
  }

  public static boolean improvedMetricSum(ComponentBottleneck first,
                                         ComponentBottleneck second, String metric,
                                          double improvement) {

    Double firstMetric = 0.0;
    Double secondMetric = 0.0;
    for (int j = 0; j < first.getInstances().size(); j++) {
      InstanceBottleneck currentInstance = first.getInstances().get(j);
      firstMetric += Double.parseDouble(
          currentInstance.getInstanceData().getMetricValue(metric));
    }

    for (int j = 0; j < second.getInstances().size(); j++) {
      InstanceBottleneck currentInstance = second.getInstances().get(j);
      secondMetric += Double.parseDouble(
          currentInstance.getInstanceData().getMetricValue(metric));
    }
    System.out.println(firstMetric + " " + secondMetric + " " + improvement);
    if (secondMetric > firstMetric && secondMetric >= 0.9 * improvement * firstMetric ) {
      return true;
    }
    return false;
  }


  public static boolean similarMetric(ComponentBottleneck first,
                                      ComponentBottleneck second, String metric, int threshold) {
    for (int j = 0; j < first.getInstances().size(); j++) {
      int instanceId = first.getInstances().get(j).getInstanceData().getInstanceId();
      String firstValue = first.getInstances().get(j).getInstanceData()
          .getMetricValue(metric);
      if (!similarMetric(second.getInstances(), metric, instanceId, firstValue, threshold)) {
        return false;
      }
    }
    return true;
  }

  private static boolean similarMetric(ArrayList<InstanceBottleneck> instances, String metric,
                                       int instanceId, String value, int threshold) {
    boolean found = false;
    for (int i = 0; i < instances.size() && !found; i++) {
      InstanceBottleneck current = instances.get(i);
      if (current.getInstanceData().getInstanceId() == instanceId) {
        found = true;
        Double firstValue = Double.parseDouble(value);
        Double secondValue = Double.parseDouble(current.getInstanceData().getMetricValue(metric));
        if (firstValue / secondValue < threshold
            && secondValue / firstValue < threshold) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Returns true if the instances of the first component have more backpressure than
   * the same instances of the first component.
   */
  public static boolean reducedBackPressure(ComponentBottleneck first, ComponentBottleneck second) {
    for (int j = 0; j < first.getInstances().size(); j++) {
      int instanceId = first.getInstances().get(j).getInstanceData().getInstanceId();
      String backPressureValue = first.getInstances().get(j).getInstanceData()
          .getMetricValue(BACKPRESSURE_METRIC);
      if (Double.parseDouble(backPressureValue) > 0.0){
        if(!similarBackPressure(second.getInstances(), instanceId, backPressureValue)){
          return true;
        }
      }
    }
    return false;
  }

  public static boolean similarBackPressure(ComponentBottleneck first, ComponentBottleneck second) {
    for (int j = 0; j < first.getInstances().size(); j++) {
      int instanceId = first.getInstances().get(j).getInstanceData().getInstanceId();
      String backPressureValue = first.getInstances().get(j).getInstanceData()
          .getMetricValue(BACKPRESSURE_METRIC);
      if (!similarBackPressure(second.getInstances(), instanceId, backPressureValue)) {
        return false;
      }
    }
    return true;
  }

  private static boolean similarBackPressure(ArrayList<InstanceBottleneck> instances,
                                             int instanceId, String backPressureValue) {
    boolean found = false;
    for (int i = 0; i < instances.size() && !found; i++) {
      InstanceBottleneck current = instances.get(i);
      if (current.getInstanceData().getInstanceId() == instanceId) {
        found = true;
        if (Double.parseDouble(current.getInstanceData().getMetricValue(BACKPRESSURE_METRIC)) > 0
            && Double.parseDouble(backPressureValue) > 0) {
          return true;
        }
        if (Double.parseDouble(current.getInstanceData().getMetricValue(BACKPRESSURE_METRIC)) == 0
            && Double.parseDouble(backPressureValue) == 0) {
          return true;
        }
      }
    }
    return false;
  }
}
