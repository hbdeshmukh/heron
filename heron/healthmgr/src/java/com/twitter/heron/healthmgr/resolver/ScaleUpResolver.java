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
package com.twitter.heron.healthmgr.resolver;

import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.healthmgr.utils.SLAManagerUtils;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.healthmgr.ComponentBottleneck;
import com.twitter.heron.spi.healthmgr.Diagnosis;
import com.twitter.heron.spi.healthmgr.IResolver;
import com.twitter.heron.spi.healthmgr.InstanceBottleneck;
import com.twitter.heron.spi.packing.IRepacking;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlanProtoDeserializer;
import com.twitter.heron.spi.packing.PackingPlanProtoSerializer;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.ReflectionUtils;

public class ScaleUpResolver implements IResolver<ComponentBottleneck> {

  private static final String BACKPRESSURE_METRIC = "__time_spent_back_pressure_by_compid";
  private static final String EXECUTION_COUNT_METRIC = "__execute-count/default";
  private static final String BUFFER_GROWTH_RATE = "__buffer_growth_rate";
  private static final String LATEST_BUFFER_SIZE_BYTES = "__latest_buffer_size_bytes";
  private static final String LATEST_BUFFER_SIZE_PACKETS = "__latest_buffer_size_packets";
  private static final Logger LOG = Logger.getLogger(ScaleUpResolver.class.getName());


  private Config config;
  private Config runtime;
  private ISchedulerClient schedulerClient;
  /*private int newParallelism;
  private String newScaledUpComponent;*/
  private HashMap<String, Integer> scaleUpDemands = new HashMap<>();
  // Time (in seconds).
  private Double timeToDrainPendingBuffer = Double.MAX_VALUE;
  private double thresholdForAdditionalCapacity;

  @Override
  public void initialize(Config inputConfig, Config inputRuntime) {
    this.config = inputConfig;
    this.runtime = inputRuntime;
    try {
      String valueString = inputConfig.getStringValue("health.policy.addcapacity.threshold");
      this.thresholdForAdditionalCapacity = Double.parseDouble(valueString);
    } catch (Exception e) {
      this.thresholdForAdditionalCapacity = 0.02; // Default value.
    }
    schedulerClient = (ISchedulerClient) Runtime.schedulerClientInstance(runtime);
  }

  @Override
  public Boolean resolve(Diagnosis<ComponentBottleneck> diagnosis, TopologyAPI.Topology topology) {
    String topologyName = topology.getName();

    SchedulerStateManagerAdaptor manager = Runtime.schedulerStateManagerAdaptor(runtime);
    PackingPlans.PackingPlan currentPlan = manager.getPackingPlan(topologyName);

    PackingPlans.PackingPlan proposedPlan = buildNewPackingPlan(currentPlan, scaleUpDemands,
        topology);
    if (proposedPlan == null) {
      return false;
    }

    Scheduler.UpdateTopologyRequest updateTopologyRequest =
        Scheduler.UpdateTopologyRequest.newBuilder()
            .setCurrentPackingPlan(currentPlan)
            .setProposedPackingPlan(proposedPlan)
            .build();

    if (!schedulerClient.updateTopology(updateTopologyRequest)) {
      LOG.log(Level.SEVERE, "Failed to update topology with Scheduler, updateTopologyRequest="
          + updateTopologyRequest);
      return false;
    }

    try {
      TimeUnit.MINUTES.sleep(1);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // Clean the connection when we are done.
    LOG.fine("Scheduler updated topology successfully.");
    return true;

  }

  @Override
  public double estimateOutcome(Diagnosis<ComponentBottleneck> diagnosis,
                                TopologyAPI.Topology topology) {
    if (diagnosis.getSummary() == null) {
      throw new RuntimeException("Not valid diagnosis object");
    }
    for (ComponentBottleneck cb : diagnosis.getSummary()) {
      scaleUpDemands.put(cb.getComponentName(), computeScaleUpFactor(cb));
    }

    // NOTE - The output value is immaterial when action log is disabled.
    return 1;
  }

  private int computeScaleUpFactor(ComponentBottleneck current) {
    double totalBackpressureTime = 0;
    for (InstanceBottleneck instanceData : current.getInstances()) {
      if (instanceData.hasMetric(BACKPRESSURE_METRIC)) {
        int backpressureTime = Integer.valueOf(instanceData.getDataPoint(BACKPRESSURE_METRIC));
        LOG.log(Level.INFO, "Instance: {0}, back-pressure: {1}",
                new Object[]{instanceData.getInstanceData().getInstanceNameId(), backpressureTime});
        totalBackpressureTime += backpressureTime;
      }
    }

    if (totalBackpressureTime > 1000) {
      totalBackpressureTime = 999;
      LOG.log(Level.WARNING, "Invalid total back-pressure-time/sec: " + totalBackpressureTime);
    } else if (totalBackpressureTime < 20) {
      totalBackpressureTime = 0;
      LOG.log(Level.WARNING, "Ignore noisy back-pressure-time/sec: " + totalBackpressureTime);
    }

    LOG.info("Total back-pressure: " + totalBackpressureTime);

    if (Double.compare(totalBackpressureTime, 0.0) > 0) {
      double unusedCapacity = (1.0 * totalBackpressureTime) / (1000 - totalBackpressureTime);
      // scale up fencing: do not scale more than 4 times the current size
      unusedCapacity = unusedCapacity > 4.0 ? 4.0 : unusedCapacity;
      LOG.info("Unused capacity: " + unusedCapacity);
      return (int) Math.ceil(current.getInstances().size() * (1 + unusedCapacity));
    } else {
      // There is no backpressure in the system. Check if pending buffers are growing.
      Double totalGrowthRatePerSecond = 0.0;
      Double totalExecutionCount = 0.0;
      Double totalPendingBufferSize = 0.0;
      for (InstanceBottleneck instanceData : current.getInstances()) {
        if (instanceData.hasMetric(BUFFER_GROWTH_RATE)) {
          if (!Double.valueOf(instanceData.getInstanceData().getMetricValue(BUFFER_GROWTH_RATE)).isNaN()) {
            totalGrowthRatePerSecond += Double.valueOf(instanceData.getInstanceData().getMetricValue(BUFFER_GROWTH_RATE));
          }
        }
        if (instanceData.hasMetric(EXECUTION_COUNT_METRIC)) {
          if (!Double.valueOf(instanceData.getInstanceData().getMetricValue(EXECUTION_COUNT_METRIC)).isNaN()) {
            totalExecutionCount += Double.valueOf(instanceData.getInstanceData().getMetricValue(EXECUTION_COUNT_METRIC));
          }
        }
        if (instanceData.hasMetric(LATEST_BUFFER_SIZE_PACKETS)) {
          if (!Double.valueOf(instanceData.getInstanceData().getMetricValue(LATEST_BUFFER_SIZE_PACKETS)).isNaN()) {
            totalPendingBufferSize += Double.valueOf(instanceData.getInstanceData().getMetricValue(LATEST_BUFFER_SIZE_PACKETS));
          }
        }
      }

      final double totalExecutionRate = totalExecutionCount;

      assert !totalGrowthRatePerSecond.isNaN();
      assert !totalExecutionCount.isNaN();
      assert !totalPendingBufferSize.isNaN();

      DecimalFormat format = new DecimalFormat("#.###");
      LOG.info("GROWTH: " + format.format(totalGrowthRatePerSecond) + " packets/sec" +
      " EXECUTION: " + format.format(totalExecutionCount)+ " packets/sec" +
      " PENDING: " + format.format(totalPendingBufferSize) + " packets");

      // TODO(harshad) - In the for loop above, get the total buffer size.
      if (Double.compare(totalGrowthRatePerSecond, 0.0) > 0) {
        final Double additionalCapacity = totalGrowthRatePerSecond + totalPendingBufferSize/timeToDrainPendingBuffer;
        boolean needToScaleUp = false;
        if (Double.compare(totalExecutionRate, 0.0) > 0) {
          needToScaleUp = Double.compare(additionalCapacity / totalExecutionRate, thresholdForAdditionalCapacity) > 0;
        }
        LOG.info("Requested additional capacity factor: " + format.format(additionalCapacity));
        if (needToScaleUp) {
          // scale up fencing: do not scale more than 4 times the current size
          LOG.info("Scale up multiplier = " + (1 + additionalCapacity/totalExecutionRate));
          final double scaleUpMultiplier = Math
                  .min(((additionalCapacity/totalExecutionRate) + 1), 4.0);
          LOG.info("Scale up multiplier (capped): " + format.format(scaleUpMultiplier));
          final int returnValue
                  = Math.min((int) Math.ceil(scaleUpMultiplier) * current.getInstances().size(),
                  (int) Math.ceil(scaleUpMultiplier * current.getInstances().size()));
          LOG.info("Returning " + returnValue + " to scale up");
          return returnValue;
        } else {
          // Retain the current number of intsances.
          String printValue = format.format((Double.compare(totalExecutionRate, 0.0) > 0)
                  ? additionalCapacity / totalExecutionRate
                  : 0.0);
          LOG.info("No need to scale up - observed value: " + printValue
                    + " threshold for adding capacity: " + thresholdForAdditionalCapacity);
          return current.getInstances().size();
        }
      } else {
        LOG.info("Not scaling up - total growth rate = " + format.format(totalGrowthRatePerSecond));
        // No need to scale up.
        return current.getInstances().size();
      }
    }
  }

  @Override
  public boolean successfulAction(Diagnosis<ComponentBottleneck> oldDiagnosis,
                                  Diagnosis<ComponentBottleneck> newDiagnosis, double improvement) {

    Set<ComponentBottleneck> oldSummary = oldDiagnosis.getSummary();
    Set<ComponentBottleneck> newSummary = newDiagnosis.getSummary();
    ComponentBottleneck oldComponent = oldSummary.iterator().next();
    ComponentBottleneck newComponent = newSummary.iterator().next();
    System.out.println("old " + oldComponent.toString());
    System.out.println("new " + newComponent.toString());
    if (!oldComponent.getComponentName().equals(newComponent.getComponentName())) {
      return true;
    }
    if (SLAManagerUtils.reducedBackPressure(oldComponent, newComponent)) {
      LOG.info("reduced backpressure");
      return true;
    }
    if (SLAManagerUtils.improvedMetricSum(oldComponent, newComponent,
        EXECUTION_COUNT_METRIC, improvement)) {
      LOG.info("third");
      return true;
    }

    return false;
  }

  @Override
  public void close() {
  }

  PackingPlans.PackingPlan buildNewPackingPlan(PackingPlans.PackingPlan currentProtoPlan,
                                               Map<String, Integer> changeRequests,
                                               TopologyAPI.Topology topology) {
    PackingPlanProtoDeserializer deserializer = new PackingPlanProtoDeserializer();
    PackingPlanProtoSerializer serializer = new PackingPlanProtoSerializer();
    PackingPlan currentPackingPlan = deserializer.fromProto(currentProtoPlan);

    List<String> candidateComponents = new ArrayList<>();

    Map<String, Integer> componentCounts = currentPackingPlan.getComponentCounts();
    for (String currComponent : changeRequests.keySet()) {
      int currentCount = componentCounts.get(currComponent);
      if (currentCount == changeRequests.get(currComponent)) {
        LOG.info("Desired parallelism of " + currComponent + " is same as current: " + changeRequests.get(currComponent));
      } else {
        candidateComponents.add(currComponent);
      }
    }

    // Use the list below to store the component to be changed.
    List<String> changedComponent = new ArrayList<>();
    if (candidateComponents.isEmpty()) {
      LOG.info("No component requires change - not building new packing plan");
      return null;
    } else {
      // Find the component with the highest demand.
      int maxDemand = Integer.MIN_VALUE;
      for (String currComponent : candidateComponents) {
        if (changeRequests.get(currComponent) > maxDemand) {
          maxDemand = changeRequests.get(currComponent);
          changedComponent.clear();
          changedComponent.add(currComponent);
        }
      }
    }

    assert changedComponent.size() == 1;

    Map<String, Integer> componentChanges =
            parallelismDeltaSingleComponent(componentCounts, changeRequests, changedComponent.iterator().next());

    // Create an instance of the packing class
    String repackingClass = Context.repackingClass(config);
    IRepacking packing;
    try {
      // create an instance of the packing class
      packing = ReflectionUtils.newInstance(repackingClass);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      throw new IllegalArgumentException(
          "Failed to instantiate packing instance: " + repackingClass, e);
    }
    try {
      packing.initialize(config, topology);
      PackingPlan packedPlan = packing.repack(currentPackingPlan, componentChanges);
      return serializer.toProto(packedPlan);
    } finally {
      SysUtils.closeIgnoringExceptions(packing);
    }
  }

  Map<String, Integer> parallelismDelta(Map<String, Integer> componentCounts,
                                        Map<String, Integer> changeRequests) {
    Map<String, Integer> componentDeltas = new HashMap<>();
    for (String component : changeRequests.keySet()) {
      if (!componentCounts.containsKey(component)) {
        throw new IllegalArgumentException(
            "Invalid component name in update request: " + component);
      }
      Integer newValue = changeRequests.get(component);
      Integer delta = newValue - componentCounts.get(component);
      if (delta != 0) {
        componentDeltas.put(component, delta);
      }
    }
    return componentDeltas;
  }

  Map<String, Integer> parallelismDeltaSingleComponent(Map<String, Integer> componentCounts,
                                                       Map<String, Integer> changeRequests,
                                                       String componentName) {
    Map<String, Integer> componentDeltas = new HashMap<>();
    if (!componentCounts.containsKey(componentName)) {
      throw new IllegalArgumentException(
              "Invalid component name in update request: " + componentName);
    }
    Integer newValue = changeRequests.get(componentName);
    Integer delta = newValue - componentCounts.get(componentName);
    if (delta != 0) {
      componentDeltas.put(componentName, delta);
    }
    return componentDeltas;
  }
}

