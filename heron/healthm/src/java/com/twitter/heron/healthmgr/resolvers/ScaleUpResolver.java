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
package com.twitter.heron.healthmgr.resolvers;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.microsoft.dhalion.api.IResolver;
import com.microsoft.dhalion.core.EventManager;
import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.diagnoser.Diagnosis;
import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.InstanceMetrics;
import com.microsoft.dhalion.resolver.Action;
import com.twitter.heron.api.generated.TopologyAPI.Topology;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.healthmgr.common.HealthManagerEvents.TopologyUpdate;
import com.twitter.heron.healthmgr.common.HealthMgrConstants;
import com.twitter.heron.healthmgr.common.PackingPlanProvider;
import com.twitter.heron.healthmgr.common.TopologyProvider;
import com.twitter.heron.healthmgr.sensors.BufferSizeSensor;
import com.twitter.heron.healthmgr.sensors.ExecuteCountSensor;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.IRepacking;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlanProtoSerializer;
import com.twitter.heron.spi.utils.ReflectionUtils;

import javax.inject.Inject;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class ScaleUpResolver implements IResolver {
  private static final String BACK_PRESSURE = HealthMgrConstants.METRIC_INSTANCE_BACK_PRESSURE;
  private static final String BUFFER_GROWTH_RATE = HealthMgrConstants.BUFFER_GROWTH_RATE;
  private static final String BUFFER_SIZE = HealthMgrConstants.METRIC_BUFFER_SIZE;
  protected static final String EXEC_COUNT = HealthMgrConstants.METRIC_EXE_COUNT;
  protected static final String GROWING_BUFFER = HealthMgrConstants.SYMPTOM_GROWING_BUFFER;
  private static final Logger LOG = Logger.getLogger(ScaleUpResolver.class.getName());

  private TopologyProvider topologyProvider;
  private PackingPlanProvider packingPlanProvider;
  private ISchedulerClient schedulerClient;
  private EventManager eventManager;
  private BufferSizeSensor bufferSizeSensor;
  private ExecuteCountSensor executeCountSensor;

  private Config config;
  // TODO(harshad) - Read this parameter from the config.
  private double thresholdForPendingBuffer = 100.0;
  // TODO(harshad) - Read this parameter from the config.
  private double thresholdForGrowthRate = 0.0;
  private Double timeToDrainPendingBuffer = Double.MAX_VALUE;

  @Inject
  public ScaleUpResolver(TopologyProvider topologyProvider,
                         PackingPlanProvider packingPlanProvider,
                         ISchedulerClient schedulerClient,
                         EventManager eventManager,
                         BufferSizeSensor bufferSizeSensor,
                         ExecuteCountSensor executeCountSensor,
                         Config config) {
    this.topologyProvider = topologyProvider;
    this.packingPlanProvider = packingPlanProvider;
    this.schedulerClient = schedulerClient;
    this.eventManager = eventManager;
    this.bufferSizeSensor = bufferSizeSensor;
    this.executeCountSensor = executeCountSensor;
    this.config = config;
  }

  private class ScaleUpComputerForBufferGrowth {
    protected String componentName;
    protected Symptom symptom;

    public ScaleUpComputerForBufferGrowth(Symptom symptom) {
      LOG.info("Constructor of ScaleUpComputerForBufferGrowth - symptom" + symptom);
      this.symptom = symptom;
    }

    int computeScaleUpFactor() {
      // We target that component that has the highest buffer growth rate.
      // Key = component name.
      Map<String, ComponentMetrics> components = symptom.getComponents();
      Map<String, Double> componentGrowthRate = new HashMap<>();
      String componentWithHighestGrowth = components.keySet().iterator().next();
      Double highestGrowthRate = Double.MIN_VALUE;
      for (String currComponentName : components.keySet()) {
        final Double currComponentGrowthRate = getComponentGrowthRate(components.get(currComponentName));
        componentGrowthRate.put(currComponentName, currComponentGrowthRate);
        if (Double.compare(currComponentGrowthRate, highestGrowthRate) > 0) {
          highestGrowthRate = currComponentGrowthRate;
          componentWithHighestGrowth = currComponentName;
        }
      }
      // Ensure that the highest growth rate is positive.
      if (Double.compare(highestGrowthRate, 0.0) > 0) {
        // Get the rate of execution for this component.
        this.componentName = componentWithHighestGrowth;
        final Double execRateOfComponent = getExecutionRate(componentWithHighestGrowth);
        final Double pendingBufferSize = getPendingBufferSize(componentWithHighestGrowth);
        final int numInstances
                = packingPlanProvider.get().getComponentCounts().get(componentWithHighestGrowth);
        final boolean scaleUpNeeded = checkIfScaleUpIsNeeded(highestGrowthRate,
                execRateOfComponent,
                pendingBufferSize,
                numInstances);
        DecimalFormat format = new DecimalFormat("#.###");
        if (scaleUpNeeded) {
          final Double additionalCapacity = highestGrowthRate + pendingBufferSize/timeToDrainPendingBuffer;
          LOG.info("GROWTH: " + format.format(highestGrowthRate) + " packets/sec" +
                  " EXECUTION: " + format.format(execRateOfComponent)+ " packets/sec" +
                  " PENDING: " + format.format(pendingBufferSize) + " packets");
          // scale up fencing: do not scale more than 4 times the current size
          final double scaleUpMultiplier = Math
                  .min(((additionalCapacity/execRateOfComponent) + 1), 4.0);
          LOG.info("Scale up multiplier (capped): " + format.format(scaleUpMultiplier));
          final int returnValue
                  = Math.min((int) Math.ceil(scaleUpMultiplier) * numInstances,
                  (int) Math.ceil(scaleUpMultiplier * numInstances));
          LOG.info("Request to increase " + componentWithHighestGrowth + " instances to " + returnValue);
          return returnValue;
        } else {
          // Retain the current number of instances.
          LOG.info("No need to scale up - GROWTH: " + format.format(highestGrowthRate)
                  + " EXECUTION: " + format.format(execRateOfComponent));
          return numInstances;
        }
      } else {
        // All the components have a growth rate that is lower than the threshold.
        return packingPlanProvider.get().getComponentCounts().get(componentWithHighestGrowth);
      }
    }

    String getComponentName() {
      return componentName;
    }
  }

  private class ScaleUpFactorForBackPressure extends ScaleUpComputerForBufferGrowth {
    public ScaleUpFactorForBackPressure(Symptom symptom) {
      super(symptom);
    }

    @Override
    int computeScaleUpFactor() {
      if (this.symptom != null) {
        LOG.info(symptom.toString());
      } else {
        LOG.info("ScaleUpResolver::computeScaleUpFactor symptom is null");
      }
      if (this.symptom.getComponents().size() > 1) {
        throw new UnsupportedOperationException("Multiple components with back pressure symptom");
      }
      ComponentMetrics componentMetrics = this.symptom.getComponent();
      this.componentName = componentMetrics.getName();
      double totalCompBpTime = 0;
      String compName = componentMetrics.getName();
      for (InstanceMetrics instanceMetrics : componentMetrics.getMetrics().values()) {
        double instanceBp = instanceMetrics.getMetricValue(BACK_PRESSURE);
        LOG.info(String.format("Instance:%s, bpTime:%.0f", instanceMetrics.getName(), instanceBp));
        totalCompBpTime += instanceBp;
      }

      LOG.info(String.format("Component: %s, bpTime: %.0f", compName, totalCompBpTime));
      if (totalCompBpTime >= 1000) {
        totalCompBpTime = 999;
      }
      LOG.warning(String.format("Comp:%s, bpTime after filter: %.0f", compName, totalCompBpTime));

      double unusedCapacity = (1.0 * totalCompBpTime) / (1000 - totalCompBpTime);
      // scale up fencing: do not scale more than 4 times the current size
      unusedCapacity = unusedCapacity > 4.0 ? 4.0 : unusedCapacity;
      int parallelism = (int) Math.ceil(componentMetrics.getMetrics().size() * (1 + unusedCapacity));
      LOG.info(String.format("Component's, %s, unused capacity is: %.3f. New parallelism: %d",
              compName, unusedCapacity, parallelism));
      return parallelism;
    }
  }

  @Override
  public List<Action> resolve(List<Diagnosis> diagnosis) {
    for (Diagnosis diagnoses : diagnosis) {
      ScaleUpComputerForBufferGrowth scaleFactorComputer;
      Symptom bpSymptom = diagnoses.getSymptoms().get(BACK_PRESSURE);
      Symptom bufferGrowthSymptom = diagnoses.getSymptoms().get(GROWING_BUFFER);
      LOG.info("bufferGrowthSymptom is " + bufferGrowthSymptom);
      scaleFactorComputer = bpSymptom == null
              ? new ScaleUpComputerForBufferGrowth(bufferGrowthSymptom)
              : new ScaleUpFactorForBackPressure(bpSymptom);

      final int newParallelism = scaleFactorComputer.computeScaleUpFactor();
      Map<String, Integer> changeRequest = new HashMap<>();
      changeRequest.put(scaleFactorComputer.getComponentName(), newParallelism);

      return getResolveActions(changeRequest);
    }
    return null;
  }

  private List<Action> getResolveActions(Map<String, Integer> changeRequest) {
    PackingPlan currentPackingPlan = packingPlanProvider.get();
    PackingPlan newPlan = buildNewPackingPlan(changeRequest, currentPackingPlan);
    if (newPlan == null) {
      return null;
    }

    Scheduler.UpdateTopologyRequest updateTopologyRequest =
        Scheduler.UpdateTopologyRequest.newBuilder()
            .setCurrentPackingPlan(getSerializedPlan(currentPackingPlan))
            .setProposedPackingPlan(getSerializedPlan(newPlan))
            .build();

    LOG.info("Sending Updating topology request: " + updateTopologyRequest);
    if (!schedulerClient.updateTopology(updateTopologyRequest)) {
      throw new RuntimeException(String.format("Failed to update topology with Scheduler, " +
          "updateTopologyRequest=%s", updateTopologyRequest));
    }

    TopologyUpdate action = new TopologyUpdate();
    LOG.info("Broadcasting topology update event");
    eventManager.onEvent(action);

    LOG.info("Scheduler updated topology successfully.");

    List<Action> actions = new ArrayList<>();
    actions.add(action);
    return actions;
  }

  private boolean checkIfScaleUpIsNeeded(Double totalGrowthRatePerSecond,
                                         Double totalExecutionRate,
                                         Double totalPendingBufferSize,
                                         int numInstances) {
    // NOTE - The check on totalExecutionRate is needed because it is used
    // as a denominator in the scale up factor computation.
    return Double.compare(totalPendingBufferSize, numInstances * thresholdForPendingBuffer) > 0
            && Double.compare(totalExecutionRate, 0.0) > 0
            && Double.compare(totalGrowthRatePerSecond, thresholdForGrowthRate) > 0;
  }

  private Double getComponentGrowthRate(ComponentMetrics componentMetrics) {
    HashMap<String, InstanceMetrics> metrics = componentMetrics.getMetrics();
    Double growthRate = 0.0;

    for (InstanceMetrics instanceMetrics : metrics.values()) {
      assert !instanceMetrics.getMetricValue(BUFFER_GROWTH_RATE).isNaN();
      growthRate += instanceMetrics.getMetricValue(BUFFER_GROWTH_RATE);
    }
    return growthRate;
  }

  private Double getExecutionRate(String componentName) {
    final Map<String, ComponentMetrics> stringComponentMetricsMap
            = executeCountSensor.get(componentName);
    Double result = 0.0;
    assert stringComponentMetricsMap.containsKey(componentName);
    final HashMap<String, InstanceMetrics> metrics
            = stringComponentMetricsMap.get(componentName).getMetrics();
    for (InstanceMetrics instanceMetrics : metrics.values()) {
      result += instanceMetrics.getMetricValue(EXEC_COUNT);
    }
    return result;
  }

  private Double getPendingBufferSize(String componentName) {
    final Map<String, ComponentMetrics> stringComponentMetricsMap
            = bufferSizeSensor.get(componentName);
    Double result = 0.0;
    assert stringComponentMetricsMap.containsKey(componentName);
    final HashMap<String, InstanceMetrics> metrics
            = stringComponentMetricsMap.get(componentName).getMetrics();
    LOG.info(metrics.toString());
    for (InstanceMetrics instanceMetrics : metrics.values()) {
      result += instanceMetrics.getMetricValue(BUFFER_SIZE);
    }
    return result;
  }

  @VisibleForTesting
  PackingPlan buildNewPackingPlan(Map<String, Integer> changeRequests,
                                  PackingPlan currentPackingPlan) {
    Map<String, Integer> componentDeltas = new HashMap<>();
    Map<String, Integer> componentCounts = currentPackingPlan.getComponentCounts();
    for (String compName : changeRequests.keySet()) {
      if (!componentCounts.containsKey(compName)) {
        throw new IllegalArgumentException(String.format(
            "Invalid component name in scale up diagnosis: %s. Valid components include: %s",
            compName, Arrays.toString(
                componentCounts.keySet().toArray(new String[componentCounts.keySet().size()]))));
      }

      Integer newValue = changeRequests.get(compName);
      int delta = newValue - componentCounts.get(compName);
      if (delta == 0) {
        LOG.info(String.format("New parallelism for %s is unchanged: %d", compName, newValue));
        continue;
      }

      componentDeltas.put(compName, delta);
    }

    // Create an instance of the packing class
    IRepacking packing = getRepackingClass(Context.repackingClass(config));

    Topology topology = topologyProvider.get();
    try {
      packing.initialize(config, topology);
      PackingPlan packedPlan = packing.repack(currentPackingPlan, componentDeltas);
      return packedPlan;
    } finally {
      SysUtils.closeIgnoringExceptions(packing);
    }
  }

  @VisibleForTesting
  IRepacking getRepackingClass(String repackingClass) {
    IRepacking packing;
    try {
      // create an instance of the packing class
      packing = ReflectionUtils.newInstance(repackingClass);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      throw new IllegalArgumentException(
          "Failed to instantiate packing instance: " + repackingClass, e);
    }
    return packing;
  }

  @VisibleForTesting
  PackingPlans.PackingPlan getSerializedPlan(PackingPlan packedPlan) {
    PackingPlanProtoSerializer serializer = new PackingPlanProtoSerializer();
    return serializer.toProto(packedPlan);
  }

  @Override
  public void close() {
  }
}
