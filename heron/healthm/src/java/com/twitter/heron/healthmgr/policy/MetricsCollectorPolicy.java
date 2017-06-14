package com.twitter.heron.healthmgr.policy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.InstanceMetrics;
import com.microsoft.dhalion.policy.HealthPolicyImpl;
import com.twitter.heron.healthmgr.HealthPolicyConfig;
import com.twitter.heron.healthmgr.detectors.BufferGrowthDetector;
import com.twitter.heron.healthmgr.sensors.BackPressureSensor;
import com.twitter.heron.healthmgr.sensors.BufferSizeSensor;
import com.twitter.heron.healthmgr.sensors.ExecuteCountSensor;

import javax.inject.Inject;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class MetricsCollectorPolicy extends HealthPolicyImpl {
  private static final Logger LOG
          = Logger.getLogger(MetricsCollectorPolicy.class.getName());

  private HealthPolicyConfig healthConfig;
  private ExecuteCountSensor executeCountSensor;
  private BackPressureSensor backPressureSensor;
  private BufferSizeSensor bufferSizeSensor;
  private BufferGrowthDetector bufferGrowthDetector;

  @Inject
  MetricsCollectorPolicy(HealthPolicyConfig healthConfig,
                         ExecuteCountSensor executeCountSensor,
                         BackPressureSensor backPressureSensor,
                         BufferSizeSensor bufferSizeSensor,
                         BufferGrowthDetector bufferGrowthDetector) {
    this.healthConfig = healthConfig;
    this.executeCountSensor = executeCountSensor;
    this.backPressureSensor = backPressureSensor;
    this.bufferSizeSensor = bufferSizeSensor;
    this.bufferGrowthDetector = bufferGrowthDetector;
  }

  @Override
  public List<Symptom> executeDetectors() {
    final Map<String, ComponentMetrics> execCounts = executeCountSensor.get();
    final Map<String, ComponentMetrics> backPressures = backPressureSensor.get();
    final Map<String, ComponentMetrics> bufferSizes = bufferSizeSensor.get();
    final List<Symptom> growthRateSymptoms = bufferGrowthDetector.detect();
    Map<String, ComponentMetrics> mergedMetrics = new HashMap<>();

    mergeMetricsHelper(mergedMetrics, execCounts);
    mergeMetricsHelper(mergedMetrics, backPressures);
    mergeMetricsHelper(mergedMetrics, bufferSizes);

    if (growthRateSymptoms != null) {
      for (Symptom growthRateSymptom : growthRateSymptoms) {
        if (growthRateSymptom != null) {
          final Map<String, ComponentMetrics> components = growthRateSymptom.getComponents();
          mergeMetricsHelper(components, mergedMetrics);
        }
      }
    }
    addLables(mergedMetrics);
    serializeToJson(mergedMetrics);
    return null;
  }

  private void mergeMetricsHelper(Map<String, ComponentMetrics> destinationMetrics,
                                  final Map<String, ComponentMetrics> sourceMetrics) {
    for (String srcComponentName : sourceMetrics.keySet()) {
      if (destinationMetrics.containsKey(srcComponentName)) {
        ComponentMetrics.merge(destinationMetrics.get(srcComponentName), sourceMetrics.get(srcComponentName));
      } else {
        // destinationMetrics doesn't have an entry for component with name srcComponentName.
        destinationMetrics.put(srcComponentName, sourceMetrics.get(srcComponentName));
      }
    }
  }

  private void serializeToJson(final Map<String, ComponentMetrics> metricsMap) {
    ObjectMapper objectMapper = new ObjectMapper();
    Long l = System.currentTimeMillis();
    try {
      objectMapper.writerWithDefaultPrettyPrinter()
              .writeValue(new File("/tmp/ml-dhalion/ml-dhalion-topology" + l.toString() + ".json"), metricsMap);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void addLables(Map<String, ComponentMetrics> metricsMap) {
    // Search for the component names in the config file.
    // 0 means a negative label, 1 means a positive label.
    // The default value is 0.
    for (String componentName : metricsMap.keySet()) {
      Integer labelAsInt = Integer.parseInt(healthConfig.getConfig(componentName, "0"));
      // We add a dummy instance whose name is componentName + "-label".
      InstanceMetrics instanceMetrics = new InstanceMetrics(componentName + "-label");
      instanceMetrics.addMetric("label", labelAsInt);
      metricsMap.get(componentName).addInstanceMetric(instanceMetrics);
    }
  }
}
