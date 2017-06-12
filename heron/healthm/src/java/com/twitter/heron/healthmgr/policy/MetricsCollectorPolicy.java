package com.twitter.heron.healthmgr.policy;

import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.InstanceMetrics;
import com.microsoft.dhalion.policy.HealthPolicyImpl;
import com.twitter.heron.healthmgr.common.HealthManagerEvents;
import com.twitter.heron.healthmgr.detectors.BufferGrowthDetector;
import com.twitter.heron.healthmgr.sensors.BackPressureSensor;
import com.twitter.heron.healthmgr.sensors.BufferSizeSensor;
import com.twitter.heron.healthmgr.sensors.ExecuteCountSensor;
import org.apache.reef.wake.EventHandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class MetricsCollectorPolicy extends HealthPolicyImpl
        implements EventHandler<HealthManagerEvents.TopologyUpdate> {
  private static final Logger LOG
          = Logger.getLogger(DynamicResourceAllocationPolicy.class.getName());

  private ExecuteCountSensor executeCountSensor;
  private BackPressureSensor backPressureSensor;
  private BufferSizeSensor bufferSizeSensor;
  private BufferGrowthDetector bufferGrowthDetector;

  class MetricsCollector {
    // Key = component name
    private Map<String, List<InstanceMetrics>> values = new HashMap<>();

    MetricsCollector(){
    }

    public void addMetrics(String componentName, String metric, InstanceMetrics instanceMetrics) {
      if (!values.containsKey(metric)) {
        values.put(metric, new ArrayList<InstanceMetrics>());
      }
      values.get(componentName).add(instanceMetrics);
    }

    @Override
    public String toString() {
      StringBuilder stringBuilder = new StringBuilder();
      for (String componentName : values.keySet()) {
        for (InstanceMetrics instanceMetrics : values.get(componentName)) {
          stringBuilder.append("Component: " + componentName + " Metric: " + componentName +
                  " Value: " + instanceMetrics.getMetricValue(componentName));
          stringBuilder.append("\n");
        }
      }
      return String.valueOf(stringBuilder);
    }
  }

  public MetricsCollectorPolicy(ExecuteCountSensor executeCountSensor,
                                BackPressureSensor backPressureSensor,
                                BufferSizeSensor bufferSizeSensor,
                                BufferGrowthDetector bufferGrowthDetector) {
    this.executeCountSensor = executeCountSensor;
    this.backPressureSensor = backPressureSensor;
    this.bufferSizeSensor = bufferSizeSensor;
    this.bufferGrowthDetector = bufferGrowthDetector;
  }

  @Override
  public void onNext(HealthManagerEvents.TopologyUpdate value) {
  }

  @Override
  public List<Symptom> executeDetectors() {
    final Map<String, ComponentMetrics> execCounts = executeCountSensor.get();
    final Map<String, ComponentMetrics> backPressures = backPressureSensor.get();
    final Map<String, ComponentMetrics> bufferSizes = bufferSizeSensor.get();
    final List<Symptom> growthRateSymptoms = bufferGrowthDetector.detect();
    MetricsCollector metricsCollector = new MetricsCollector();

    addMetricsHelper(execCounts, metricsCollector);
    addMetricsHelper(backPressures, metricsCollector);
    addMetricsHelper(bufferSizes, metricsCollector);

    for (Symptom growthRateSymptom : growthRateSymptoms) {
      final Map<String, ComponentMetrics> components = growthRateSymptom.getComponents();
      addMetricsHelper(components, metricsCollector);
    }
    LOG.info(metricsCollector.toString());
    return null;
  }

  private void addMetricsHelper(final Map<String, ComponentMetrics> metrics,
                                MetricsCollector metricsCollector) {
    for (String componentName : metrics.keySet()) {
      // Key = metric name.
      final HashMap<String, InstanceMetrics> instanceMetricsHashMap
              = metrics.get(componentName).getMetrics();
      for (String metricName : metrics.keySet()) {
        metricsCollector.addMetrics(componentName,
                metricName, instanceMetricsHashMap.get(metricName));
      }
    }
  }
}
