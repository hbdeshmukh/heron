package com.twitter.heron.healthmgr.detectors;

import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.InstanceMetrics;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.healthmgr.common.CurveFitter;
import com.twitter.heron.healthmgr.common.PackingPlanProvider;
import com.twitter.heron.healthmgr.common.TopologyProvider;
import com.twitter.heron.healthmgr.sensors.BufferTrendLineSensor;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class BufferGrowthDetector extends BaseDetector {
  private BufferTrendLineSensor bufferTrendLineSensor;
  private TopologyProvider topologyProvider;
  private PackingPlanProvider packingPlanProvider;
  private long numSecondsBetweenObservations = 60;
  private Double bufferGrowthRateThreshold = 0.0;

  @Inject
  public BufferGrowthDetector(BufferTrendLineSensor bufferTrendLineSensor, TopologyProvider topologyProvider, PackingPlanProvider packingPlanProvider) {
    this.bufferTrendLineSensor = bufferTrendLineSensor;
    this.topologyProvider = topologyProvider;
    this.packingPlanProvider = packingPlanProvider;
  }

  @Override
  public List<Symptom> detect() {
    final String[] boltNames = topologyProvider.getBoltNames();
    Map<String, ComponentMetrics> bufferTrendLineMetrics = bufferTrendLineSensor.get(boltNames);
    Symptom symptom = new Symptom(GROWING_BUFFER);
    for (String componentName : bufferTrendLineMetrics.keySet()) {
      HashMap<String, InstanceMetrics> metrics
              = bufferTrendLineMetrics.get(componentName).getMetrics();
      ComponentMetrics currComponentMetrics = new ComponentMetrics(componentName);
      for (String instanceName : metrics.keySet()) {
        List<Double> bufferSizes = new ArrayList<>();
        for (String metricAsStr : metrics.get(instanceName).getMetricNames()) {
          for (Double d : metrics.get(instanceName).getMetricValues(metricAsStr).values()) {
            bufferSizes.add(d);
          }
        }
        currComponentMetrics.addInstanceMetric(new InstanceMetrics(
                        instanceName, BUFFER_GROWTH_RATE, getIncreaseRate(bufferSizes)));
      }
      if (currComponentMetrics.anyInstanceAboveLimit(
              BUFFER_GROWTH_RATE, bufferGrowthRateThreshold)) {
        symptom.addComponentMetrics(currComponentMetrics);
      }
    }

    if (symptom.getComponents().size() > 0) {
      List<Symptom> symptoms = new ArrayList<>();
      symptoms.add(symptom);
      return symptoms;
    }

    return null;
  }

  private Double getIncreaseRate(List<Double> data) {
    CurveFitter curveFitter = new CurveFitter();
    List<Double> xPoints = new ArrayList<>();
    for (int i = 0; i < data.size(); i++) {
      if (!Double.valueOf(i * numSecondsBetweenObservations).isNaN()) {
        xPoints.add(new Double(i * numSecondsBetweenObservations));
      }
    }
    curveFitter.linearCurveFit(xPoints, data);
    return curveFitter.getSlope();
  }
}
