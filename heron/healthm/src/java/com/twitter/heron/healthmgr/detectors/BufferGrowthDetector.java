package com.twitter.heron.healthmgr.detectors;

import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.InstanceMetrics;
import com.twitter.heron.healthmgr.common.CurveFitter;
import com.twitter.heron.healthmgr.sensors.BufferTrendLineSensor;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BufferGrowthDetector extends BaseDetector {
  private BufferTrendLineSensor bufferTrendLineSensor;
  private long numSecondsBetweenObservations = 60;
  private Double bufferGrowthRateThreshold = 0.0;

  @Inject
  public void initialize(BufferTrendLineSensor bufferTrendLineSensor) {
    this.bufferTrendLineSensor = bufferTrendLineSensor;
  }

  @Override
  public List<Symptom> detect() {
    Map<String, ComponentMetrics> bufferTrendLineMetrics = bufferTrendLineSensor.get();
    List<Symptom> symptoms = new ArrayList<>();
    for (String componentName : bufferTrendLineMetrics.keySet()) {
      HashMap<String, InstanceMetrics> metrics
              = bufferTrendLineMetrics.get(componentName).getMetrics();
      ComponentMetrics currComponentMetrics = new ComponentMetrics(componentName);
      for (String instanceName : metrics.keySet()) {
        List<Double> bufferSizes = new ArrayList<>();
        for (String metricAsStr : metrics.get(instanceName).getMetrics()) {
          bufferSizes.add(Double.parseDouble(metricAsStr));
        }
        currComponentMetrics.addInstanceMetric(new InstanceMetrics(
                        instanceName, BUFFER_GROWTH_RATE, getIncreaseRate(bufferSizes)));
      }
      if (currComponentMetrics.anyInstanceAboveLimit(
              BUFFER_GROWTH_RATE, bufferGrowthRateThreshold)) {
        symptoms.add(new Symptom(GROWING_BUFFER, currComponentMetrics));
      }
    }
    return symptoms;
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
