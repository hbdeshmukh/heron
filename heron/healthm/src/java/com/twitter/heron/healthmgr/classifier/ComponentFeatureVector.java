package com.twitter.heron.healthmgr.classifier;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ComponentFeatureVector {
  // Key = metric name (e.g. "__execute-count/default")
  private Map<String, List<Double>> featureVector = new HashMap<>();
  private boolean label;
  private String componentName;

  public void setLabel(boolean label) {
    this.label = label;
  }
  public boolean getLabel() {
    return label;
  }

  public String getComponentName() {
    return componentName;
  }

  public ComponentFeatureVector(String componentName) {
    this.componentName = componentName;
  }

  public void addFeature(String featureName, List<Double> featureValuesForInstances) {
    if (!featureVector.containsKey(featureName)) {
      featureVector.put(featureName, featureValuesForInstances);
    } else {
      List<Double> doubles = featureVector.get(featureName);
      doubles.addAll(featureValuesForInstances);
      featureVector.put(featureName, doubles);
    }
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    for (String featureName : featureVector.keySet()) {
      for (Double featureValue : featureVector.get(featureName)) {
        stringBuilder.append(featureValue);
        stringBuilder.append(",");
      }
    }
    stringBuilder.append(label ? "1" : "0");
    return stringBuilder.toString();
  }
}
