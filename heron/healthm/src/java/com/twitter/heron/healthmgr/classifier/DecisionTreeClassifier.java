package com.twitter.heron.healthmgr.classifier;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import net.minidev.json.JSONArray;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class DecisionTreeClassifier {
  private static final Logger LOG
          = Logger.getLogger(DecisionTreeClassifier.class.getName());

  private List<String> featureNames = new ArrayList<>();
  private String LABEL_JSON_KEY = "label";
  private String LABEL_JSON_KEY_SUFFIX = "-label";

  DecisionTreeClassifier() {
    featureNames.add("__execute-count/default");
  }

  public ComponentFeatureVector constructComponentVector(String jsonFileName, String componentName) {
    ComponentFeatureVector vector = new ComponentFeatureVector(componentName);
    DocumentContext documentContext = null;
    try {
      documentContext = JsonPath.parse(new File(jsonFileName));
    } catch (IOException e) {
      e.printStackTrace();
    }
    for (String featureName : featureNames) {
      JSONArray componentArray = documentContext.read("$.." + componentName + ".." + featureName);
      LOG.info(componentArray.toString());
      LOG.info("JsonArray has " + componentArray.size() + " elements for feature " + featureName);
      List<Double> featureValues = new ArrayList<>();
      for (int currEntry = 0; currEntry < componentArray.size(); currEntry++) {
        Map<String, Double> componentMap = (Map<String, Double>) componentArray.get(currEntry);
        for (Double aDouble : componentMap.values()) {
          featureValues.add(aDouble);
        }
      }
      vector.addFeature(featureName, featureValues);
    }
    return vector;
  }

  public void setComponentLabel(String jsonFileName, String componentName, ComponentFeatureVector vector) {
    DocumentContext documentContext = null;
    try {
      documentContext = JsonPath.parse(new File(jsonFileName));
    } catch (IOException e) {
      e.printStackTrace();
    }
    JSONArray jsonArray = documentContext.read("$.." + componentName + ".." + componentName + LABEL_JSON_KEY_SUFFIX + ".." +LABEL_JSON_KEY);
    LOG.info(jsonArray.toString());
    Map<String, Double> componentMap = (Map<String, Double>) jsonArray.get(0);
    final Collection<Double> values = componentMap.values();
    vector.setLabel(Double.compare(1.0, values.iterator().next()) == 0);
  }

  public static void main(String[] args) {
    DecisionTreeClassifier decisionTreeClassifier = new DecisionTreeClassifier();
    final String directoryStr = "/tmp/ml-dhalion/";
    File directory = new File(directoryStr);
    File[] listOfFiles = directory.listFiles();
    List<String> fileNames = new ArrayList<>();
    for (int fileId = 0; fileId < listOfFiles.length; fileId++) {
      fileNames.add(directoryStr + listOfFiles[fileId].getName());
    }
    LOG.info(fileNames.toString());
    StringBuilder trainingData = new StringBuilder("@DATA\n");
    for (String fileName : fileNames) {
      final ComponentFeatureVector vector = decisionTreeClassifier.constructComponentVector(fileName, "count");
      decisionTreeClassifier.setComponentLabel(fileName, "count", vector);
      trainingData.append(vector.toString());
      trainingData.append("\n");
    }
    LOG.info(trainingData.toString());
  }
}
