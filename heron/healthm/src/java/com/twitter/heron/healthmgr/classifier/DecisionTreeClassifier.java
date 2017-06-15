package com.twitter.heron.healthmgr.classifier;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import net.minidev.json.JSONArray;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
import java.util.logging.Logger;

public class DecisionTreeClassifier {
  private static final Logger LOG
          = Logger.getLogger(DecisionTreeClassifier.class.getName());

  private List<String> featureNames = new ArrayList<>();
  private String LABEL_JSON_KEY = "label";
  private String LABEL_JSON_KEY_SUFFIX = "-label";

  DecisionTreeClassifier() {
    featureNames.add("__execute-count/default");
    featureNames.add("__time_spent_back_pressure_by_compid/");
    featureNames.add("__connection_buffer_by_instanceid/");
    featureNames.add("BUFFER_GROWTH_RATE");
  }

  @SuppressWarnings("unchecked")
  public ComponentFeatureVector constructComponentVector(String jsonFileName, String componentName) {
    ComponentFeatureVector vector = new ComponentFeatureVector(componentName);
    DocumentContext documentContext = null;
    try {
      documentContext = JsonPath.parse(new File(jsonFileName));
    } catch (IOException e) {
      e.printStackTrace();
    }
    int numInstances = 0;
    LOG.info(featureNames.toString());
    for (String featureName : featureNames) {
      JSONArray componentArray = documentContext.read("$.." + componentName + ".." + featureName);
      List<Double> featureValues = new ArrayList<>();
      for (int currEntry = 0; currEntry < componentArray.size(); currEntry++) {
        Map<String, Object> componentMap = (Map<String, Object>) componentArray.get(currEntry);
        for (String key : componentMap.keySet()) {
          double value;
          if (componentMap.get(key) instanceof BigDecimal) {
            BigDecimal tmpValue = (BigDecimal) componentMap.get(key);
            value = tmpValue.doubleValue();
          } else {
            value = (double) componentMap.get(key);
          }
          featureValues.add(value);
        }
      }
      // If we don't have enough features, add 0 as a dummy value.
      numInstances = Math.max(numInstances, featureValues.size());
      if (featureValues.size() < numInstances) {
        LOG.info("For feature " + featureName + " observed values: " + featureValues.size() + " # instances: " + numInstances);
        for (int i = featureValues.size(); i < numInstances; i++) {
          featureValues.add(new Double(0));
        }
      }
      vector.addFeature(featureName, featureValues);
    }
    return vector;
  }

  @SuppressWarnings("unchecked")
  public void setComponentLabel(String jsonFileName, String componentName, ComponentFeatureVector vector) {
    DocumentContext documentContext = null;
    try {
      documentContext = JsonPath.parse(new File(jsonFileName));
    } catch (IOException e) {
      e.printStackTrace();
    }
    JSONArray jsonArray = documentContext.read("$.." + componentName + ".." + componentName + LABEL_JSON_KEY_SUFFIX + ".." +LABEL_JSON_KEY);
    Map<String, Double> componentMap = (Map<String, Double>) jsonArray.get(0);
    final Collection<Double> values = componentMap.values();
    vector.setLabel(Double.compare(1.0, values.iterator().next()) == 0);
  }

  public static void main(String[] args) {
    DecisionTreeClassifier decisionTreeClassifier = new DecisionTreeClassifier();
    final String directoryStr = "/tmp/ml-dhalion/";
    File directory = new File(directoryStr);
    File[] listOfFilesAsArray = directory.listFiles();
    List<File> listOfFiles = Arrays.asList(listOfFilesAsArray);
    /*List<File> listOfFiles = new ArrayList<>();
    listOfFiles.add(new File("/tmp/ml-dhalion/ml-dhalion-topology1497461389102.json"));*/
    List<String> fileNames = new ArrayList<>();
    for (int fileId = 0; fileId < listOfFiles.size(); fileId++) {
      fileNames.add(directoryStr + listOfFiles.get(fileId).getName());
    }
    int runCount = 0;
    StringBuilder trainingData = new StringBuilder();
    for (String fileName : fileNames) {
      try {
        final ComponentFeatureVector vector = decisionTreeClassifier.constructComponentVector(fileName, "count");
        decisionTreeClassifier.setComponentLabel(fileName, "count", vector);
        if (runCount == 0) {
          constructHeader(trainingData, vector);
          runCount++;
        }
        trainingData.append(vector.toString());
        trainingData.append("\n");
      } catch (Exception e) {
        LOG.info("Filename: " + fileName);
        e.printStackTrace();
        return;
      }
    }
    try {
      FileWriter fileWriter = new FileWriter("/home/osboxes/Desktop/train.arff");
      BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
      bufferedWriter.write(trainingData.toString());
      bufferedWriter.close();
      fileWriter.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    //LOG.info(trainingData.toString());
  }

  private static void constructHeader(StringBuilder trainingData, ComponentFeatureVector vector) {
    Set<String> allFeatureNames = vector.getFeatureNames();
    final int numInstances = vector.getNumInstances();
    for (String allFeatureName : allFeatureNames) {
      for (int i = 0; i < numInstances; i++) {
        trainingData.append(allFeatureName + i);
        trainingData.append(",");
      }
    }
    trainingData.append("label\n");
  }
}
