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


package com.twitter.heron.healthmgr.sensors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.microsoft.dhalion.api.MetricsProvider;
import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.InstanceMetrics;
import com.twitter.heron.healthmgr.common.HealthMgrConstants;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

public class TrackerMetricsProvider implements MetricsProvider {
  private static final Logger LOG = Logger.getLogger(TrackerMetricsProvider.class.getName());
  private final WebTarget baseTarget;
  private final WebTarget baseTargetTimeline;

  @Inject
  public TrackerMetricsProvider(@Named(HealthMgrConstants.CONF_TRACKER_URL) String trackerURL,
                                @Named(HealthMgrConstants.CONF_TOPOLOGY_NAME) String topologyName,
                                @Named(HealthMgrConstants.CONF_CLUSTER) String cluster,
                                @Named(HealthMgrConstants.CONF_ENVIRON) String environ) {
    LOG.info("Metrics will be provided by tracker at :" + trackerURL);

    Client client = ClientBuilder.newClient();
    this.baseTarget = client.target(trackerURL)
        .path("topologies/metrics")
        .queryParam("cluster", cluster)
        .queryParam("environ", environ)
        .queryParam("topology", topologyName);

    this.baseTargetTimeline = client.target(trackerURL)
            .path("topologies/metricstimeline")
            .queryParam("cluster", cluster)
            .queryParam("environ", environ)
            .queryParam("topology", topologyName);
  }

  @Override
  public Map<String, ComponentMetrics> getComponentMetrics(String metric,
                                                           int durationSec,
                                                           String... components) {
    Map<String, ComponentMetrics> result = new HashMap<>();
    for (String component : components) {
      String response = getMetricsFromTracker(metric, component, durationSec);
      Map<String, InstanceMetrics> metrics = parse(response, component, metric);
      ComponentMetrics componentMetric = new ComponentMetrics(component, metrics);
      result.put(component, componentMetric);
    }
    return result;
  }

  @Override
  public Map<String, ComponentMetrics> getComponentMetrics(String metric,
                                                           int startTimeSec,
                                                           int durationSec,
                                                           String... components) {
    LOG.info("Calling TrackerMetricsProvider::getComponentMetrics");
    Map<String, ComponentMetrics> result = new HashMap<>();
    for (String component : components) {
      String response = getMetricsFromTrackerForTimeline(metric, component, startTimeSec, durationSec);

      ObjectMapper objectMapper = new ObjectMapper();
      try {
        JsonNode rootNode = objectMapper.readTree(response);
        if (!validateResponseJson(rootNode, metric)) {
          LOG.info("The response doesn't have the necessary fields");
          return result;
        }
        LOG.info(rootNode.toString());
        // We need to reorder the json as the metrics received by the json are not
        // chronologically sorted. 
        ComponentMetrics componentMetrics = sortJsonByTimestamp(rootNode, metric, component);
        LOG.info(componentMetrics.toString());
        if (componentMetrics != null) {
          result.put(component, componentMetrics);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return result;
  }

  private Map<String, InstanceMetrics> parse(String response, String component, String metric) {
    Map<String, InstanceMetrics> metricsData = new HashMap<>();

    Map<String, Map<String, String>> metricsMap = parseMetrics(response);
    if (metricsMap == null || metricsMap.get(metric) == null) {
      LOG.info(String.format("Did not get any metrics from tracker for %s:%s ", component, metric));
      return metricsData;
    }

    Map<String, String> instanceMetrics = metricsMap.get(metric);
    for (String instanceName : instanceMetrics.keySet()) {
      double value = Double.parseDouble(instanceMetrics.get(instanceName));
      InstanceMetrics instanceMetric = new InstanceMetrics(instanceName);
      instanceMetric.addMetric(metric, value);

      metricsData.put(instanceName, instanceMetric);
    }

    return metricsData;
  }

  private Map<String, Map<String, String>> parseMetrics(String response) {
    if (response == null || response.isEmpty()) {
      return null;
    }

    ObjectMapper mapper = new ObjectMapper();
    TrackerOutput output;
    try {
      output = mapper.readValue(response, TrackerOutput.class);
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse tracker response.", e);
    }

    if (output == null || output.getResult() == null || output.getResult().getMetrics() == null) {
      return null;
    }

    return output.getResult().getMetrics();
  }

  @Override
  public void close() {

  }

  @VisibleForTesting
  String getMetricsFromTracker(String metric, String component, int durationSec) {
    WebTarget target = baseTarget
        .queryParam("metricname", metric)
        .queryParam("component", component)
        .queryParam("interval", durationSec);
//    if (instance != null) {
//      target.queryParam("instance", instance.getName());
//    }

    LOG.fine("Tracker Query URI: " + target.getUri());

    Response r = target.request(MediaType.APPLICATION_JSON_TYPE).get();
    return r.readEntity(String.class);
  }

  @VisibleForTesting
  String getMetricsFromTrackerForTimeline(String metric, String component, int startTime, int durationSec) {
    WebTarget target = baseTargetTimeline
            .queryParam("metricname", metric)
            .queryParam("component", component)
            .queryParam("starttime", startTime)
            .queryParam("endtime", startTime + durationSec);
    LOG.info(target.toString());
    LOG.fine("Tracker Query URI: " + target.getUri());

    Response r = target.request(MediaType.APPLICATION_JSON_TYPE).get();
    return r.readEntity(String.class);
  }

  private boolean validateResponseJson(JsonNode rootNode, String metric) {
    boolean parseCondition = false;
    if (rootNode.has("result")) {
      if (rootNode.get("result").has("timeline")) {
        parseCondition = rootNode.get("result").get("timeline").has(metric);
      }
    }
    return parseCondition;
  }

  private ComponentMetrics sortJsonByTimestamp(JsonNode rootNode, String metric, String component) {
    // NOTE - In this class the metric name includes instance name as well.
    JsonNode metricNode = rootNode.get("result").get("timeline").get(metric);
    Iterator<String> iter = metricNode.fieldNames();
    while (iter.hasNext()) {
      String fieldName = iter.next();
      // TODO(harshad) - Remove the hard coded string stmgr.
      if (fieldName.startsWith("stmgr")) {
        JsonNode node = metricNode.get(fieldName);
        // We need to sort the metrics using the timestamps as the key.
        Iterator<Map.Entry<String, JsonNode>> metrics = node.fields();
        // Key = timestamp, value = the metric's value.
        Map<Long, Double> metricsSortedByTimeStamp = new TreeMap<>();
        while (metrics.hasNext()) {
          Map.Entry<String, JsonNode> currNode = metrics.next();
          String value = currNode.getValue().asText();
          String key = currNode.getKey();
          try {
            Double metricValueAsDouble = Double.parseDouble(value);
            metricsSortedByTimeStamp.put(Long.parseLong(key), metricValueAsDouble);
          } catch(NumberFormatException ne) {
            // The metric value is not a number, ignore the entry.
          }
        }
        ComponentMetrics componentMetrics = new ComponentMetrics(component);
        InstanceMetrics instanceMetrics = new InstanceMetrics(metric);
        instanceMetrics.addMetric(metric, metricsSortedByTimeStamp);
        componentMetrics.addInstanceMetric(instanceMetrics);
        return componentMetrics;
      }
    }
    return null;
  }
}