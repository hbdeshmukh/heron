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


package com.twitter.heron.healthmgr.sinkvisitor;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.healthmgr.detector.DataSkewDetector;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsInfo;
import com.twitter.heron.spi.metricsmgr.sink.SinkVisitor;

public class TrackerVisitor implements SinkVisitor {
  private static final Logger LOG = Logger.getLogger(TrackerVisitor.class.getName());
  public static final int INTERVAL = 300;

  private WebTarget baseTarget;
  private WebTarget baseTargetWithStartAndEndTimes;

  @Override
  public void initialize(Config conf, Config runtime) {
    TopologyAPI.Topology topology = Runtime.topology(runtime);
    String trackerURL = Runtime.trackerURL(runtime);
    LOG.info("Metrics will be read from:" + trackerURL);

    Client client = ClientBuilder.newClient();
    this.baseTarget = client.target(trackerURL)
        .path("topologies/metrics")
        .queryParam("cluster", Context.cluster(conf))
        .queryParam("environ", "default")
        .queryParam("topology", topology.getName())
        .queryParam("interval", INTERVAL);

    this.baseTargetWithStartAndEndTimes = client.target(trackerURL)
            .path("topologies/metricstimeline")
            .queryParam("cluster", Context.cluster(conf))
            .queryParam("environ", "default")
            .queryParam("topology", topology.getName());
  }

  @Override
  public Collection<MetricsInfo> getNextMetric(String metric, String... component) {
    List<MetricsInfo> metricsInfo = new ArrayList<MetricsInfo>();
    for (int j = 0; j < component.length; j++) {
      WebTarget target = baseTarget.queryParam("metricname", metric)
          .queryParam("component", component[j]);
      Response r = target.request(MediaType.APPLICATION_JSON_TYPE).get();
      TrackerOutput result = r.readEntity(TrackerOutput.class);

      List<MetricsInfo> tmp = convert(result, metric);
      metricsInfo.addAll(tmp);
    }
    return metricsInfo;
  }

  @Override
  public Collection<MetricsInfo> getNextMetric(String metric, long startTime, long endTime, String... component) {
    // This method gets called separately for each instance.
    List<MetricsInfo> metricsInfo = new ArrayList<>();
    for (int j = 0; j < component.length; j++) {
      WebTarget target = baseTargetWithStartAndEndTimes.queryParam("metricname", metric)
              .queryParam("component", component[j])
              .queryParam("starttime", startTime)
              .queryParam("endtime", endTime);
      System.out.println(target.toString());
      Response r = target.request(MediaType.APPLICATION_JSON_TYPE).get();
      String responseAsJson = r.readEntity(String.class);
      // NOTE(harshad) - The string parsing of the Json response reorders the metrics somehow.
      ObjectMapper objectMapper = new ObjectMapper();
      try {
        JsonNode rootNode = objectMapper.readTree(responseAsJson);
        JsonNode metricNode = rootNode.get("result").get("timeline").get(metric);
        Iterator<String> iter = metricNode.fieldNames();
        while (iter.hasNext()) {
          String fieldName = iter.next();
          if (fieldName.startsWith("stmgr-")) {
            JsonNode node = metricNode.get(fieldName);
            // We need to sort the metrics using the timestamps as the key.
            Iterator<Map.Entry<String, JsonNode>> metrics = node.fields();
            Map<Double, String> metricsSortedByTimeStamp = new TreeMap<>();
            while (metrics.hasNext()) {
              Map.Entry<String, JsonNode> currNode = metrics.next();
              String value = currNode.getValue().asText();
              String key = currNode.getKey();
              try {
                Double metricValueAsDouble = Double.parseDouble(value);
                metricsSortedByTimeStamp.put(metricValueAsDouble, value);
              } catch(NumberFormatException ne) {
                // The metric value is not a number, ignore the entry.
              }
            }
            // Now iterate over metricsSortedByTimeStamp and insert the sorted entries in metricsInfo.
            for (Double currTimestamp : metricsSortedByTimeStamp.keySet()) {
              metricsInfo.add(new MetricsInfo(metric, metricsSortedByTimeStamp.get(currTimestamp)));
            }
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return metricsInfo;
  }

  @Override
  public void close() {
  }

  private List<MetricsInfo> convert(TrackerOutput output, String metricName) {
    List<MetricsInfo> metricsInfo = new ArrayList<MetricsInfo>();
    if (output == null || output.getResult() == null || output.getResult().getMetrics() == null) {
      LOG.info("No metric received: " + metricName);
      return metricsInfo;
    }

    Map<String, String> instanceData = output.getResult().getMetrics().get(metricName);
    if (instanceData != null) {
      for (String instanceName : instanceData.keySet()) {
        Double value = Double.parseDouble(instanceData.get(instanceName)) / INTERVAL;
        if (metricName.contains(DataSkewDetector.AVG_PENDING_PACKETS)) {
          value *= INTERVAL / 60;
          // avg pending count returns per second average over a minute long window
        }
        metricsInfo.add(new MetricsInfo(instanceName, String.valueOf(value.longValue())));
      }
    }
    return metricsInfo;
  }
}
