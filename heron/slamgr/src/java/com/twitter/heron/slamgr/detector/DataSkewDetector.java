//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License

package com.twitter.heron.slamgr.detector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.slamgr.outlierdetection.SimpleMADOutlierDetector;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsInfo;
import com.twitter.heron.spi.metricsmgr.sink.SinkVisitor;
import com.twitter.heron.spi.slamgr.ComponentBottleneck;
import com.twitter.heron.spi.slamgr.Diagnosis;
import com.twitter.heron.spi.slamgr.ThresholdBasedDetector;

/**
 * Detects the instances that have data skew.
 */
public class DataSkewDetector extends ThresholdBasedDetector<ComponentBottleneck> {

  private SinkVisitor visitor;

  public DataSkewDetector(double threshold) {
    super(threshold);
  }

  @Override
  public boolean initialize(Config config, SinkVisitor sVisitor) {
    this.visitor = sVisitor;
    return true;
  }

  @Override
  public Diagnosis<ComponentBottleneck> detect(TopologyAPI.Topology topology) {
    List<TopologyAPI.Bolt> bolts = topology.getBoltsList();
    String[] boltNames = new String[bolts.size()];

    Set<ComponentBottleneck> bottlenecks = new HashSet<ComponentBottleneck>();

    for (int i = 0; i < boltNames.length; i++) {
      String component = bolts.get(i).getComp().getName();
      Iterable<MetricsInfo> metricsResults = this.visitor.getNextMetric("__execute-count/default",
          component);

      ComponentBottleneck currentBottleneck;
      currentBottleneck = new ComponentBottleneck(component);

      ArrayList<Double> data = new ArrayList<>();
      for (MetricsInfo metricsInfo : metricsResults) {
        data.add(Double.parseDouble(metricsInfo.getValue()));
      }

      //detect outliers for each bolt
      Double[] dataPoints = new Double[data.size()];
      data.toArray(dataPoints);
      SimpleMADOutlierDetector outlierDetector = new SimpleMADOutlierDetector(this.getThreshold());
      outlierDetector.load(dataPoints);
      int[] outliers = outlierDetector.detectOutliers();

      int current = 0;
      if (outliers.length != 0) {
        for (int j = 0; j < outliers.length; j++) {
          for (MetricsInfo metricsInfo : metricsResults) {
            if (current == j) {
              String[] parts = metricsInfo.getName().split("_");
              Set<MetricsInfo> metrics = new HashSet<>();
              metrics.add(new MetricsInfo("__execute-count/default", metricsInfo.getValue()));
              currentBottleneck.add(Integer.parseInt(parts[1]),
                  Integer.parseInt(parts[3]), metrics);
            }
            current++;
          }
        }
        bottlenecks.add(currentBottleneck);
      }

    }
    // for (ComponentBottleneck s : bottlenecks) {
    //   System.out.println(s.toString());
    // }
    return new Diagnosis<ComponentBottleneck>(bottlenecks);
  }

  @Override
  public void close() {
  }

}


