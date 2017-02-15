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

package com.twitter.heron.healthmgr.detector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.healthmgr.detector.outlierdetection.SimpleMADOutlierDetector;
import com.twitter.heron.healthmgr.utils.SLAManagerUtils;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.healthmgr.ComponentBottleneck;
import com.twitter.heron.spi.healthmgr.Diagnosis;
import com.twitter.heron.spi.healthmgr.ThresholdBasedDetector;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsInfo;
import com.twitter.heron.spi.metricsmgr.sink.SinkVisitor;

/**
 * Detects the instances that have data skew.
 */
public class SkewDetector extends ThresholdBasedDetector<ComponentBottleneck> {

  private SinkVisitor visitor;
  private String metric;

  public SkewDetector(String metric, double threshold) {
    super(threshold);
    this.metric = metric;
  }

  @Override
  public void initialize(Config config, Config runtime) {
    this.visitor = Runtime.metricsReader(runtime);
  }

  @Override
  public Diagnosis<ComponentBottleneck> detect(TopologyAPI.Topology topology) {
    List<TopologyAPI.Bolt> bolts = topology.getBoltsList();
    String[] boltNames = new String[bolts.size()];

    Set<ComponentBottleneck> bottlenecks = new HashSet<ComponentBottleneck>();

    for (int i = 0; i < boltNames.length; i++) {
      String component = bolts.get(i).getComp().getName();
      Iterable<MetricsInfo> metricsResults = this.visitor.getNextMetric(metric,
          component);

      //detect outliers
      Double[] dataPoints = SLAManagerUtils.getDoubleDataPoints(metricsResults);
      SimpleMADOutlierDetector outlierDetector = new SimpleMADOutlierDetector(this.getThreshold());
      ArrayList<Integer> outliers = outlierDetector.detectOutliers(dataPoints);

      if (outliers.size() != 0) {
        ComponentBottleneck currentBottleneck;
        currentBottleneck = new ComponentBottleneck(component);
        for (int j = 0; j < outliers.size(); j++) {
          int current = 0;
          for (MetricsInfo metricsInfo : metricsResults) {
            //System.out.println(current + " " + j + outliers.get(j));
            if (current == outliers.get(j)) {
              SLAManagerUtils.updateComponentBottleneck(currentBottleneck, metric,
                  metricsInfo);
            }
            current++;
          }
        }
        bottlenecks.add(currentBottleneck);
      }

    }

    return new Diagnosis<ComponentBottleneck>(bottlenecks);
  }


  @Override
  public boolean similarDiagnosis(Diagnosis<ComponentBottleneck> firstDiagnosis,
                                  Diagnosis<ComponentBottleneck> secondDiagnosis){
    return false;
  }

  @Override
  public void close() {
  }

}


