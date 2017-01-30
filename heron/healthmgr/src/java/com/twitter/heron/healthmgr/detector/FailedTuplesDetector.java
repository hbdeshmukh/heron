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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.healthmgr.Diagnosis;
import com.twitter.heron.spi.healthmgr.IDetector;
import com.twitter.heron.spi.healthmgr.InstanceBottleneck;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsInfo;
import com.twitter.heron.spi.metricsmgr.sink.SinkVisitor;

/**
 * Detects the instances that have failed tuples.
 */
public class FailedTuplesDetector implements IDetector<InstanceBottleneck> {
  private SinkVisitor visitor;

  @Override
  public void initialize(Config config, Config runtime) {
    visitor = Runtime.metricsReader(runtime);
  }

  @Override
  public Diagnosis<InstanceBottleneck> detect(TopologyAPI.Topology topology) {
    List<TopologyAPI.Bolt> bolts = topology.getBoltsList();
    String[] boltNames = new String[bolts.size()];
    for (int i = 0; i < boltNames.length; i++) {
      boltNames[i] = bolts.get(i).getComp().getName();
    }
    Iterable<MetricsInfo> metricsResults = this.visitor.getNextMetric("__fail-count/default",
        boltNames);
    Set<InstanceBottleneck> instanceInfo = new HashSet<InstanceBottleneck>();
    for (MetricsInfo metricsInfo : metricsResults) {
      String[] parts = metricsInfo.getName().split("_");
      Set<MetricsInfo> metrics = new HashSet<>();
      metrics.add(new MetricsInfo("__fail-count/default", metricsInfo.getValue()));
      instanceInfo.add(new InstanceBottleneck(Integer.parseInt(parts[1]),
          Integer.parseInt(parts[3]), metrics));
    }
    return new Diagnosis<InstanceBottleneck>(instanceInfo);
  }

  @Override
  public void close() {
  }
}


