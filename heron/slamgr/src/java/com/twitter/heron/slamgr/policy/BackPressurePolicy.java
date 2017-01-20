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


package com.twitter.heron.slamgr.policy;

import java.util.ArrayList;
import java.util.Set;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.slamgr.TopologyGraph;
import com.twitter.heron.slamgr.detector.BackPressureDetector;
import com.twitter.heron.slamgr.detector.FailedTuplesDetector;
import com.twitter.heron.slamgr.resolver.FailedTuplesResolver;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.metricsmgr.sink.SinkVisitor;
import com.twitter.heron.spi.slamgr.ComponentBottleneck;
import com.twitter.heron.spi.slamgr.Diagnosis;
import com.twitter.heron.spi.slamgr.InstanceBottleneck;
import com.twitter.heron.spi.slamgr.SLAPolicy;

import static com.twitter.heron.spi.slamgr.utils.BottleneckUtils.appears;

public class BackPressurePolicy implements SLAPolicy {

  private BackPressureDetector detector = new BackPressureDetector();
  private FailedTuplesResolver resolver = new FailedTuplesResolver();

  private TopologyAPI.Topology topology;
  private ArrayList<String> topologySort = null;


  @Override
  public void initialize(Config conf, TopologyAPI.Topology t,
                         SinkVisitor visitor) {
    this.topology = t;
    detector.initialize(conf, visitor);
    resolver.initialize(conf);
  }

  @Override
  public void execute() {
    Diagnosis<ComponentBottleneck> diagnosis = detector.detect(topology);
    boolean found = false;

    if (diagnosis != null) {

      if (topologySort == null) {
        topologySort = getTopologySort(topology);
      }
      Set<ComponentBottleneck> summary = diagnosis.getSummary();
      if (summary.size() != 0) {
        for (int i = 0; i < topologySort.size() && !found; i++) {
          String name = topologySort.get(i);
          if (appears(summary,name)) {
            System.out.println(name);
            //data skew detector
            //slow host detector
            //network partitioning
          }
        }
      }
      //resolver.resolve(diagnosis, topology);
    }
  }

  @Override
  public void close() {
    detector.close();
    resolver.close();
  }

  private ArrayList<String> getTopologySort(TopologyAPI.Topology topology) {
    TopologyGraph topologyGraph = new TopologyGraph();
    for (TopologyAPI.Bolt.Builder bolt : topology.toBuilder().getBoltsBuilderList()) {
      String boltName = bolt.getComp().getName();

      // To get the parent's component to construct a graph of topology structure
      for (TopologyAPI.InputStream inputStream : bolt.getInputsList()) {
        String parent = inputStream.getStream().getComponentName();
        topologyGraph.addEdge(parent, boltName);
      }
    }
    return topologyGraph.topologicalSort();
  }
}
