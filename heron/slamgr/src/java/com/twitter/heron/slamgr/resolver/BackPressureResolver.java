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
package com.twitter.heron.slamgr.resolver;

import java.util.ArrayList;
import java.util.Set;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.slamgr.TopologyGraph;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.slamgr.ComponentBottleneck;
import com.twitter.heron.spi.slamgr.Diagnosis;
import com.twitter.heron.spi.slamgr.IResolver;

public class BackPressureResolver implements IResolver<ComponentBottleneck> {

  private static final Logger LOG = Logger.getLogger(BackPressureResolver.class.getName());
  private ArrayList<String> topologySort = null;

  @Override
  public void initialize(Config config) {

  }

  @Override
  public Boolean resolve(Diagnosis<ComponentBottleneck> diagnosis, TopologyAPI.Topology topology) {

    if (topologySort == null) {
      topologySort = getTopologySort(topology);
    }

    boolean found = false;
    //is there data skew?
    //do we need autoscaling?
   Set<ComponentBottleneck> summary = diagnosis.getSummary();
    if (summary.size() != 0) {
      for (int i = 0; i < topologySort.size() && !found; i++) {
        String name = topologySort.get(i);
        if (appears(summary,name)) {
          //System.out.println(name);
        }
      }
    }

    return null;
  }

  private boolean appears(Set<ComponentBottleneck> summary, String component){
    for(ComponentBottleneck bottleneck: summary){
      if(bottleneck.getComponentName().equals(component)){
        return true;
      }
    }
    return false;
  }

  @Override
  public void close() {

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
