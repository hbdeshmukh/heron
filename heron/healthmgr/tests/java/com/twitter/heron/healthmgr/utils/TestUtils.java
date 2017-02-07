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

package com.twitter.heron.healthmgr.utils;

import com.google.common.util.concurrent.SettableFuture;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.packing.roundrobin.RoundRobinPacking;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.spi.common.ClusterDefaults;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Key;
import com.twitter.heron.spi.packing.IPacking;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlanProtoSerializer;

public final class TestUtils {

  private static final String BOLT_NAME = "exclaim1";
  private static final String SPOUT_NAME = "word";

  private TestUtils() {
  }

  public static TopologyAPI.Topology getTopology(String topologyName) {
    TopologyBuilder topologyBuilder = new TopologyBuilder();

    topologyBuilder.setSpout(SPOUT_NAME, new TestSpout(), 2);

    topologyBuilder.setBolt(BOLT_NAME, new TestBolt(), 2).
        shuffleGrouping(SPOUT_NAME);

    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();
    topologyConfig.put(com.twitter.heron.api.Config.TOPOLOGY_STMGRS, 2);


    TopologyAPI.Topology topology =
        topologyBuilder.createTopology().
            setName(topologyName).
            setConfig(topologyConfig).
            setState(TopologyAPI.TopologyState.RUNNING).
            getTopology();
    return topology;
  }

  public static PackingPlan getPackingPlan(TopologyAPI.Topology topology, IPacking packing) {

    Config config = Config.newBuilder()
        .put(Key.TOPOLOGY_ID, topology.getId())
        .put(Key.TOPOLOGY_NAME, topology.getName())
        .putAll(ClusterDefaults.getDefaults())
        .build();

    packing.initialize(config, topology);
    return packing.pack();
  }

  public static PackingPlans.PackingPlan testProtoPackingPlan(
      TopologyAPI.Topology topology, IPacking packing) {
    PackingPlan plan = getPackingPlan(topology, packing);
    PackingPlanProtoSerializer serializer = new PackingPlanProtoSerializer();
    return serializer.toProto(plan);
  }

  public static SettableFuture<PackingPlans.PackingPlan> getTestPacking(
      TopologyAPI.Topology topology) {
    PackingPlans.PackingPlan packingPlan =
        testProtoPackingPlan(topology, new RoundRobinPacking());
    final SettableFuture<PackingPlans.PackingPlan> future = SettableFuture.create();
    future.set(packingPlan);
    return future;
  }

}
