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

package com.twitter.heron.healthmgr.detector;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.healthmgr.services.DetectorService;
import com.twitter.heron.healthmgr.sinkvisitor.TrackerVisitor;
import com.twitter.heron.healthmgr.utils.TestUtils;
import com.twitter.heron.packing.roundrobin.ResourceCompliantRRPacking;
import com.twitter.heron.packing.roundrobin.RoundRobinPacking;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Key;
import com.twitter.heron.spi.healthmgr.ComponentBottleneck;
import com.twitter.heron.spi.healthmgr.Diagnosis;
import com.twitter.heron.spi.packing.PackingPlan;


public class LowPendingPacketsDetectorTest {
  private Config config;
  private Config runtime;

  private TopologyAPI.Topology topology;

  /**
   * Basic setup before executing a test case
   */
  @Before
  public void setUp() throws Exception {
    this.topology = TestUtils.getTopology("ex");
    config = Config.newBuilder()
        .put(Key.REPACKING_CLASS, ResourceCompliantRRPacking.class.getName())
        .put(Key.INSTANCE_CPU, "1")
        .put(Key.INSTANCE_RAM, ByteAmount.fromMegabytes(192).asBytes())
        .put(Key.INSTANCE_DISK, ByteAmount.fromGigabytes(1).asBytes())
        .put(Key.TRACKER_URL, "http://localhost:8888")
        .put(Key.CLUSTER, "local")
        .put("health.policy.scaledown.low.packet.limit", 500)
        .build();

    RoundRobinPacking packing = new RoundRobinPacking();
    packing.initialize(config, topology);
    PackingPlan packingPlan = packing.pack();


    TrackerVisitor visitor = new TrackerVisitor();
    runtime = Config.newBuilder()
        .put(Key.TOPOLOGY_DEFINITION, topology)
        .put(Key.TRACKER_URL, "http://localhost:8888")
        .put(Key.PACKING_PLAN, packingPlan)
        .put(Key.METRICS_READER_INSTANCE, visitor)
        .put(Key.HEALTH_MGR_DETECTOR_SERVICE, new DetectorService())
        .build();
    visitor.initialize(config, runtime);
  }

  @Test
  public void testDetector() {

    TrackerVisitor visitor = new TrackerVisitor();
    visitor.initialize(config, runtime);

    LowPendingPacketsDetector detector = new LowPendingPacketsDetector();
    detector.initialize(config, runtime);
    Diagnosis<ComponentBottleneck> result = detector.detect(topology);
    Assert.assertEquals(1, result.getSummary().size());
  }
}
