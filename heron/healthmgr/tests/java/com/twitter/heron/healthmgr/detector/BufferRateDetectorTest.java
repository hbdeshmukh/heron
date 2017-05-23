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

import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.statemgr.localfs.LocalFileSystemStateManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.healthmgr.sinkvisitor.TrackerVisitor;
import com.twitter.heron.healthmgr.utils.TestUtils;
import com.twitter.heron.packing.roundrobin.ResourceCompliantRRPacking;
import com.twitter.heron.packing.roundrobin.RoundRobinPacking;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Key;
import com.twitter.heron.spi.healthmgr.ComponentBottleneck;
import com.twitter.heron.spi.healthmgr.Diagnosis;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.utils.ReflectionUtils;
import com.twitter.heron.spi.utils.TopologyUtils;


@RunWith(PowerMockRunner.class)
@PrepareForTest({
        TopologyUtils.class, ReflectionUtils.class, TopologyAPI.Topology.class})
public class BufferRateDetectorTest {
  private Config config;
  private TopologyAPI.Topology topology;
  private Config runtime;
  private IStateManager stateManager;

  /**
   * Basic setup before executing a test case
   */
  @Before
  public void setUp() throws Exception {
    this.topology = TestUtils.getTopology("NoAckTopology3Stages");
    config = Config.newBuilder()
            .put(Key.REPACKING_CLASS, ResourceCompliantRRPacking.class.getName())
            .put(Key.INSTANCE_CPU, "1")
            .put(Key.INSTANCE_RAM, ByteAmount.fromMegabytes(192).asBytes())
            .put(Key.INSTANCE_DISK, ByteAmount.fromGigabytes(1).asBytes())
            .put(Key.STATEMGR_ROOT_PATH, "/home/osboxes/.herondata/repository/state/local")
            .put(Key.STATE_MANAGER_CLASS, LocalFileSystemStateManager.class.getName())
            .put(Key.TOPOLOGY_NAME, "NoAckTopology3Stages")
            .put(Key.CLUSTER, "local")
            .put(Key.TRACKER_URL, "http://localhost:8888")
            .put(Key.SCHEDULER_IS_SERVICE, true)
            .build();

    stateManager = new LocalFileSystemStateManager();
    stateManager.initialize(config);

    SchedulerStateManagerAdaptor adaptor =
            new SchedulerStateManagerAdaptor(stateManager, 5000);

    RoundRobinPacking packing = new RoundRobinPacking();
    packing.initialize(config, topology);
    PackingPlan packingPlan = packing.pack();
    System.out.print(packingPlan.getContainers().toString());

    TrackerVisitor visitor = new TrackerVisitor();
    runtime = Config.newBuilder()
            .put(Key.TOPOLOGY_DEFINITION, topology)
            .put(Key.SCHEDULER_STATE_MANAGER_ADAPTOR, adaptor)
            .put(Key.TRACKER_URL, "http://localhost:8888")
            .put(Key.PACKING_PLAN, packingPlan)
            .put(Key.METRICS_READER_INSTANCE, visitor)
            .build();
    visitor.initialize(config, runtime);
  }

  @Test
  public void testDetector() {

    BufferRateDetector detector = new BufferRateDetector();
    detector.initialize(config, runtime);

    Diagnosis<ComponentBottleneck> result = detector.detect(topology);
    Assert.assertEquals(1, result.getSummary().size());
  }
}
