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

package com.twitter.heron.slamgr.policy;

import com.google.common.util.concurrent.SettableFuture;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.slamgr.sinkvisitor.TrackerVisitor;
import com.twitter.heron.slamgr.utils.TestBolt;
import com.twitter.heron.slamgr.utils.TestSpout;
import com.twitter.heron.slamgr.utils.TestUtils;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.ConfigKeys;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.utils.ReflectionUtils;
import com.twitter.heron.spi.utils.TopologyUtils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
    TopologyUtils.class, ReflectionUtils.class, TopologyAPI.Topology.class})
public class BackPressurePolicyTest {

  private static final String STATE_MANAGER_CLASS = "STATE_MANAGER_CLASS";
  private IStateManager stateManager;
  private Config config;
  private TopologyAPI.Topology topology;

  /**
   * Basic setup before executing a test case
   */
  @Before
  public void setUp() throws Exception {
    this.topology = TestUtils.getTopology("DataSkewTopology");
    config = mock(Config.class);
    when(config.getStringValue(ConfigKeys.get("STATE_MANAGER_CLASS"))).
        thenReturn(STATE_MANAGER_CLASS);

    // Mock objects to be verified
    stateManager = mock(IStateManager.class);

    final SettableFuture<PackingPlans.PackingPlan> future = TestUtils.getTestPacking(this.topology);
    when(stateManager.getPackingPlan(null, "DataSkewTopology")).thenReturn(future);

    // Mock ReflectionUtils stuff
    PowerMockito.spy(ReflectionUtils.class);
    PowerMockito.doReturn(stateManager).
        when(ReflectionUtils.class, "newInstance", STATE_MANAGER_CLASS);
  }

  @Test
  public void testDetector() throws InterruptedException {

    TrackerVisitor visitor = new TrackerVisitor();
    visitor.initialize(null, topology);

    BackPressurePolicy policy = new BackPressurePolicy();
    policy.initialize(config, null, topology, visitor);

    for (int i = 0; i < 10; i++) {
      policy.execute();
    }
  }
}
