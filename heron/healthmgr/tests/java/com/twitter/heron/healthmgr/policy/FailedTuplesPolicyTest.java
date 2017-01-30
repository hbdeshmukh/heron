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

package com.twitter.heron.healthmgr.policy;

import org.junit.Test;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.healthmgr.sinkvisitor.TrackerVisitor;
import com.twitter.heron.spi.utils.TopologyTests;

public class FailedTuplesPolicyTest {

  private static final String BOLT_NAME = "exclaim1";
  private static final String SPOUT_NAME = "word";

  private TopologyAPI.Topology getTopology(
      int spoutParallelism, int boltParallelism,
      com.twitter.heron.api.Config topologyConfig) {
    return TopologyTests.createTopology("ExclamationTopology", topologyConfig, SPOUT_NAME,
        BOLT_NAME, spoutParallelism, boltParallelism);
  }

  @Test
  public void testDetector() throws InterruptedException {

    TopologyAPI.Topology topology = getTopology(1, 2, new com.twitter.heron.api.Config());
    TrackerVisitor visitor = new TrackerVisitor();
    visitor.initialize(null, null);

    FailedTuplesPolicy policy = new FailedTuplesPolicy();
    policy.initialize(null, null);

    for (int i = 0; i < 10; i++) {
      policy.execute();
    }
  }
}
