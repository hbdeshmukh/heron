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

package com.twitter.heron.healthmgr.resolver;

import java.util.Set;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.healthmgr.Diagnosis;
import com.twitter.heron.spi.healthmgr.IResolver;
import com.twitter.heron.spi.healthmgr.InstanceBottleneck;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsInfo;

public class FailedTuplesResolver implements IResolver<InstanceBottleneck> {

  @Override
  public void initialize(Config config, Config runtime) {

  }

  @Override
  public Boolean resolve(Diagnosis<InstanceBottleneck> diagnosis, TopologyAPI.Topology topology) {
    Set<InstanceBottleneck> summary = diagnosis.getSummary();
    for (InstanceBottleneck result : summary) {
      for (MetricsInfo metric : result.getInstanceData().getMetrics()) {
        System.out.println("Instance " + result.getInstanceData().getInstanceId() + " in container "
            + result.getInstanceData().getContainerId()
            + " has " + metric.getValue() + " failed tuples.");
      }
    }
    return true;
  }

  @Override
  public void close() {

  }
}
