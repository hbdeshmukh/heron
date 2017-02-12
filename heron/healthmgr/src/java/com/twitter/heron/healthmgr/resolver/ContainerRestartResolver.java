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

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.classification.InterfaceAudience;
import com.twitter.heron.healthmgr.utils.SLAManagerUtils;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.scheduler.TopologyRuntimeManagementException;
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.healthmgr.ComponentBottleneck;
import com.twitter.heron.spi.healthmgr.Diagnosis;
import com.twitter.heron.spi.healthmgr.IResolver;
import com.twitter.heron.spi.healthmgr.InstanceBottleneck;

public class ContainerRestartResolver implements IResolver<InstanceBottleneck> {
  private static final Logger LOG = Logger.getLogger(ContainerRestartResolver.class.getName());
  private Config runtime;
  private ISchedulerClient schedulerClient;
  private TopologyAPI.Topology topology;

  @Override
  public void initialize(Config inputConfig, Config inputRuntime) {
    this.runtime = inputRuntime;
    this.schedulerClient = (ISchedulerClient) Runtime.schedulerClientInstance(runtime);
    this.topology = Runtime.topology(runtime);
  }

  @Override
  public Boolean resolve(Diagnosis<InstanceBottleneck> diagnosis, TopologyAPI.Topology topology) {
    if (diagnosis.getSummary() == null) {
      throw new RuntimeException("Not valid diagnosis object");
    }

    // unique set of containers to be restarted
    Set<Integer> containerIds = new HashSet<>();
    for (InstanceBottleneck bottleneck : diagnosis.getSummary()) {
      containerIds.add(bottleneck.getInstanceData().getContainerId());
    }

    String topologyName = topology.getName();
    for (Integer containerId : containerIds) {
      Scheduler.RestartTopologyRequest restartTopologyRequest =
          Scheduler.RestartTopologyRequest.newBuilder()
              .setTopologyName(topologyName)
              .setContainerIndex(containerId)
              .build();

      LOG.log(Level.INFO, "Restarting topology's container, {1}:{0}",
          new Object[]{containerId, topologyName});

      if (!schedulerClient.restartTopology(restartTopologyRequest)) {
        throw new TopologyRuntimeManagementException(String.format(
            "Failed to restart topology's container, %s:%d", topologyName, containerId));
      }
    }


    // Clean the connection when we are done.
    LOG.fine("Scheduler updated topology successfully.");
    return true;
  }

  /**
   * Called to compute the expected outcome of the resolver given a diagnosis
   */
  @Override
  public double estimateOutcome(Diagnosis<InstanceBottleneck> diagnosis,
                                TopologyAPI.Topology topology){
    return 0;
  }


  /**
   * Checks whether the newDiagnosis reflects the improvement expected
   * by resolving the oldDiagnosis
   */
  @Override
  public boolean successfulAction(Diagnosis<InstanceBottleneck> oldDiagnosis,
                                  Diagnosis<InstanceBottleneck> newDiagnosis,
                           double improvement){
    return true;
  }

  @Override
  public void close() {
  }
}
