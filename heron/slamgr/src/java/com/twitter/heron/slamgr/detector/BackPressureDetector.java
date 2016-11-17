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
package com.twitter.heron.slamgr.detector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.slamgr.TopologyGraph;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsInfo;
import com.twitter.heron.spi.metricsmgr.sink.SinkVisitor;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlanProtoDeserializer;
import com.twitter.heron.spi.slamgr.Diagnosis;
import com.twitter.heron.spi.slamgr.IDetector;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.ReflectionUtils;

public class BackPressureDetector implements IDetector<BackPressureResult> {

  private static final Logger LOG = Logger.getLogger(BackPressureDetector.class.getName());
  private SinkVisitor visitor;
  private IStateManager statemgr;

  @Override
  public boolean initialize(Config config, SinkVisitor sVisitor) {
    this.visitor = sVisitor;
    String statemgrClass = Context.stateManagerClass(config);
    try {
      // create an instance of state manager
      statemgr = ReflectionUtils.newInstance(statemgrClass);
      // initialize the state manager
      statemgr.initialize(config);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      LOG.log(Level.SEVERE, "Failed to instantiate instances", e);
      return false;
    }
    return true;
  }



  @Override
  public Diagnosis<BackPressureResult> detect(TopologyAPI.Topology topology)
      throws RuntimeException {

    PackingPlan packingPlan = getPackingPlan(topology);
    BackPressureResult result = new BackPressureResult();
    for (PackingPlan.ContainerPlan containerPlan : packingPlan.getContainers()) {
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
        String name = "container_" + containerPlan.getId()
            + "_" + instancePlan.getComponentName()
            + "_" + instancePlan.getTaskId();
        System.out.println(name);
        Iterable<MetricsInfo> metricsResults =
            this.visitor.getNextMetric("__time_spent_back_pressure_by_compid/" + name, "__stmgr__");
        String[] parts = metricsResults.toString().replace("]", "").replace(" ", "").split("=");
        result.add(instancePlan.getComponentName(), containerPlan.getId(),
            instancePlan.getTaskId(), Integer.parseInt(parts[1]));
        System.out.println(result.toString());
      }
    }
    Set<BackPressureResult> diagnosis = new HashSet<BackPressureResult>();
    diagnosis.add(result);
    return new Diagnosis<BackPressureResult>(diagnosis);
  }

  private PackingPlan getPackingPlan(TopologyAPI.Topology topology) {
    SchedulerStateManagerAdaptor adaptor = new SchedulerStateManagerAdaptor(statemgr, 5000);

    // get a packed plan and schedule it
    PackingPlans.PackingPlan serializedPackingPlan = adaptor.getPackingPlan(topology.getName());
    if (serializedPackingPlan == null) {
      throw new RuntimeException(String.format("Failed to fetch PackingPlan for topology: %s " +
          "from the state manager", topology.getName()));
    }
    LOG.log(Level.INFO, "Packing plan fetched from state: {0}", serializedPackingPlan);
    PackingPlan packedPlan = new PackingPlanProtoDeserializer().fromProto(serializedPackingPlan);
    return packedPlan;
  }

  @Override
  public void close() {
  }
}


