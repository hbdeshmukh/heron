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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.healthmgr.utils.SLAManagerUtils;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.healthmgr.ComponentBottleneck;
import com.twitter.heron.spi.healthmgr.Diagnosis;
import com.twitter.heron.spi.healthmgr.IDetector;
import com.twitter.heron.spi.metricsmgr.sink.SinkVisitor;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlanProtoDeserializer;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;

public class BackPressureDetector implements IDetector<ComponentBottleneck> {

  private static final Logger LOG = Logger.getLogger(BackPressureDetector.class.getName());
  private static final String BACKPRESSURE_METRIC = "__time_spent_back_pressure_by_compid";
  private Config runtime;

  @Override
  public void initialize(Config inputConfig, Config inputRuntime) {
    this.runtime = inputRuntime;
  }

  @Override
  public Diagnosis<ComponentBottleneck> detect(TopologyAPI.Topology topology)
      throws RuntimeException {
    LOG.info("Executing: " + this.getClass().getName());
    SinkVisitor visitor = Runtime.metricsReader(runtime);
    PackingPlan packingPlan = getPackingPlan(topology);

    HashMap<String, ComponentBottleneck> results = SLAManagerUtils.retrieveMetricValues(
        BACKPRESSURE_METRIC, "", "__stmgr__", visitor, packingPlan);

    Set<ComponentBottleneck> bottlenecks = new HashSet<ComponentBottleneck>();
    for (ComponentBottleneck bottleneck : results.values()) {
      if (bottleneck.containsNonZero(BACKPRESSURE_METRIC)) {
        //System.out.println("bottleneck name " + bottleneck.getComponentName().toString());
        bottlenecks.add(bottleneck);
      }
    }

    return new Diagnosis<ComponentBottleneck>(bottlenecks);
  }

  private PackingPlan getPackingPlan(TopologyAPI.Topology topology) {
    // TODO this could be optimized
    SchedulerStateManagerAdaptor adaptor = Runtime.schedulerStateManagerAdaptor(runtime);
    PackingPlans.PackingPlan protoPackingPlan = adaptor.getPackingPlan(topology.getName());
    PackingPlanProtoDeserializer deserializer = new PackingPlanProtoDeserializer();
    return deserializer.fromProto(protoPackingPlan);
  }

  @Override
  public boolean similarDiagnosis(Diagnosis<ComponentBottleneck> firstDiagnosis,
                                  Diagnosis<ComponentBottleneck> secondDiagnosis) {
    return false;
  }

  @Override
  public void close() {
  }
}


