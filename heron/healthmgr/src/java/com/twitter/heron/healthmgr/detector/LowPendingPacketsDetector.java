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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.healthmgr.services.DetectorService;
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

public class LowPendingPacketsDetector implements IDetector<ComponentBottleneck> {

  private static final Logger LOG = Logger.getLogger(LowPendingPacketsDetector.class.getName());
  private static final String AVG_PENDING_PACKETS = "__connection_buffer_by_intanceid";
  private SinkVisitor visitor;
  private Config runtime;
  private int packetThreshold = 0;
  private BackPressureDetector backpressureDetector = new BackPressureDetector();
  private DetectorService detectorService;

  @Override
  public void initialize(Config inputConfig, Config inputRuntime) {
    this.runtime = inputRuntime;
    this.visitor = Runtime.metricsReader(runtime);
    this.backpressureDetector.initialize(inputConfig, runtime);
    detectorService = (DetectorService) Runtime.getDetectorService(runtime);
    this.packetThreshold =
        Integer.valueOf(inputConfig.getStringValue("health.policy.scaledown.low.packet.limit"));

    LOG.info("Scale down detector's low packet limit is " + packetThreshold);
  }

  @Override
  public Diagnosis<ComponentBottleneck> detect(TopologyAPI.Topology topology)
      throws RuntimeException {

    Diagnosis<ComponentBottleneck> backPressuredDiagnosis =
        detectorService.run(backpressureDetector, topology);
    if(backPressuredDiagnosis.getSummary().size() == 0) {
      LOG.info("Executing: " + this.getClass().getName());
      PackingPlan packingPlan = BackPressureDetector.getPackingPlan(topology, runtime);
      HashMap<String, ComponentBottleneck> results = SLAManagerUtils.retrieveMetricValues(
          AVG_PENDING_PACKETS, "packets", "__stmgr__", this.visitor, packingPlan);

      List<TopologyAPI.Spout> spouts = topology.getSpoutsList();

      Set<ComponentBottleneck> bottlenecks = new HashSet<ComponentBottleneck>();
      for (ComponentBottleneck bottleneck : results.values()) {
        //System.out.println(bottleneck.toString()) ;
        int position = contains(spouts, bottleneck.getComponentName());
        if (position == -1 &&
            bottleneck.containsBelow(AVG_PENDING_PACKETS, String.valueOf(packetThreshold))) {
          //System.out.println("bottleneck name " + bottleneck.getComponentName().toString());
          bottlenecks.add(bottleneck);
        }
      }
      return new Diagnosis<ComponentBottleneck>(bottlenecks);
    }
    return null;
  }

  private int contains(List<TopologyAPI.Spout> spouts, String name) {
    for (int i = 0; i < spouts.size(); i++) {
      if (spouts.get(i).getComp().getName().equals(name)) {
        return i;
      }
    }
    return -1;
  }

  @Override
  public boolean similarDiagnosis(Diagnosis<ComponentBottleneck> firstDiagnosis,
                                  Diagnosis<ComponentBottleneck> secondDiagnosis){
    return false;
  }


  @Override
  public void close() {
    this.backpressureDetector.close();
  }
}


