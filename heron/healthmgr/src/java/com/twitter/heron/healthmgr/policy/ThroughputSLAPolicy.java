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

import java.util.List;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.healthmgr.detector.BackPressureDetector;
import com.twitter.heron.healthmgr.detector.ReportingDetector;
import com.twitter.heron.healthmgr.resolver.SpoutScaleUpResolver;
import com.twitter.heron.healthmgr.services.DetectorService;
import com.twitter.heron.healthmgr.services.ResolverService;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.healthmgr.ComponentBottleneck;
import com.twitter.heron.spi.healthmgr.Diagnosis;
import com.twitter.heron.spi.healthmgr.HealthPolicy;


public class ThroughputSLAPolicy implements HealthPolicy {

  private static final String EMIT_COUNT_METRIC = "__emit-count/default";
  private BackPressureDetector backPressureDetector = new BackPressureDetector();
  private ReportingDetector emitCountDetector = new ReportingDetector(EMIT_COUNT_METRIC);

  private SpoutScaleUpResolver spoutScaleUpResolver = new SpoutScaleUpResolver();
  private TopologyAPI.Topology topology;

  private DetectorService detectorService;
  private ResolverService resolverService;
  private double maxThroughput = 0;

  private boolean performedAction = false;

  @Override
  public void initialize(Config conf, Config runtime) {
    this.topology = Runtime.topology(runtime);

    backPressureDetector.initialize(conf, runtime);
    emitCountDetector.initialize(conf, runtime);

    spoutScaleUpResolver.initialize(conf, runtime);
    detectorService = (DetectorService) Runtime
        .getDetectorService(runtime);
    resolverService = (ResolverService) Runtime
        .getResolverService(runtime);
  }

  public void setSpoutThroughput(double throughput) {
    this.maxThroughput = throughput;
  }

  @Override
  public void execute() {

    performedAction = false;
    Diagnosis<ComponentBottleneck> backPressureDiagnosis =
        detectorService.run(backPressureDetector, topology);
    System.out.println("Found backpressure");

    if (backPressureDiagnosis.getSummary().size() == 0) {
      System.out.println("No backpressure");
      Diagnosis<ComponentBottleneck> emitCountDiagnosis =
          detectorService.run(emitCountDetector, topology);
      List<TopologyAPI.Spout> spouts = topology.getSpoutsList();
      for (ComponentBottleneck component : emitCountDiagnosis.getSummary()) {
        int position = contains(spouts, component.getComponentName());
        if (position != -1) {
          Diagnosis<ComponentBottleneck> spoutDiagnosis = new Diagnosis<ComponentBottleneck>();
          spoutDiagnosis.addToDiagnosis(component);
          if (!resolverService.isBlackListedAction(topology, "SPOUT_SCALE_UP_RESOLVER",
              spoutDiagnosis, emitCountDetector)) {
            spoutScaleUpResolver.setMaxEMitCount(this.maxThroughput);
            double outcomeImprovement = resolverService.estimateResolverOutcome(
                spoutScaleUpResolver, topology, spoutDiagnosis);
            resolverService.run(spoutScaleUpResolver, topology, "SPOUT_SCALE_UP_RESOLVER",
                spoutDiagnosis, outcomeImprovement);
            performedAction = true;
          } else {
            System.out.println("Trying sth else");
          }
        }
      }
    }
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
  public void evaluate() {
    /*if(performedAction) {
      ActionEntry<? extends Bottleneck> lastAction = resolverService.getLog()
          .getLastAction(topology.getName());
      System.out.println("last action " + lastAction);
      evaluateAction(emitCountDetector, spoutScaleUpResolver, lastAction);
    }*/
  }

  /*@SuppressWarnings("unchecked")
  private <T extends Bottleneck> void evaluateAction(IDetector<T> detector, IResolver<T> resolver,
                                                     ActionEntry<? extends Bottleneck> lastAction) {
    Boolean success = true;
    Diagnosis<? extends Bottleneck> newDiagnosis;
    newDiagnosis = detectorService.run(detector, topology);
    List<TopologyAPI.Spout> spouts = topology.getSpoutsList();
    Diagnosis<ComponentBottleneck> spoutDiagnosis = new Diagnosis<>();

    for(ComponentBottleneck component : ((Diagnosis<ComponentBottleneck>) newDiagnosis).getSummary()){
      int position = contains(spouts, component.getComponentName());
      if(position != -1){
        spoutDiagnosis.addToDiagnosis(component);
      }
    }
    if (newDiagnosis != null) {
      success = resolverService.isSuccesfulAction(resolver,
          ((ActionEntry<T>) lastAction).getDiagnosis(), (Diagnosis<T>) spoutDiagnosis,
          ((ActionEntry<T>) lastAction).getChange());
      System.out.println("evaluating " + success);
      if (!success) {
        System.out.println("bad action");
        resolverService.addToBlackList(topology, lastAction.getAction(), lastAction.getDiagnosis(),
            ((ActionEntry<T>) lastAction).getChange());
      }
    }
  }*/

  @Override
  public void close() {
    backPressureDetector.close();
    emitCountDetector.close();
    spoutScaleUpResolver.close();
  }
}
