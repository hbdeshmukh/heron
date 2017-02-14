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

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.healthmgr.actionlog.ActionEntry;
import com.twitter.heron.healthmgr.detector.DataSkewDetector;
import com.twitter.heron.healthmgr.detector.LimitedParallelismDetector;
import com.twitter.heron.healthmgr.detector.SlowInstanceDetector;
import com.twitter.heron.healthmgr.resolver.ScaleUpResolver;
import com.twitter.heron.healthmgr.services.DetectorService;
import com.twitter.heron.healthmgr.services.ResolverService;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.healthmgr.Bottleneck;
import com.twitter.heron.spi.healthmgr.ComponentBottleneck;
import com.twitter.heron.spi.healthmgr.Diagnosis;
import com.twitter.heron.spi.healthmgr.HealthPolicy;
import com.twitter.heron.spi.healthmgr.IDetector;
import com.twitter.heron.spi.healthmgr.IResolver;


public class BackPressurePolicy implements HealthPolicy {

  private LimitedParallelismDetector limitedParallelismDetector = new LimitedParallelismDetector();
  private DataSkewDetector dataSkewDetector = new DataSkewDetector();
  private SlowInstanceDetector slowInstanceDetector = new SlowInstanceDetector();

  private ScaleUpResolver scaleUpResolver = new ScaleUpResolver();
  private TopologyAPI.Topology topology;

  private DetectorService detectorService;
  private ResolverService resolverService;

  @Override
  public void initialize(Config conf, Config runtime) {
    this.topology = Runtime.topology(runtime);

    limitedParallelismDetector.initialize(conf, runtime);
    dataSkewDetector.initialize(conf, runtime);
    slowInstanceDetector.initialize(conf, runtime);

    scaleUpResolver.initialize(conf, runtime);
    detectorService = (DetectorService) Runtime
        .getDetectorService(runtime);
    resolverService = (ResolverService) Runtime
        .getResolverService(runtime);
  }

  @Override
  public void execute() {

    Diagnosis<ComponentBottleneck> slowInstanceDiagnosis =
        detectorService.run(slowInstanceDetector, topology);

    if (slowInstanceDiagnosis != null) {
      if (!resolverService.isBlackListedAction(topology, "SLOW_INSTANCE_RESOLVER",
          slowInstanceDiagnosis, slowInstanceDetector)) {
      }
    }

    Diagnosis<ComponentBottleneck> dataSkewDiagnosis =
        detectorService.run(dataSkewDetector, topology);

    if (dataSkewDiagnosis != null) {
      if (!resolverService.isBlackListedAction(topology, "DATA_SKEW_RESOLVER",
          dataSkewDiagnosis, dataSkewDetector)) {

      }
    }

    Diagnosis<ComponentBottleneck> limitedParallelismDiagnosis =
        detectorService.run(limitedParallelismDetector, topology);

    if (limitedParallelismDiagnosis != null) {
      if (!resolverService.isBlackListedAction(topology, "SCALE_UP_RESOLVER",
          limitedParallelismDiagnosis, limitedParallelismDetector)) {
        double outcomeImprovement = resolverService.estimateResolverOutcome(scaleUpResolver,
            topology, limitedParallelismDiagnosis);
        resolverService.run(scaleUpResolver, topology, "SCALE_UP_RESOLVER",
            limitedParallelismDiagnosis, outcomeImprovement);
      }
    }
  }

  @Override
  public void evaluate() {
    ActionEntry<? extends Bottleneck> lastAction = resolverService.getLog()
        .getLastAction(topology.getName());
    System.out.println("last action " + lastAction);
    switch (lastAction.getAction()) {
      case "DATA_SKEW_RESOLVER": {
        evaluateAction(dataSkewDetector, null, lastAction);
        break;
      }
      case "SLOW_INSTANCE_RESOLVER":
        evaluateAction(slowInstanceDetector, null, lastAction);
        break;
      case "SCALE_UP_RESOLVER":
        evaluateAction(limitedParallelismDetector, scaleUpResolver, lastAction);
        break;
      default:
        break;
    }
  }

  @SuppressWarnings("unchecked")
  private <T extends Bottleneck> void evaluateAction(IDetector<T> detector, IResolver<T> resolver,
                                                     ActionEntry<? extends Bottleneck> lastAction) {
    Boolean success = false;
    Diagnosis<? extends Bottleneck> newDiagnosis;
    newDiagnosis = detectorService.run(detector, topology);
    if (newDiagnosis != null) {
      success = resolverService.isSuccesfulAction(resolver,
          ((ActionEntry<T>) lastAction).getDiagnosis(), (Diagnosis<T>) newDiagnosis,
          ((ActionEntry<T>) lastAction).getChange());
      System.out.println("evaluating" + success);
      if (!success) {
        System.out.println("bad action");
        resolverService.addToBlackList(topology, lastAction.getAction(), lastAction.getDiagnosis(),
            ((ActionEntry<T>) lastAction).getChange());
      }
    }
  }

  @Override
  public void close() {
    limitedParallelismDetector.close();
    dataSkewDetector.close();
    slowInstanceDetector.close();
    scaleUpResolver.close();
  }
}
