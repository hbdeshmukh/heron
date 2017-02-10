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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.healthmgr.clustering.DiscreteValueClustering;
import com.twitter.heron.healthmgr.detector.BackPressureDetector;
import com.twitter.heron.healthmgr.detector.ReportingDetector;
import com.twitter.heron.healthmgr.resolver.ScaleUpResolver;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.healthmgr.ComponentBottleneck;
import com.twitter.heron.spi.healthmgr.Diagnosis;
import com.twitter.heron.spi.healthmgr.HealthPolicy;
import com.twitter.heron.spi.healthmgr.InstanceBottleneck;
import com.twitter.heron.spi.healthmgr.utils.BottleneckUtils;


public class BackPressurePolicy implements HealthPolicy {
  private static final String BACKPRESSURE_METRIC = "__time_spent_back_pressure_by_compid";
  private static final String EXECUTION_COUNT_METRIC = "__execute-count/default";

  private BackPressureDetector backpressureDetector = new BackPressureDetector();
  private ReportingDetector executeCountDetector = new ReportingDetector(EXECUTION_COUNT_METRIC);
  private ScaleUpResolver scaleUpResolver = new ScaleUpResolver();
  private TopologyAPI.Topology topology;

  //private DetectorService detectorService;
  //private ResolverService resolverService;

  @Override
  public void initialize(Config conf, Config runtime) {
    this.topology = Runtime.topology(runtime);
    backpressureDetector.initialize(conf, runtime);
    executeCountDetector.initialize(conf, runtime);
    scaleUpResolver.initialize(conf, runtime);
    //detectorService = (DetectorService) Runtime
    //    .schedulerClientInstance(runtime);
    // resolverService = (ResolverService) Runtime
    //.schedulerClientInstance(runtime);
  }

  @Override
  public void execute() {
    // Diagnosis<ComponentBottleneck> backPressuredDiagnosis =
    //  detectorService.run(backpressureDetector, topology);
    Diagnosis<ComponentBottleneck> backPressuredDiagnosis = backpressureDetector.detect(topology);
    Diagnosis<ComponentBottleneck> executeCountDiagnosis = executeCountDetector.detect(topology);
    //Diagnosis<ComponentBottleneck> executeCountDiagnosis =
    // detectorService.run(executeCountDetector, topology);

    if (backPressuredDiagnosis != null && executeCountDiagnosis != null) {
      Set<ComponentBottleneck> backPressureSummary = backPressuredDiagnosis.getSummary();
      Set<ComponentBottleneck> executeCountSummary = executeCountDiagnosis.getSummary();

      if (backPressureSummary.size() != 0 && executeCountSummary.size() != 0) {
        BottleneckUtils.merge(backPressureSummary, executeCountSummary);
        
        ComponentBottleneck current = backPressureSummary.iterator().next();
        Problem problem = identifyProblem(current);
        if (problem == Problem.LIMITED_PARALLELISM) {
          double scaleFactor = computeScaleUpFactor(current);
          int newParallelism = (int) Math.ceil(current.getInstances().size() * scaleFactor);
          System.out.println("scale factor " + scaleFactor
              + " new parallelism " + newParallelism);

          Diagnosis<ComponentBottleneck> currentDiagnosis = new Diagnosis<>();
          currentDiagnosis.addToDiagnosis(current);
          newParallelism = (int) Math.ceil(current.getInstances().size() * scaleFactor);
          scaleUpResolver.setParallelism(newParallelism);
          scaleUpResolver.resolve(currentDiagnosis, topology);
          //resolverService.run(scaleUpResolver, topology,
          //  "LIMITED_PARALLELISM", currentDiagnosis);
        }
      }
    }
  }

  private double computeScaleUpFactor(ComponentBottleneck current) {

    double executeCountSum = BottleneckUtils.computeSum(current, EXECUTION_COUNT_METRIC);

    double maxSum = 0;
    for (int i = 0; i < current.getInstances().size(); i++) {
      double backpressureTime = Double.parseDouble(
          current.getInstances().get(i).getDataPoint(BACKPRESSURE_METRIC));
      double executeCount = Double.parseDouble(
          current.getInstances().get(i).getDataPoint(EXECUTION_COUNT_METRIC));
      maxSum += (1 + backpressureTime / 60000) * executeCount;
      System.out.println(i + " backpressure " + backpressureTime + " execute: " + executeCount
          + " " + (1 + backpressureTime / 60000) * executeCount + " " + maxSum);
    }
    return maxSum / executeCountSum;
  }

  private Problem identifyProblem(ComponentBottleneck current) {
    Double[] backPressureDataPoints = current.getDataPoints(BACKPRESSURE_METRIC);
    DiscreteValueClustering clustering = new DiscreteValueClustering();
    HashMap<String, ArrayList<Integer>> backPressureClusters =
        clustering.createBinaryClusters(backPressureDataPoints, 0.0);

    int clusterAt0 = backPressureClusters.get("0.0") == null
        ? 0 : backPressureClusters.get("0.0").size();
    int clusterAt1 = backPressureClusters.get("1.0") == null
        ? 0 : backPressureClusters.get("1.0").size();

    if (clusterAt1 < (10 * clusterAt0) / 100) {
      switch (compareExecuteCounts(current)) {
        case 0:
          return Problem.LIMITED_PARALLELISM;
        case -1:
          return Problem.SLOW_INSTANCE;
        case 1:
          return Problem.DATA_SKEW;
        default:
          return Problem.OTHER;
      }
    } else {
      return Problem.LIMITED_PARALLELISM;
    }
  }

  private int compareExecuteCounts(ComponentBottleneck bottleneck) {

    double backPressureExecuteCounts = 0;
    double nonBackPressureExecuteCounts = 0;
    int noBackPressureInstances = 0;
    for (int j = 0; j < bottleneck.getInstances().size(); j++) {
      InstanceBottleneck currentInstance = bottleneck.getInstances().get(j);
      if (!currentInstance.getInstanceData().getMetricValue(BACKPRESSURE_METRIC).equals("0.0")) {
        backPressureExecuteCounts += Double.parseDouble(
            currentInstance.getInstanceData().getMetricValue(EXECUTION_COUNT_METRIC));
        noBackPressureInstances++;
      } else {
        nonBackPressureExecuteCounts += Double.parseDouble(
            currentInstance.getInstanceData().getMetricValue(EXECUTION_COUNT_METRIC));
      }
    }
    int noNonBackPressureInstances = bottleneck.getInstances().size() - noBackPressureInstances;
    if (backPressureExecuteCounts / noBackPressureInstances > 2 * (
        nonBackPressureExecuteCounts / noNonBackPressureInstances)) {
      return 1;
    } else {
      if (backPressureExecuteCounts / noBackPressureInstances < 0.5 * (
          nonBackPressureExecuteCounts / noNonBackPressureInstances)) {
        return -1;
      } else {
        return 0;
      }
    }
  }

  @Override
  public void close() {
    backpressureDetector.close();
    executeCountDetector.close();
    scaleUpResolver.close();
  }

  enum Problem {
    SLOW_INSTANCE, DATA_SKEW, LIMITED_PARALLELISM, OTHER
  }
}
