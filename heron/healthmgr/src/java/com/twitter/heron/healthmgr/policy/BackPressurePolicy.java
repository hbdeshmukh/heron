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
import com.twitter.heron.healthmgr.detector.BackPressureDetector;
import com.twitter.heron.healthmgr.detector.outlierdetection.SimpleMADOutlierDetector;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.healthmgr.TopologyGraph;
import com.twitter.heron.healthmgr.resolver.ScaleUpResolver;
import com.twitter.heron.healthmgr.clustering.DiscreteValueClustering;
import com.twitter.heron.healthmgr.detector.ReportingDetector;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.healthmgr.ComponentBottleneck;
import com.twitter.heron.spi.healthmgr.Diagnosis;
import com.twitter.heron.spi.healthmgr.SLAPolicy;
import com.twitter.heron.spi.healthmgr.utils.BottleneckUtils;

public class BackPressurePolicy implements SLAPolicy {

  private final String BACKPRESSURE_METRIC = "__time_spent_back_pressure_by_compid";
  private final String EXECUTION_COUNT_METRIC = "__execute-count/default";

  private BackPressureDetector backpressureDetector = new BackPressureDetector();
  private ReportingDetector executeCountDetector = new ReportingDetector(EXECUTION_COUNT_METRIC);
  private ScaleUpResolver scaleUpResolver = new ScaleUpResolver();

  private TopologyAPI.Topology topology;
  private ArrayList<String> topologySort = null;


  @Override
  public void initialize(Config conf, Config runtime) {
    this.topology = Runtime.topology(runtime);
    backpressureDetector.initialize(conf, runtime);
    executeCountDetector.initialize(conf, runtime);
    scaleUpResolver.initialize(conf, runtime);
  }

  @Override
  public void execute() {
    Diagnosis<ComponentBottleneck> backPressuredDiagnosis = backpressureDetector.detect(topology);
    Diagnosis<ComponentBottleneck> executeCountDiagnosis = executeCountDetector.detect(topology);
    boolean found = false;

    if (backPressuredDiagnosis != null && executeCountDiagnosis != null) {

      Set<ComponentBottleneck> backPressureSummary = backPressuredDiagnosis.getSummary();
      Set<ComponentBottleneck> executeCountSummary = executeCountDiagnosis.getSummary();

      if (backPressureSummary.size() != 0 && executeCountSummary.size() != 0) {
        BottleneckUtils.merge(backPressureSummary, executeCountSummary);

        if (topologySort == null) {
          topologySort = getTopologySort(topology);
        }
        for (int i = 0; i < topologySort.size() && !found; i++) {
          String name = topologySort.get(i);
          ComponentBottleneck current = BottleneckUtils.getComponentBottleneck(backPressureSummary, name);
          if (current != null) {
            System.out.println("Bottleneck " + name);
            Problem problem = identifyProblem(current);
            //check is need to scaleUp
            boolean scaleUp = needScaleUp(current, 30);
            System.out.println("LLLL " + scaleUp);
            if (false) {
              Diagnosis<ComponentBottleneck> currentDiagnosis = new Diagnosis<>();
              currentDiagnosis.addToDiagnosis(current);
              scaleUpResolver.resolve(currentDiagnosis, topology);
              found = true;
            }
          }
        }
      }
    }
  }

  private Problem identifyProblem(ComponentBottleneck current) {
    Double[] backPressureDataPoints = current.getDataPoints(BACKPRESSURE_METRIC);
    DiscreteValueClustering clustering = new DiscreteValueClustering();
    HashMap<Double, ArrayList<Integer>> backPressureClusters =
        clustering.createBinaryClusters(backPressureDataPoints, 0.0);
    System.out.println("BackPressureOutliers" + backPressureClusters.toString());

    Double[] executionCountDataPoints = current.getDataPoints(EXECUTION_COUNT_METRIC);
    SimpleMADOutlierDetector executeCountOutlierDetector = new SimpleMADOutlierDetector(1.0);
    ArrayList<Integer> executeCountOutliers =
        executeCountOutlierDetector.detectOutliers(executionCountDataPoints);
    System.out.println("ExecuteCountOutliers" + executeCountOutliers.toString());

    return Problem.LIMITED_PARALLELISM;
  }

  private boolean needScaleUp(ComponentBottleneck current, int threshold) {
    Double[] dataPoints = current.getDataPoints(BACKPRESSURE_METRIC);
    SimpleMADOutlierDetector outlierDetector = new SimpleMADOutlierDetector(1.0);
    ArrayList<Integer> outliers = outlierDetector.detectOutliers(dataPoints);
    System.out.println("Outliers" + outliers.toString());
    if (outliers.size() * 100 < threshold * dataPoints.length) {
      return true;
    }
    return false;
  }

  @Override
  public void close() {
    backpressureDetector.close();
    executeCountDetector.close();
    scaleUpResolver.close();
  }

  private ArrayList<String> getTopologySort(TopologyAPI.Topology topology) {
    TopologyGraph topologyGraph = new TopologyGraph();
    for (TopologyAPI.Bolt.Builder bolt : topology.toBuilder().getBoltsBuilderList()) {
      String boltName = bolt.getComp().getName();

      // To get the parent's component to construct a graph of topology structure
      for (TopologyAPI.InputStream inputStream : bolt.getInputsList()) {
        String parent = inputStream.getStream().getComponentName();
        topologyGraph.addEdge(parent, boltName);
      }
    }
    return topologyGraph.topologicalSort();
  }

  private enum Problem {
    SLOW_HOST, DATA_SKEW, LIMITED_PARALLELISM
  }
}
