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


import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.healthmgr.services.DetectorService;
import com.twitter.heron.healthmgr.utils.SLAManagerUtils;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.healthmgr.ComponentBottleneck;
import com.twitter.heron.spi.healthmgr.Diagnosis;
import com.twitter.heron.spi.healthmgr.IDetector;
import com.twitter.heron.spi.healthmgr.InstanceBottleneck;
import com.twitter.heron.spi.healthmgr.InstanceInfo;
import com.twitter.heron.spi.healthmgr.utils.BottleneckUtils;

public class DataSkewDetector implements IDetector<ComponentBottleneck> {
  private static final Logger LOG = Logger.getLogger(DataSkewDetector.class.getName());

  private static final String BACKPRESSURE_METRIC = "__time_spent_back_pressure_by_compid";
  private static final String EXECUTION_COUNT_METRIC = "__execute-count/default";

  private Config runtime;

  private BackPressureDetector backpressureDetector = new BackPressureDetector();
  private ReportingDetector executeCountDetector = new ReportingDetector(EXECUTION_COUNT_METRIC);

  private DetectorService detectorService;

  @Override
  public void initialize(Config config, Config inputRuntime) {
    this.runtime = inputRuntime;
    this.backpressureDetector.initialize(config, inputRuntime);
    this.executeCountDetector.initialize(config, inputRuntime);
    detectorService = (DetectorService) Runtime
        .getDetectorService(runtime);
  }

  @Override
  public Diagnosis<ComponentBottleneck> detect(TopologyAPI.Topology topology) {
    Diagnosis<ComponentBottleneck> backPressuredDiagnosis =
        detectorService.run(backpressureDetector, topology);

    Diagnosis<ComponentBottleneck> executeCountDiagnosis =
        detectorService.run(executeCountDetector, topology);

    Set<ComponentBottleneck> backPressureSummary = backPressuredDiagnosis.getSummary();
    Set<ComponentBottleneck> executeCountSummary = executeCountDiagnosis.getSummary();

    if (backPressureSummary.size() != 0 && executeCountSummary.size() != 0) {
      BottleneckUtils.merge(backPressureSummary, executeCountSummary);

      ComponentBottleneck current = backPressureSummary.iterator().next();
      if (existsDataSkew(current)) {
        Diagnosis<ComponentBottleneck> currentDiagnosis = new Diagnosis<>();
        currentDiagnosis.addToDiagnosis(current);
        return currentDiagnosis;
      }
    }
    return null;
  }


  @Override
  public boolean similarDiagnosis(Diagnosis<ComponentBottleneck> firstDiagnosis,
                                  Diagnosis<ComponentBottleneck> secondDiagnosis) {

    Set<ComponentBottleneck> firstSummary = firstDiagnosis.getSummary();
    Set<ComponentBottleneck> secondSummary = secondDiagnosis.getSummary();
    ComponentBottleneck first = firstSummary.iterator().next();
    ComponentBottleneck second = secondSummary.iterator().next();
    if (!first.getComponentName().equals(second.getComponentName())
        || !SLAManagerUtils.sameInstanceIds(first, second)) {
      return false;
    } else {
      if (!SLAManagerUtils.similarBackPressure(first, second)) {
        return false;
      }
      if (!SLAManagerUtils.similarMetric(first, second, EXECUTION_COUNT_METRIC, 2)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void close() {
    backpressureDetector.close();
    executeCountDetector.close();
  }

  private boolean existsDataSkew(ComponentBottleneck current) {
    return compareExecuteCounts(current) == 1;
  }

  static int compareExecuteCounts(ComponentBottleneck bottleneck) {
    double sumBPExecuteCounts = 0;
    double sumNonBPExecuteCounts = 0;
    int bpInstanceCount = 0;

    final int totalInstances = bottleneck.getInstances().size();

    for (int j = 0; j < totalInstances; j++) {
      InstanceInfo instanceData = bottleneck.getInstances().get(j).getInstanceData();
      double executionCount =
          Double.parseDouble(instanceData.getMetricValue(EXECUTION_COUNT_METRIC));
      LOG.log(Level.INFO, "Instance: {0}, executeCount: {1}",
          new Object[]{instanceData.getInstanceNameId(), executionCount});
      if (instanceData.getMetricValue(BACKPRESSURE_METRIC).equals("0.0")) {
        sumNonBPExecuteCounts += executionCount;
      } else {
        sumBPExecuteCounts += executionCount;
        bpInstanceCount++;
      }
    }

    double avgBPExecuteCount = sumBPExecuteCounts / bpInstanceCount;
    double avgNonBPExecuteCount = sumNonBPExecuteCounts / (totalInstances - bpInstanceCount);

    if (bpInstanceCount < totalInstances && avgBPExecuteCount > 2 * avgNonBPExecuteCount) {
      return 1; // data skew
    } else if (bpInstanceCount < totalInstances && avgBPExecuteCount < 0.5 * avgNonBPExecuteCount) {
      return -1; // slow instance
    } else {
      return 0;
    }
  }
}
