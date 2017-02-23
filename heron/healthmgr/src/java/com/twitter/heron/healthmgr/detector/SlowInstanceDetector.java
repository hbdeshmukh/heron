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

import java.util.HashSet;
import java.util.Set;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.healthmgr.services.DetectorService;
import com.twitter.heron.healthmgr.utils.SLAManagerUtils;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.healthmgr.ComponentBottleneck;
import com.twitter.heron.spi.healthmgr.Diagnosis;
import com.twitter.heron.spi.healthmgr.IDetector;
import com.twitter.heron.spi.healthmgr.utils.BottleneckUtils;
import com.twitter.heron.spi.metricsmgr.sink.SinkVisitor;
import com.twitter.heron.spi.packing.PackingPlan;

public class SlowInstanceDetector implements IDetector<ComponentBottleneck> {
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
    SinkVisitor visitor = Runtime.metricsReader(runtime);
    PackingPlan packingPlan = BackPressureDetector.getPackingPlan(topology, runtime);

    Diagnosis<ComponentBottleneck> backPressuredDiagnosis =
        detectorService.run(backpressureDetector, topology);

    Diagnosis<ComponentBottleneck> executeCountDiagnosis =
        detectorService.run(executeCountDetector, topology);

    Set<ComponentBottleneck> backPressureSummary = backPressuredDiagnosis.getSummary();
    Set<ComponentBottleneck> executeCountSummary = executeCountDiagnosis.getSummary();
    Set<ComponentBottleneck> pendingBufferPacket = new HashSet<>();
    pendingBufferPacket.addAll(SLAManagerUtils.retrieveMetricValues(
        DataSkewDetector.AVG_PENDING_PACKETS, "packets", "__stmgr__", visitor, packingPlan)
        .values());

    if (backPressureSummary.size() > 0
        && executeCountSummary.size() > 0
        && pendingBufferPacket.size() > 0) {
      BottleneckUtils.merge(backPressureSummary, executeCountSummary);
      BottleneckUtils.merge(backPressureSummary, pendingBufferPacket);

      ComponentBottleneck current = backPressureSummary.iterator().next();
      if (existSlowInstances(current)) {
        Diagnosis<ComponentBottleneck> currentDiagnosis = new Diagnosis<>();
        currentDiagnosis.addToDiagnosis(current);
        return currentDiagnosis;
      }
    }
    return null;
  }

  @Override
  public boolean similarDiagnosis(Diagnosis<ComponentBottleneck> firstDiagnosis,
                                  Diagnosis<ComponentBottleneck> secondDiagnosis){
    return true;
  }

  @Override
  public void close() {
    backpressureDetector.close();
    executeCountDetector.close();
  }

  private boolean existSlowInstances(ComponentBottleneck current) {
    return DataSkewDetector.compareExecuteCounts(current) == -1;
  }
}
