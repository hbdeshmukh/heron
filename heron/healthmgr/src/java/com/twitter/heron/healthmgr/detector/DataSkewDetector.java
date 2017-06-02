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
import com.twitter.heron.spi.metricsmgr.sink.SinkVisitor;
import com.twitter.heron.spi.packing.PackingPlan;

public class DataSkewDetector implements IDetector<ComponentBottleneck> {
  private static final Logger LOG = Logger.getLogger(DataSkewDetector.class.getName());

  public static final String AVG_PENDING_PACKETS = "__connection_buffer_by_intanceid";
  public static final String AVG_PENDING_BYTES = "__connection_buffer_by_intanceid";
  private static final String BACKPRESSURE_METRIC = "__time_spent_back_pressure_by_compid";
  private static final String EXECUTION_COUNT_METRIC = "__execute-count/default";

  private Config runtime;

  private BackPressureDetector backpressureDetector = new BackPressureDetector();
  private ReportingDetector executeCountDetector = new ReportingDetector(EXECUTION_COUNT_METRIC);
  private BufferRateDetector bufferRateDetector = new BufferRateDetector();

  @Override
  public void initialize(Config config, Config inputRuntime) {
    this.runtime = inputRuntime;
    this.backpressureDetector.initialize(config, inputRuntime);
    this.executeCountDetector.initialize(config, inputRuntime);
    this.bufferRateDetector.initialize(config, inputRuntime);
  }

  @Override
  public Diagnosis<ComponentBottleneck> detect(TopologyAPI.Topology topology) {
    return commonDiagnosis(runtime, topology, backpressureDetector, executeCountDetector, bufferRateDetector, 1);
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

  static Diagnosis<ComponentBottleneck> commonDiagnosis(Config runtime,
                                                        TopologyAPI.Topology topology,
                                                        BackPressureDetector backpressureDetector,
                                                        ReportingDetector executeCountDetector,
                                                        BufferRateDetector bufferRateDetector,
                                                        int expected) {
    DetectorService detectorService = (DetectorService) Runtime.getDetectorService(runtime);
    SinkVisitor visitor = Runtime.metricsReader(runtime);
    PackingPlan packingPlan = BackPressureDetector.getPackingPlan(topology, runtime);

    Diagnosis<ComponentBottleneck> backPressuredDiagnosis =
        detectorService.run(backpressureDetector, topology);
    Set<ComponentBottleneck> backPressureSummary = backPressuredDiagnosis.getSummary();
    if (backPressureSummary.size() > 0) {
      Diagnosis<ComponentBottleneck> executeCountDiagnosis =
              detectorService.run(executeCountDetector, topology);
      Set<ComponentBottleneck> executeCountSummary = executeCountDiagnosis.getSummary();

      Set<ComponentBottleneck> pendingBufferPacket = new HashSet<>();
      pendingBufferPacket.addAll(SLAManagerUtils.retrieveMetricValues(
              DataSkewDetector.AVG_PENDING_PACKETS, "packets", "__stmgr__", visitor, packingPlan)
              .values());

      BottleneckUtils.merge(backPressureSummary, executeCountSummary);
      BottleneckUtils.merge(backPressureSummary, pendingBufferPacket);

      ComponentBottleneck current = backPressureSummary.iterator().next();
      if (compareExecuteCounts(current) == expected) {
        Diagnosis<ComponentBottleneck> currentDiagnosis = new Diagnosis<>();
        currentDiagnosis.addToDiagnosis(current);
        LOG.info("Reactive limited scale down diagnosed");
        LOG.info(current.toString());
        return currentDiagnosis;
      }
    } else {
      LOG.info(" ************************ No backpressure found **************");
      Diagnosis<ComponentBottleneck> bufferRateDiagnosis =
              detectorService.run(bufferRateDetector, topology);
      Set<ComponentBottleneck> bufferRateSummary = bufferRateDiagnosis.getSummary();
      if (bufferRateSummary.size() == 0) {
        return null;
      }

      Diagnosis<ComponentBottleneck> executeCountDiagnosis =
              detectorService.run(executeCountDetector, topology);
      Set<ComponentBottleneck> executeCountSummary = executeCountDiagnosis.getSummary();

      BottleneckUtils.merge(bufferRateSummary, executeCountSummary);
      Diagnosis<ComponentBottleneck> currentDiagnosis = new Diagnosis<>();
      for (ComponentBottleneck componentBottleneck : bufferRateSummary) {
        currentDiagnosis.addToDiagnosis(componentBottleneck);
      }
      return currentDiagnosis;
    }
    return null;
  }

  static int compareExecuteCounts(ComponentBottleneck bottleneck) {
    // EC and ex below stand for execution count
    double exMax = Double.MIN_VALUE;
    double exMin = Double.MAX_VALUE;
    for (InstanceBottleneck instance : bottleneck.getInstances()) {
      double executionCount =
          Double.parseDouble(instance.getInstanceData().getMetricValue(EXECUTION_COUNT_METRIC));
      exMax = exMax < executionCount ? executionCount : exMax;
      exMin = exMin > executionCount ? executionCount : exMin;
    }

    LOG.info("execute count max:" + exMax + " min:" + exMin);

    if (exMax > 1.5 * exMin) {
      // there is wide gap between max and min executionCount, potential skew
      for (InstanceBottleneck instance : bottleneck.getInstances()) {
        InstanceInfo data = instance.getInstanceData();
        double executionCount = Double.parseDouble(data.getMetricValue(EXECUTION_COUNT_METRIC));
        if (exMax < executionCount * 1.25) {
          double bpValue = Double.parseDouble(data.getMetricValue(BACKPRESSURE_METRIC));
          if (bpValue > 0) {
            LOG.info(String.format("SKEW: %s:%d back-pressure(%f) and high execution count: %f",
                data.getInstanceNameId(), data.getInstanceId(), bpValue, executionCount));
            return 1; // skew
          }
        }
      }
    }

    double bufferMax = Double.MIN_VALUE;
    double bufferMin = Double.MAX_VALUE;
    for (InstanceBottleneck instance : bottleneck.getInstances()) {
      double bufferSize =
          Double.parseDouble(instance.getInstanceData().getMetricValue(AVG_PENDING_PACKETS));
      bufferMax = bufferMax < bufferSize ? bufferSize : bufferMax;
      bufferMin = bufferMin > bufferSize ? bufferSize : bufferMin;
    }

    LOG.info("bufferMax:" + bufferMax + " bufferMin:" + bufferMin);

    if (bufferMax > 25 * bufferMin) {
      // there is wide gap between max and min bufferSize, potential slow instance
      for (InstanceBottleneck instance : bottleneck.getInstances()) {
        InstanceInfo data = instance.getInstanceData();
        double bufferSize = Double.parseDouble(data.getMetricValue(AVG_PENDING_PACKETS));
        if (bufferMax < bufferSize * 25) {
          double bpValue = Double.parseDouble(data.getMetricValue(BACKPRESSURE_METRIC));
          if (bpValue > 0) {
            LOG.info(String.format("SLOW: %s:%d back-pressure(%f) and high buffer size: %f",
                data.getInstanceNameId(), data.getInstanceId(), bpValue, bufferSize));
            return -1; // slow instance
          }
        }
      }
    }

    LOG.info(String.format("SCALE: Limited parallelism issue"));
    return 0;
  }
}
