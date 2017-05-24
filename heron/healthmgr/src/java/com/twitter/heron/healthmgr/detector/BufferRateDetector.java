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

import java.util.*;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.healthmgr.services.DetectorService;
import com.twitter.heron.healthmgr.utils.CurveFitter;
import com.twitter.heron.healthmgr.utils.SLAManagerUtils;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.healthmgr.*;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsInfo;
import com.twitter.heron.spi.metricsmgr.sink.SinkVisitor;
import com.twitter.heron.spi.packing.InstanceId;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlanProtoDeserializer;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;

public class BufferRateDetector implements IDetector<ComponentBottleneck> {

  private static final Logger LOG = Logger.getLogger(LowPendingPacketsDetector.class.getName());
  private static final String AVG_PENDING_PACKETS = "__connection_buffer_by_intanceid";
  private SinkVisitor visitor;
  private Config runtime;
  private int packetThreshold = 0;
  private DetectorService detectorService;
  private long singleObservationLength = 600; // The duration of each observation interval in seconds.
  private long numSecondsBetweenObservations = 60;
  // TODO(harshad) - Verify if metricstimeline API accepts starttime and endtime values in seconds.

  @Override
  public void initialize(Config inputConfig, Config inputRuntime) {
    this.runtime = inputRuntime;
    this.visitor = Runtime.metricsReader(runtime);
    detectorService = (DetectorService) Runtime.getDetectorService(runtime);
  }

  @Override
  public Diagnosis<ComponentBottleneck> detect(TopologyAPI.Topology topology)
          throws RuntimeException {

    LOG.info("Executing: " + this.getClass().getName());
    PackingPlan packingPlan = getPackingPlan(topology, runtime);
    //HashMap<String, List<ComponentBottleneck>> resultsForAllIntervals = new HashMap<>();
    long endTime = System.currentTimeMillis() / 1000;

    // Key = component name. Value = list of ComponentBottlenecks, where each of the ComponentBottleneck represents
    // one time sub-interval.
    HashMap<String, List<ComponentBottleneck>> resultsForAllIntervals = SLAManagerUtils.retrieveMetricValuesForInterval(
            AVG_PENDING_PACKETS, "packets", "__stmgr__", this.visitor,
            endTime - (singleObservationLength),
            endTime, packingPlan);

    Set<ComponentBottleneck> trends = findTrends(resultsForAllIntervals);

    return new Diagnosis<>(trends);
  }

  public static PackingPlan getPackingPlan(TopologyAPI.Topology topology, Config runtime) {
    // TODO this could be optimized
    SchedulerStateManagerAdaptor adaptor = Runtime.schedulerStateManagerAdaptor(runtime);
    PackingPlans.PackingPlan protoPackingPlan = adaptor.getPackingPlan(topology.getName());
    PackingPlanProtoDeserializer deserializer = new PackingPlanProtoDeserializer();
    return deserializer.fromProto(protoPackingPlan);
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
                                  Diagnosis<ComponentBottleneck> secondDiagnosis) {
    return false;
  }

  private boolean isIncreasingSequence(List<Double> data) {
    return getIncreaseRate(data) > 0;
  }

  private Double getIncreaseRate(List<Double> data) {
    CurveFitter curveFitter = new CurveFitter();
    List<Double> xPoints = new ArrayList<>();
    for (int i = 0; i < data.size(); i++) {
      xPoints.add(new Double(i * numSecondsBetweenObservations));
    }
    curveFitter.linearCurveFit(xPoints, data);
    System.out.println(data);
    System.out.println(curveFitter);
    return curveFitter.getSlope();
  }

  private Set<ComponentBottleneck> findTrends(HashMap<String, List<ComponentBottleneck>> observations) {
    // For each instance, find its trend.
    /* TODO(harshad) To improve the performance of this method, refactor the InstanceBottleneck class
    so that it has a list of metric values.*/
    Set<ComponentBottleneck> result = new HashSet<>();
    Set<String> componentNames = observations.keySet();
    for (String currComponentName : componentNames) {
      // First construct a bottleneck class for the current component.
      ComponentBottleneck currComponentBottleneck = new ComponentBottleneck(currComponentName);
      List<ComponentBottleneck> currComponentInstances = observations.get(currComponentName);
      // We assume that all instances produce same number of observations.
      // To get the number of instances, we refer to the last observation.
      final int numInstances = currComponentInstances.get(currComponentInstances.size() - 1).getInstances().size();
      for (int instanceID = 0; instanceID < numInstances; instanceID++) {
        // Construct a sequence of Doubles for each instance.
        List<Double> instanceMetrics = new ArrayList<>();
        InstanceInfo currInstanceInfo = currComponentInstances.get(currComponentInstances.size() - 1).getInstances().get(instanceID).getInstanceData();
        final int currInstanceId = currInstanceInfo.getInstanceId();
        final int currInstanceContainerId = currInstanceInfo.getContainerId();
        for (int bottleneckID = 0; bottleneckID < currComponentInstances.size(); bottleneckID++) {
          // Get the metric for the given instance in the current bottleneck.
          // TODO(harshad) - Pass the metric name (right now hard coded to AVG_PENDING_PACKETS) to this function.
          instanceMetrics.add(currComponentInstances.get(bottleneckID).getDataPoints(AVG_PENDING_PACKETS)[instanceID]);
        }
        // Now get the rate of increase in buffered packets for this instance.
        Double bufferedPacketsIncreaseRate = getIncreaseRate(instanceMetrics);
        Set<MetricsInfo> currInstanceMetricsInfo = new HashSet<>();
        currInstanceMetricsInfo
                .add(new MetricsInfo(currComponentName + Integer.toString(currInstanceId),
                        bufferedPacketsIncreaseRate.toString()));
        // Create
        currComponentBottleneck
                .add(currInstanceContainerId,
                        new PackingPlan.
                          InstancePlan(new InstanceId(currComponentName, currInstanceId, 0), null),
                        currInstanceMetricsInfo);
      }
      result.add(currComponentBottleneck);
    }
    return result;
  }

  @Override
  public void close() {
  }
}