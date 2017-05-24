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
import com.twitter.heron.spi.healthmgr.Bottleneck;
import com.twitter.heron.spi.healthmgr.ComponentBottleneck;
import com.twitter.heron.spi.healthmgr.Diagnosis;
import com.twitter.heron.spi.healthmgr.IDetector;
import com.twitter.heron.spi.metricsmgr.sink.SinkVisitor;
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
  // TODO(harshad) - Verify if metricstimeline API accepts starttime and endtime values in seconds.

  @Override
  public void initialize(Config inputConfig, Config inputRuntime) {
    this.runtime = inputRuntime;
    this.visitor = Runtime.metricsReader(runtime);
    //this.bufferRateDetector.initialize(inputConfig, runtime);
    detectorService = (DetectorService) Runtime.getDetectorService(runtime);
        /*this.packetThreshold =
                Integer.valueOf(inputConfig.getStringValue("health.policy.scaleup.high.packet.limit"));

        LOG.info("Scale up detector's high packet limit is " + packetThreshold);*/
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

    List<Boolean> trends = findTrends(resultsForAllIntervals, "split");

    Set<ComponentBottleneck> bottlenecks = new HashSet<>();
    if (trends.contains(Boolean.TRUE)) {
      // There's at least one increasing trend.
      List<ComponentBottleneck> allBottlenecks = resultsForAllIntervals.values().iterator().next();
      // Return the latest bottleneck.
      bottlenecks.add(allBottlenecks.get(allBottlenecks.size() - 1));
    }

    // TODO(harshad) - Make sure that the bottlenecks indeed belong to bolts.
    if (bottlenecks.isEmpty()) {
      return null;
    } else {
      return new Diagnosis<>(bottlenecks);
    }
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
    CurveFitter curveFitter = new CurveFitter();
    List<Double> xPoints = new ArrayList<>();
    for (int i = 0; i < data.size(); i++) {
      xPoints.add(new Double(i * 60));
    }
    curveFitter.linearCurveFit(xPoints, data);
    System.out.println(data);
    System.out.println(curveFitter);
    return curveFitter.getSlope() > 0;
  }

  private List<Boolean> findTrends(HashMap<String, List<ComponentBottleneck>> observations, String userComponent) {
    // For each instance, find its trend.
    // Note - this is not efficient. To improve the performance of this method, refactor the InstanceBottleneck class
    // so that it has a list of metric values.
    List<Boolean> result = new ArrayList<>();
    // We assume only one key in the above set.
    assert observations.containsKey(userComponent);
    List<ComponentBottleneck> values = observations.get(userComponent);
    // Find the number of instances. This is a big assumptions that number of observations is the same as number of instances.
    final int numInstances = values.get(values.size() - 1).getInstances().size();
    for (int instanceID = 0; instanceID < numInstances; instanceID++) {
      // Construct a sequence of Doubles for each instance.
      List<Double> instanceMetrics = new ArrayList<>();
      for (int bottleneckID = 0; bottleneckID < values.size(); bottleneckID++) {
        // Get the metric for the give instance in the current bottleneck.
        // TODO(harshad) - Pass the metric name (right now hard coded to AVG_PENDING_PACKETS) to this function.
        instanceMetrics.add(values.get(bottleneckID).getDataPoints(AVG_PENDING_PACKETS)[instanceID]);
      }
      result.add(isIncreasingSequence(instanceMetrics));
    }
    return result;
  }

  @Override
  public void close() {
  }
}