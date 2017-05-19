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

public class BufferRateDetector implements IDetector<ComponentBottleneck> {

    private static final Logger LOG = Logger.getLogger(LowPendingPacketsDetector.class.getName());
    private static final String AVG_PENDING_PACKETS = "__connection_buffer_by_intanceid";
    private SinkVisitor visitor;
    private Config runtime;
    private int packetThreshold = 0;
    private BackPressureDetector backpressureDetector = new BackPressureDetector();
    private DetectorService detectorService;
    private long numPastObservations = 3;  // The number of past observations we will use in discovering a trend.
    private long singleObservationLength = 2000; // The duration of each observation interval in seconds.
    // TODO(harshad) - Verify if metricstimeline API accepts starttime and endtime values in seconds.

    @Override
    public void initialize(Config inputConfig, Config inputRuntime) {
        this.runtime = inputRuntime;
        this.visitor = Runtime.metricsReader(runtime);
        this.backpressureDetector.initialize(inputConfig, runtime);
        detectorService = (DetectorService) Runtime.getDetectorService(runtime);
        this.packetThreshold =
                Integer.valueOf(inputConfig.getStringValue("health.policy.scaleup.high.packet.limit"));

        LOG.info("Scale up detector's high packet limit is " + packetThreshold);
    }

    @Override
    public Diagnosis<ComponentBottleneck> detect(TopologyAPI.Topology topology)
            throws RuntimeException {

        LOG.info("Executing: " + this.getClass().getName());
        PackingPlan packingPlan = BackPressureDetector.getPackingPlan(topology, runtime);
        HashMap<String, List<ComponentBottleneck>> resultsForAllIntervals = new HashMap<>();
        long endTime = System.currentTimeMillis()/1000;

        for (int observationCount = 0; observationCount < numPastObservations; observationCount++) {
            HashMap<String, ComponentBottleneck> currResults = SLAManagerUtils.retrieveMetricValuesForInterval(
                    AVG_PENDING_PACKETS, "packets", "__stmgr__", this.visitor,
                    endTime - ((observationCount + 1) * singleObservationLength),
                    endTime - (observationCount * singleObservationLength), packingPlan);
            mergeHashMaps(currResults, resultsForAllIntervals);
        }

        Set<ComponentBottleneck> bottlenecks = new HashSet<ComponentBottleneck>();
        for (List<ComponentBottleneck> listBottleNecks : resultsForAllIntervals.values()) {
            if (isDescendingSequence(listBottleNecks)) {
                // Return the most recent bottleneck.
                bottlenecks.add(listBottleNecks.get(0));
            }
        }

        // TODO(harshad) - Make sure that the bottlenecks indeed belong to bolts.
        if (bottlenecks.isEmpty()) {
            return null;
        } else {
            return new Diagnosis<ComponentBottleneck>(bottlenecks);
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
    public boolean similarDiagnosis(Diagnosis<ComponentBottleneck> firstDiagnosis,
                                    Diagnosis<ComponentBottleneck> secondDiagnosis){
        return false;
    }

    /**
     * Merge a source hash map to a destination hash map. The destination hash map's value is a list.
     * @param sourceMap
     * @param destinationMap
     */
    private void mergeHashMaps(HashMap<String, ComponentBottleneck> sourceMap, HashMap<String, List<ComponentBottleneck>> destinationMap) {
        for (HashMap.Entry<String, ComponentBottleneck> entry : sourceMap.entrySet()) {
            if (destinationMap.containsKey(entry.getKey())) {
                // Key already present, append to the value list.
                destinationMap.get(entry.getKey()).add(entry.getValue());
            } else {
                // Key not present, create a list first and then insert to the map.
                List<ComponentBottleneck> valueList = new ArrayList<>();
                valueList.add(entry.getValue());
                destinationMap.put(entry.getKey(), valueList);
            }
        }
    }

    private boolean isDescendingSequence(List<ComponentBottleneck> bottlenecks) {
        for (int i = 0; i < bottlenecks.size() - 1; i++) {
            Double[] dataPoints1 = bottlenecks.get(i).getDataPoints(AVG_PENDING_PACKETS);
            Double[] dataPoints2 = bottlenecks.get(i + 1).getDataPoints(AVG_PENDING_PACKETS);
            if (dataPoints1.length == 1 && dataPoints2.length == 1) {
                if (dataPoints1[0] < dataPoints2[0]) {
                    // Found an increasing subsequence.
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public void close() {
        this.backpressureDetector.close();
    }
}


