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

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.healthmgr.services.DetectorService;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.healthmgr.ComponentBottleneck;
import com.twitter.heron.spi.healthmgr.Diagnosis;
import com.twitter.heron.spi.healthmgr.IDetector;

public class SlowInstanceDetector implements IDetector<ComponentBottleneck> {
  private static final String EXECUTION_COUNT_METRIC = "__execute-count/default";

  private Config runtime;

  private BackPressureDetector backpressureDetector = new BackPressureDetector();
  private ReportingDetector executeCountDetector = new ReportingDetector(EXECUTION_COUNT_METRIC);
  private BufferRateDetector bufferRateDetector = new BufferRateDetector();

  private DetectorService detectorService;

  @Override
  public void initialize(Config config, Config inputRuntime) {
    this.runtime = inputRuntime;
    this.backpressureDetector.initialize(config, inputRuntime);
    this.executeCountDetector.initialize(config, inputRuntime);
    this.bufferRateDetector.initialize(config, inputRuntime);
    detectorService = (DetectorService) Runtime
        .getDetectorService(runtime);
  }

  @Override
  public Diagnosis<ComponentBottleneck> detect(TopologyAPI.Topology topology) {
    return DataSkewDetector
        .commonDiagnosis(runtime, topology, backpressureDetector, executeCountDetector, bufferRateDetector, -1);
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
}
