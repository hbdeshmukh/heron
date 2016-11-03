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
import com.twitter.heron.healthmgr.detector.FailedTuplesDetector;
import com.twitter.heron.healthmgr.detector.FailedTuplesResult;
import com.twitter.heron.healthmgr.resolver.FailedTuplesResolver;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.healthmgr.Diagnosis;
import com.twitter.heron.spi.healthmgr.SLAPolicy;

public class FailedTuplesPolicy implements SLAPolicy {

  private FailedTuplesDetector detector = new FailedTuplesDetector();
  private FailedTuplesResolver resolver = new FailedTuplesResolver();
  private TopologyAPI.Topology topology;

  @Override
  public void initialize(Config conf, Config runtime) {
    this.topology = Runtime.topology(runtime);
    detector.initialize(conf, runtime);
    resolver.initialize(conf, runtime);
  }

  @Override
  public void execute() {
    Diagnosis<FailedTuplesResult> diagnosis = detector.detect(topology);
    if (diagnosis != null) {
      resolver.resolve(diagnosis, topology);
    }
  }

  @Override
  public void close() {
    detector.close();
    resolver.close();
  }
}
