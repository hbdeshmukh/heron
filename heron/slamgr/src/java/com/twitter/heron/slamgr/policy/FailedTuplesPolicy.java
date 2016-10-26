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


package com.twitter.heron.slamgr.policy;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.slamgr.detector.FailedTuplesDetector;
import com.twitter.heron.slamgr.resolver.FailedTuplesResolver;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.metricsmgr.sink.SinkVisitor;
import com.twitter.heron.spi.slamgr.Diagnosis;
import com.twitter.heron.spi.slamgr.SLAPolicy;

public class FailedTuplesPolicy implements SLAPolicy {

  private FailedTuplesDetector detector;
  private FailedTuplesResolver resolver;
  private TopologyAPI.Topology topology;

  @Override
  public void initialize(Config conf, TopologyAPI.Topology t,
                         SinkVisitor visitor) {
    this.topology = t;
    detector.initialize(conf, visitor);
    resolver.initialize(conf);
  }

  @Override
  public void execute() {
    Diagnosis<String> diagnosis = detector.detect(topology);
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
