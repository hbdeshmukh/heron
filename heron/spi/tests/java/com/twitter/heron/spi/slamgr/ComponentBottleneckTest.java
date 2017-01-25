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
package com.twitter.heron.spi.slamgr;


import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.spi.metricsmgr.metrics.MetricsInfo;


public class ComponentBottleneckTest {

  @Test
  public void testComponentBottleneckEquals() {
    ComponentBottleneck bottleneck = new ComponentBottleneck("component");
    Set<MetricsInfo> metrics = new HashSet<MetricsInfo>();
    metrics.add(new MetricsInfo("testMetric1", "1"));
    metrics.add(new MetricsInfo("testMetric2", "2"));
    bottleneck.add(1, 1, metrics);
    bottleneck.add(1, 2, metrics);
    Assert.assertEquals(bottleneck.contains("testMetric1", "1"), true);
    Assert.assertEquals(bottleneck.contains("testMetric1", "0"), false);

  }
}
