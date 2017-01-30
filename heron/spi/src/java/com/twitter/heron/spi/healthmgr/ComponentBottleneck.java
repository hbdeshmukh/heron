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
package com.twitter.heron.spi.healthmgr;


import java.util.ArrayList;
import java.util.Set;

import com.twitter.heron.spi.metricsmgr.metrics.MetricsInfo;

public class ComponentBottleneck extends Bottleneck {
  String componentName;
  ArrayList<InstanceBottleneck> instances;

  public ComponentBottleneck(String componentName) {
    this.componentName = componentName;
    this.instances = new ArrayList<>();
  }

  public void add(int containerId, int instanceId, Set<MetricsInfo> metrics) {
    instances.add(new InstanceBottleneck(containerId, instanceId, metrics));
  }

  public String toString() {
    return componentName + " " + instances.toString();
  }

  public String getComponentName() {
    return componentName;
  }

  public ArrayList<InstanceBottleneck> getInstances() {
    return instances;
  }

  public boolean contains(String metric, String value) {
    for (InstanceBottleneck instanceBottleneck : instances) {
      if (instanceBottleneck.contains(metric, value))
        return true;
    }
    return false;
  }

  public boolean containsNonZero(String metric) {
    int count = 0;
    for (InstanceBottleneck instanceBottleneck : instances) {
      if (instanceBottleneck.contains(metric, "0"))
        count++;
    }
    if(count == instances.size()){
      return false;
    }
    return true;
  }
}

