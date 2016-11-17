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
package com.twitter.heron.slamgr.detector;

import java.util.ArrayList;
import java.util.HashMap;

public class BackPressureResult {
  //Components that were causing backpressure.
  private HashMap<String, BackPressureContainer> backPressureInfo;

  public BackPressureResult() {
    this.backPressureInfo = new HashMap<String, BackPressureContainer>();
  }

  public HashMap<String, BackPressureContainer> getInstancesBackPressure() {
    return backPressureInfo;
  }

  public void add(String component, int container, int instanceId, int backPressureValue) {
    BackPressureContainer containerData = backPressureInfo.get(component);
    if (containerData == null) {
      containerData = new BackPressureContainer();
    }
    containerData.add(container, instanceId, backPressureValue);
    backPressureInfo.put(component, containerData);
  }

  public String toString() {
    return backPressureInfo.toString();
  }


  public class BackPressureContainer {
    //Container to instanceIds of specific component
    HashMap<Integer, ArrayList<InstanceBackPressure>> containerInfo;

    public BackPressureContainer() {
      this.containerInfo = new HashMap<Integer, ArrayList<InstanceBackPressure>>();
    }

    public HashMap<Integer, ArrayList<InstanceBackPressure>> getContainerInfo() {
      return containerInfo;
    }

    public void add(Integer container, int instanceId, int backPressureValue)
        throws RuntimeException {
      ArrayList<InstanceBackPressure> containerData = containerInfo.get(container);
      if (containerData == null) {
        containerData = new ArrayList<>();
      }
      containerData.add(new InstanceBackPressure(instanceId, backPressureValue));
      containerInfo.put(container, containerData);
    }

    public String toString() {
      return containerInfo.toString();
    }
  }

  public class InstanceBackPressure {
    int instanceId;
    int backPressureValue;

    public InstanceBackPressure(int instanceId, int backPressureValue) {
      this.instanceId = instanceId;
      this.backPressureValue = backPressureValue;
    }

    public int getInstanceId() {
      return instanceId;
    }

    public String toString() {
      return instanceId + ":" + backPressureValue;
    }
  }
}
