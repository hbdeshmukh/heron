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

package com.twitter.heron.healthmgr.actionlog;

import java.util.ArrayList;
import java.util.HashMap;

import com.google.common.base.Optional;

import com.twitter.heron.spi.healthmgr.Bottleneck;
import com.twitter.heron.spi.healthmgr.Diagnosis;

public class ActionLog {
  HashMap<String, ArrayList<ActionEntry<? extends Bottleneck>>> log;

  public ActionLog() {
    this.log = new HashMap<>();
  }

  private <T extends Bottleneck> void addTopologyEntry(String topologyName,
                                                       ActionEntry<T> action) {
    ArrayList<ActionEntry<? extends Bottleneck>> topologyLog = this.log.get(topologyName);
    if (topologyLog == null) {
      topologyLog = new ArrayList<>();
    }
    topologyLog.add(action);
    this.log.put(topologyName, topologyLog);
    System.out.println(this.log);
  }

  public <T extends Bottleneck> void addAction(String topologyName, String problem,
                                               Diagnosis<T> diagnosis) {
    ActionEntry<T> action = new ActionEntry<T>(problem, diagnosis);
    addTopologyEntry(topologyName, action);
  }

  public ActionEntry<? extends Bottleneck> getLastAction(String topologyName) {
    ArrayList<ActionEntry<? extends Bottleneck>> topologyLog = this.log.get(topologyName);
    if (topologyLog == null) {
      return null;
    } else {
      ActionEntry<? extends Bottleneck> entry = topologyLog.get(topologyLog.size() - 1);
      return entry;
    }
  }

  public Optional<ArrayList<ActionEntry<? extends Bottleneck>>> getAllActions(String topologyName) {
    ArrayList<ActionEntry<? extends Bottleneck>> topologyLog = this.log.get(topologyName);
    if (topologyLog == null) {
      return Optional.absent();
    } else {
      return Optional.of(topologyLog);
    }
  }
}
