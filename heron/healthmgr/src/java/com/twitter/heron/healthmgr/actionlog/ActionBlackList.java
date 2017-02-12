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

import com.twitter.heron.spi.healthmgr.Bottleneck;
import com.twitter.heron.spi.healthmgr.Diagnosis;

public class ActionBlackList {
  HashMap<String, ArrayList<ActionEntry<? extends Bottleneck>>> blackListedActions;

  public ActionBlackList(){
    blackListedActions = new HashMap<>();
  }

  public <T extends Bottleneck> void addToBlackList(String topologyName, String actionTaken,
                                                   Diagnosis<T> data){
    ActionEntry<T> action = new ActionEntry<>(actionTaken, data);
    ArrayList<ActionEntry<? extends Bottleneck>> entries = this.blackListedActions.get(topologyName);
    if (entries == null) {
      entries = new ArrayList<>();
    }
    entries.add(action);
    this.blackListedActions.put(topologyName, entries);
    System.out.println(this.blackListedActions);
  }

  public HashMap<String, ArrayList<ActionEntry<? extends Bottleneck>>> getBlackList() {
    return blackListedActions;
  }

  public ArrayList<ActionEntry<? extends Bottleneck>> getTopologyBlackList(String topologyName) {
    if(blackListedActions.containsKey(topologyName)) {
      return blackListedActions.get(topologyName);
    }
    return null;
  }

  public boolean existsBlackList(String topologyName) {
    if (blackListedActions.containsKey(topologyName)) {
      return true;
    }
    return false;
  }

  @Override
  public String toString(){
    return blackListedActions.toString();
  }
}
