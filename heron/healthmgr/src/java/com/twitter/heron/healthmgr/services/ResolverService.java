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

package com.twitter.heron.healthmgr.services;


import java.util.concurrent.Callable;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.healthmgr.actionlog.ActionBlackList;
import com.twitter.heron.healthmgr.actionlog.ActionEntry;
import com.twitter.heron.healthmgr.actionlog.ActionLog;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.healthmgr.Bottleneck;
import com.twitter.heron.spi.healthmgr.Diagnosis;
import com.twitter.heron.spi.healthmgr.IDetector;
import com.twitter.heron.spi.healthmgr.IResolver;

public class ResolverService {

  private ActionLog log;
  private ActionBlackList blacklist;

  public ResolverService() {

    this.log = new ActionLog();
    this.blacklist = new ActionBlackList();
  }

  public void initialize(Config config, Config runtime) {
  }


  public <T extends Bottleneck> boolean run(IResolver<T> resolver, TopologyAPI.Topology topology,
                                            String problem, Diagnosis<T> diagnosis) {

    log.addAction(topology.getName(), problem, diagnosis);
    return resolver.resolve(diagnosis, topology);
  }

  public <T extends Bottleneck> void addToBlackList(TopologyAPI.Topology topology, String actionTaken,
                                                    Diagnosis<T> data) {
    blacklist.addToBlackList(topology.getName(), actionTaken, data);
    System.out.println(blacklist.toString());
  }

  @SuppressWarnings("unchecked")
  public <T extends Bottleneck> boolean isBlackListedAction(TopologyAPI.Topology topology, String action,
                                                            Diagnosis<T> data,
                                                            IDetector<T> detector){
    if(blacklist.existsBlackList(topology.getName())){
      for (ActionEntry<? extends Bottleneck> actionEntry : blacklist.getTopologyBlackList(
          topology.getName())) {
        if (action.equals(actionEntry.getAction()) &&
            detector.similarDiagnosis(data, ((ActionEntry<T>) actionEntry).getDiagnosis())) {
          return true;
        }
      }
    }
    return false;
  }

  public ActionLog getLog() {
    return this.log;
  }

  public ActionBlackList getBlacklist() {
    return blacklist;
  }

  public void close() {

  }
}
