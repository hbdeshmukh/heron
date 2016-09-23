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
package com.twitter.heron.grouping;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Logger;

import com.twitter.heron.api.grouping.CustomStreamGrouping;
import com.twitter.heron.api.topology.TopologyContext;

public class RemoteAffinityGrouping implements CustomStreamGrouping {
  private static final Logger LOG = Logger.getLogger(LocalAffinityGrouping.class.getName());
  private static final long serialVersionUID = 1913733461146490337L;

  List<List<Integer>> localTargetLists = new ArrayList<>();
  int localTargetSize;
  List<List<Integer>> remoteTargetLists = new ArrayList<>();
  int remoteTargetSize;
  private int roundRobinIndex = 0;

  @Override
  public void prepare(TopologyContext context,
                      String component,
                      String streamId,
                      List<Integer> targetTasks) {
    HashSet<Integer> localTasksIds = new HashSet<>();
    try {
      BufferedReader br = new BufferedReader(new FileReader("global_task_id_file"));
      String id;
      while ((id = br.readLine()) != null) {
        localTasksIds.add(Integer.parseInt(id));
      }
      System.out.println("Task ids local to this container: " + localTasksIds);
      br.close();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (NumberFormatException e) {
      e.printStackTrace();
    }

    List<Integer> localTargetTaskIds = new ArrayList<>();
    List<Integer> remoteTargetTaskIds = new ArrayList<>();
    for (int targetTask : targetTasks) {
      if (localTasksIds.contains(targetTask)) {
        localTargetTaskIds.add(targetTask);
      } else {
        remoteTargetTaskIds.add(targetTask);
      }
    }

    localTargetSize = localTargetTaskIds.size();
    for (int targetId : localTargetTaskIds) {
      List<Integer> targetList = new ArrayList<>();
      targetList.add(targetId);
      localTargetLists.add(targetList);
    }

    remoteTargetSize = remoteTargetTaskIds.size();
    for (int targetId : remoteTargetTaskIds) {
      List<Integer> targetList = new ArrayList<>();
      targetList.add(targetId);
      remoteTargetLists.add(targetList);
    }

    System.out.println("Remote targets: " + remoteTargetTaskIds);
  }

  @Override
  public List<Integer> chooseTasks(List<Object> values) {
    if (++roundRobinIndex >= remoteTargetSize) {
      roundRobinIndex = 0;
    }
    return remoteTargetLists.get(roundRobinIndex);
  }
}
