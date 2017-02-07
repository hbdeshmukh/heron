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

package com.twitter.heron.healthmgr.clustering;


import java.util.ArrayList;
import java.util.HashMap;

public class DiscreteValueClustering extends ClusterGenerator {

  private Double[] dataPoints;

  public DiscreteValueClustering() {
  }

  public DiscreteValueClustering(int noClusters) {

    super(noClusters);
  }

  private void load(Double[] data) {
    this.dataPoints = data;
  }

  private HashMap<String, ArrayList<Integer>> createClusters() {
    HashMap<String, ArrayList<Integer>> clusters = new HashMap<String, ArrayList<Integer>>();
    for (int i = 0; i < dataPoints.length; i++) {
      ArrayList<Integer> data;
      if (clusters.containsKey(dataPoints[i])) {
        data = clusters.get(dataPoints[i]);
      } else {
        data = new ArrayList<>();
        clusters.put(Double.toString(dataPoints[i]), data);
      }
      data.add(i);
    }
    return clusters;
  }

  public HashMap<String, ArrayList<Integer>> createClusters(Double[] data) {
    load(data);
    return createClusters();
  }

  public HashMap<String, ArrayList<Integer>> createBinaryClusters(Double[] data, double threshold) {
    load(data);
    convertData(threshold);
    return createClusters();
  }

  private void convertData(double threshold) {
    for (int i = 0; i < dataPoints.length; i++) {
      if (dataPoints[i] > threshold) {
        dataPoints[i] = 1.0;
      } else {
        dataPoints[i] = 0.0;
      }
    }
  }
}
