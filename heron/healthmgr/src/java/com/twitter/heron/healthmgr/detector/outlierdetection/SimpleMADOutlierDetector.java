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
package com.twitter.heron.healthmgr.detector.outlierdetection;

import java.util.ArrayList;
import java.util.Arrays;

public class SimpleMADOutlierDetector extends OutlierDetector {
  private Double[] dataPoints;

  public SimpleMADOutlierDetector(Double threshold) {
    super(threshold);
  }

  public void load(Double[] data) {
    this.dataPoints = data;
  }

  public ArrayList<Integer> detectOutliers() {
    Double outlierMetric = Stats.mad(dataPoints);
    Double median = Stats.median(dataPoints);
    System.out.println(outlierMetric + " " + median + " " + Arrays.toString(dataPoints));
    ArrayList<Integer> outliers = new ArrayList<Integer>();
    for (int i = 0; i < dataPoints.length; i++) {
      System.out.println("OOO " + (dataPoints[i] - median) + " " + Math.abs(dataPoints[i] - median) + " " + getThreshold() * outlierMetric);
      if (Math.abs(dataPoints[i] - median) > getThreshold() * outlierMetric) {
        outliers.add(i);
      }
    }
    return outliers;
  }

  public ArrayList<Integer> detectOutliers(Double[] data) {
    this.load(data);
    return this.detectOutliers();
  }
}
