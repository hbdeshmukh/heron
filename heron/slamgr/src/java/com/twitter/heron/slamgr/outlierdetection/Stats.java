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
package com.twitter.heron.slamgr.outlierdetection;


public class Stats {

  public static double median(double[] m) {
    int middle = m.length / 2;
    if (m.length % 2 == 1) {
      return m[middle];
    } else {
      return (m[middle - 1] + m[middle]) / 2.0;
    }
  }

  public static double[] subtract(double[] m, double value) {
    double[] result = new double[m.length];
    for (int i = 0; i < m.length; i++) {
      result[i] = Math.abs(result[i] - value);
    }
    return result;
  }

  public static double mad(double[] m) {
    double median = median(m);
    double[] newData = subtract(m, median);
    return median(newData);
  }
}
