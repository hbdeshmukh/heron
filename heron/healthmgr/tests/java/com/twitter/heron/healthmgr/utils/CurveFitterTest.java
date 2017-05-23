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

package com.twitter.heron.healthmgr.utils;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class CurveFitterTest {
  public CurveFitterTest() {
  }

  @Test
  public void testCurveFitting() {
    CurveFitter curveFitter = new CurveFitter();
    List<Double> xPoints = new ArrayList<>();
    List<Double> yPoints = new ArrayList<>();

    // Add (0, 2).
    xPoints.add(0.0);
    yPoints.add(2.0);
    // Add (1, 3).
    xPoints.add(1.0);
    yPoints.add(3.0);
    // Add (2, 4).
    xPoints.add(2.0);
    yPoints.add(4.0);
    // Expect y = 2 + x as the fit.

    curveFitter.linearCurveFit(xPoints, yPoints);
    assert curveFitter.getIntercept() == 2;
    assert curveFitter.getSlope() == 1;
    assert curveFitter.getMeanSquaredError() == 0;
  }
}
