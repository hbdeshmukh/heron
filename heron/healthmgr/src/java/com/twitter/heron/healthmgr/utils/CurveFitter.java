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

import org.apache.commons.math3.stat.regression.SimpleRegression;

import java.util.List;

public class CurveFitter {
  public double getSlope() {
    return slope;
  }

  public void setSlope(double slope) {
    this.slope = slope;
  }

  public double getIntercept() {
    return intercept;
  }

  public void setIntercept(double intercept) {
    this.intercept = intercept;
  }

  public double getMeanSquaredError() {
    return meanSquaredError;
  }

  public void setMeanSquaredError(double meanSquaredError) {
    this.meanSquaredError = meanSquaredError;
  }

  private double slope;
  private double intercept;
  private double meanSquaredError;

  public CurveFitter() {
  }

  @Override
  public String toString() {
    return "Slope = " + slope + " Intercept = " + intercept + " mean squared error = " + meanSquaredError;
  }

  /**
   * Computes the slope, intercept and the RMSE for the linear fit.
   * @param x The x points
   * @param y The y points
   */
  public void linearCurveFit(List<Double> x, List<Double> y) {
    assert x.size() == y.size();
    // NOTE - The SimpleRegression model requires at least three data points.
    assert x.size() >= 3;
    SimpleRegression simpleRegression = new SimpleRegression(true);
    for (int i = 0; i < x.size(); i++) {
      simpleRegression.addData(x.get(i), y.get(i));
    }
    intercept = simpleRegression.getIntercept();
    slope = simpleRegression.getSlope();
    meanSquaredError = simpleRegression.getMeanSquareError();
  }
}
