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

package com.twitter.heron.spi.metricsmgr.metrics;

/**
 * An immutable class providing a view of MetricsInfo
 * The value is in type String, and IMetricsSink would determine how to parse it.
 */
public class MetricsInfo {
  private final String name;
  private final String value;

  public MetricsInfo(String name, String value) {
    this.name = name;
    this.value = value;
  }

  /**
   * Get the name of the metric
   *
   * @return the name of the metric
   */
  public String getName() {
    return name;
  }

  /**
   * Get the value of the metric
   *
   * @return the value of the metric
   */
  public String getValue() {
    return value;
  }

  @Override
  public String toString() {
    return String.format("%s = %s", getName(), getValue());
  }

  @Override
  public boolean equals(Object other) {
    if ((other == null) || (getClass() != other.getClass())) {
      return false;
    } else {
      MetricsInfo metric = (MetricsInfo) other;
      return name.equals(metric.getName()) && value == metric.getValue();
    }
  }

  @Override
  public int hashCode()
  {
    int hash = 7;
    hash = 31 * hash + name.hashCode();
    hash = 31 * hash + value.hashCode();
    return hash;
  }

}
