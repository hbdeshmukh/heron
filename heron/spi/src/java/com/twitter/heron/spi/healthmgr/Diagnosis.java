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
package com.twitter.heron.spi.healthmgr;

import java.util.HashSet;
import java.util.Set;

/**
 * Describes a set of problems detected by a detector
 */
public class Diagnosis<T extends Bottleneck> {

  private Set<T> summary;

  public Diagnosis(){
    summary = new HashSet<>();
  }
  public Diagnosis(Set<T> summary) {
    this.summary = summary;
  }

  public Set<T> getSummary() {
    return summary;
  }

  public void addToDiagnosis(T item){
    summary.add(item);
  }
}
