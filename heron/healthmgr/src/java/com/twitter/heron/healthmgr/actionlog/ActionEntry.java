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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;

import com.twitter.heron.spi.healthmgr.Bottleneck;
import com.twitter.heron.spi.healthmgr.Diagnosis;

public class ActionEntry<T extends Bottleneck> {
  private String actionTime;
  private String problem;
  private ArrayList<String> possibleProblems;
  private Diagnosis<T> diagnosis;

  public ActionEntry(String problem, ArrayList<String> possibleProblems,
                     Diagnosis<T> data) {
    this.actionTime = new SimpleDateFormat("yyyyMMdd_HHmmss")
        .format(Calendar.getInstance().getTime());
    this.problem = problem;
    this.possibleProblems = possibleProblems;
    this.diagnosis = data;
  }

  public String getActionTime() {
    return actionTime;
  }

  public String getProblem() {
    return problem;
  }

  public ArrayList<String> getPossibleProblems() {
    return possibleProblems;
  }

  public Diagnosis<T> getDiagnosis() {
    return diagnosis;
  }
}
