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

package com.twitter.heron.examples;

import java.util.Map;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;

import backtype.storm.metric.api.GlobalMetrics;

public final class NetworkBoundTopology {

  private NetworkBoundTopology() {
  }

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();

    int noSpouts = Integer.parseInt(args[1]);
    int noBolts = Integer.parseInt(args[2]);
    builder.setSpout("word", new NetworkBoundTopology.NetworkSpout(), noSpouts);
    builder.setBolt("exclaim1", new NetworkBoundTopology.NetworkBolt(), noBolts).
        shuffleGrouping("word");

    Config conf = new Config();
    conf.setDebug(true);
    conf.setMaxSpoutPending(10);
    conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-XX:+HeapDumpOnOutOfMemoryError");
    conf.setEnableAcking(true);
    //conf.setComponentRam("word", 512L * 1024 * 1024);
    //conf.setComponentRam("exclaim1", 512L * 1024 * 1024);
    //conf.setContainerDiskRequested(1024L * 1024 * 1024);
    //conf.setContainerCpuRequested(1);

    if (args != null && args.length > 0) {
      if ((noSpouts + noBolts) / 4 < 1) {
        conf.setNumStmgrs(1);
      } else {
        conf.setNumStmgrs((noSpouts + noBolts) / 4);
      }
      HeronSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
  }

  public static class NetworkBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1184860508880121352L;
    private long nItems;
    private long startTime;
    private OutputCollector collector;

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(
        Map conf,
        TopologyContext context,
        OutputCollector col) {
      nItems = 0;
      startTime = System.currentTimeMillis();
      this.collector = col;
    }

    @Override
    public void execute(Tuple tuple) {
      if (++nItems % 100 == 0) {
        long latency = System.currentTimeMillis() - startTime;
        System.out.println("Bolt processed " + nItems + " tuples in " + latency + " ms");
        GlobalMetrics.incr("selected_items");
        collector.ack(tuple);
      } else {
        collector.ack(tuple);
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
  }

  public static class NetworkSpout extends BaseRichSpout {

    private static final long serialVersionUID = -3217886193225455451L;
    private SpoutOutputCollector collector;
    private Values tuple;

    public void open(
        Map<String, Object> conf,
        TopologyContext context,
        SpoutOutputCollector acollector) {
      collector = acollector;
      StringBuffer randStr = new StringBuffer();
      for (int i = 0; i < 1024; i++) {
        randStr.append('a');
      }
      tuple = new Values(randStr.toString());
    }

    public void close() {
    }

    public void nextTuple() {
      for (int i = 0; i < 10000; i++) {
        collector.emit(tuple, "MESSAGE_ID");
      }
    }

    public void ack(Object msgId) {
    }

    public void fail(Object msgId) {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
  }
}
