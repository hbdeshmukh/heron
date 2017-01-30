//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License

package com.twitter.heron.examples;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public final class DataSkewTopology {
  private DataSkewTopology() {
  }

  /**
   * Main method
   */
  public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
    if (args.length < 1) {
      throw new RuntimeException("Specify topology name");
    }

    int parallelism = 1;
    if (args.length > 1) {
      parallelism = Integer.parseInt(args[1]);
    }
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("word", new WordSpout(), parallelism);
    builder.setBolt("exclaim1", new ConsumerBolt(), parallelism)
        .fieldsGrouping("word", new Fields("word"));
    Config conf = new Config();
    conf.setNumStmgrs(parallelism);

    /*
    Set config here
    */
    conf.setComponentRam("word", 2L * 1024 * 1024 * 1024);
    conf.setComponentRam("exclaim1", 3L * 1024 * 1024 * 1024);
    conf.setContainerCpuRequested(6);

    StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
  }

  /**
   * A spout that emits a random word
   */
  public static class WordSpout extends BaseRichSpout {
    private static final long serialVersionUID = 4322775001819135036L;

    private static final int ARRAY_LENGTH = 2;
    private static final int WORD_LENGTH = 10;

    private final String[] words = new String[ARRAY_LENGTH];

    private final Random rnd = new Random(31);

    private SpoutOutputCollector collector;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
      outputFieldsDeclarer.declare(new Fields("word"));
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void open(Map map, TopologyContext topologyContext,
                     SpoutOutputCollector spoutOutputCollector) {

      words[0] = "aaaaaaaaaa";
      words[1] = "bbbbbbbbbb";
      collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
      int nextInt;
      if (Math.random() <= 0.9) {
        nextInt = 0;
      } else {
        nextInt = 1;
      }
      collector.emit(new Values(words[nextInt]));
    }
  }

  /**
   * A bolt that counts the words that it receives
   */
  public static class ConsumerBolt extends BaseRichBolt {
    private static final long serialVersionUID = -5470591933906954522L;

    private OutputCollector collector;
    private Map<String, Integer> countMap;

    @SuppressWarnings("rawtypes")
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
      collector = outputCollector;
      countMap = new HashMap<String, Integer>();
    }

    @Override
    public void execute(Tuple tuple) {
      String key = tuple.getString(0);
      if (countMap.get(key) == null) {
        countMap.put(key, 1);
      } else {
        Integer val = countMap.get(key);
        countMap.put(key, ++val);
      }
      collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
  }
}
