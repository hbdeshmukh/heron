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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.yaml.snakeyaml.Yaml;

import com.twitter.heron.common.basics.ByteAmount;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.metric.api.GlobalMetrics;
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

/**
 * This is a topology in which bolt delay can be injected. Create a delay yaml config file:
 * /tmp/heron.yaml
 * Format:
 * <p>
 * Task Id:
 *   container: "yarn container id suffix"
 *   delay: "delay in micros"
 * <p>
 * Sample /tmp/heron.yaml:
 * 2:
 *   container: "003"
 *   delay: "2"
 */
public final class DelayingExclamationTopology {

  private DelayingExclamationTopology() {
  }

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();
    int parallelism = 1;

    builder.setSpout("word", new TestWordSpout(), parallelism);
    builder.setBolt("exclaim", new DelayingBolt(), parallelism).shuffleGrouping("word");

    Config conf = new Config();
    conf.setDebug(true);
    conf.setMaxSpoutPending(100000);
    conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-XX:+HeapDumpOnOutOfMemoryError");
    conf.setComponentRam("word", ByteAmount.fromGigabytes(1));
    conf.setComponentRam("exclaim", ByteAmount.fromGigabytes(1));
    conf.setContainerDiskRequested(ByteAmount.fromGigabytes(5));
    conf.setContainerCpuRequested(5);

    StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
  }

  static String getYarnContainerId() {
    return Paths.get(new File(".").getAbsolutePath()).getParent().getFileName().toString();
  }

  static class Restriction {
    private int taskId;
    private String containerId;
    private int executionDelayMicros;
    private long previousExecution;

    public Restriction(TopologyContext context, String container) {
      this.taskId = context.getThisTaskId();
      previousExecution = System.currentTimeMillis();
      containerId = container;
      System.out.println(String.format("Start task %d in container %s", taskId, containerId));
    }

    @SuppressWarnings("unchecked")
    private void setDelay() {
      if (System.currentTimeMillis() - previousExecution < 5000) {
        return;
      }
      previousExecution = System.currentTimeMillis();

      try {
        Yaml yaml = new Yaml();
        Map<Integer, Object> delayMap =
            (Map<Integer, Object>) yaml.load(new FileInputStream("/tmp/heron.yaml"));
        Map<String, String> taskConfig = (Map<String, String>) delayMap.get(taskId);
        if (taskConfig != null) {
          String containerToBeDelayed = taskConfig.get("container");

          if (containerToBeDelayed == null || containerId.endsWith(containerToBeDelayed)) {
            executionDelayMicros = Integer.valueOf(taskConfig.get("delay"));
            System.out.println(String.format("Delay for task %d in container %s is %d micros",
                taskId, containerId, executionDelayMicros));
            return;
          }
        }
        executionDelayMicros = 0;
      } catch (FileNotFoundException e) {
        System.out.println("No delay config file found");
        executionDelayMicros = 0;
      }
    }

    public void execute() {
      setDelay();
      if (executionDelayMicros > 0) {
        try {
          TimeUnit.MICROSECONDS.sleep(executionDelayMicros);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public static class DelayingBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1184860508880121352L;
    private long nItems;
    private long startTime;
    Restriction restriction;

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(
        Map conf,
        TopologyContext context,
        OutputCollector collector) {
      nItems = 0;
      startTime = System.currentTimeMillis();
      restriction = new Restriction(context, getYarnContainerId());
    }

    @Override
    public void execute(Tuple tuple) {
      restriction.execute();

      if (++nItems % 100000 == 0) {
        long latency = System.currentTimeMillis() - startTime;
        System.out.println(tuple.getString(0) + "!!!");
        System.out.println("Bolt processed " + nItems + " tuples in " + latency + " ms");
        GlobalMetrics.incr("selected_items");
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      // declarer.declare(new Fields("word"));
    }
  }

  static class TestWordSpout extends BaseRichSpout {

    private static final long serialVersionUID = -3217886193225455451L;
    private SpoutOutputCollector collector;
    private String[] words;
    private Random rand;
    Restriction restriction;

    public void open(
        Map<String, Object> conf,
        TopologyContext context,
        SpoutOutputCollector acollector) {
      collector = acollector;
      words = new String[]{"nathan", "mike", "jackson", "golda", "bertels"};
      rand = new Random();
      restriction = new Restriction(context, getYarnContainerId());
    }

    public void close() {
    }

    public void nextTuple() {
      restriction.execute();
      final String word = (int) (Math.random() * 1000) + words[rand.nextInt(words.length)];
      collector.emit(new Values(word));
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
