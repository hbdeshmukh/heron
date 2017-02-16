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

package com.twitter.heron.healthmgr;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.utils.logging.LoggingHelper;
import com.twitter.heron.healthmgr.services.DetectorService;
import com.twitter.heron.healthmgr.services.ResolverService;
import com.twitter.heron.healthmgr.sinkvisitor.TrackerVisitor;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.scheduler.client.SchedulerClientFactory;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.ConfigLoader;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Key;
import com.twitter.heron.spi.healthmgr.HealthPolicy;
import com.twitter.heron.spi.metricsmgr.sink.SinkVisitor;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlanProtoDeserializer;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.ReflectionUtils;

/**
 * e.g. options
 * -d ~/.heron -p ~/.heron/conf/local -c local -e default -r userName -t AckingTopology
 */
public class HealthManager {
  private static final Logger LOG = Logger.getLogger(HealthManager.class.getName());
  private final Config config;
  private Config runtime;
  private ScheduledExecutorService executor;
  private List<String> healthPolicies;
  private final DetectorService detectorService = new DetectorService();
  private final ResolverService resolverService = new ResolverService();

  public HealthManager(Config config, Config runtime) {
    this.config = config;
    this.runtime = runtime;
  }

  /**
   * Load the config parameters from the command line
   *
   * @param cluster, name of the cluster
   * @param role, user role
   * @param environ, user provided environment/tag
   * @param verbose, enable verbose logging
   * @return config, the command line config
   */
  protected static Config commandLineConfigs(String cluster,
                                             String role,
                                             String environ,
                                             String topologyName,
                                             Boolean verbose) {
    Config.Builder commandLineConfig = Config.newBuilder()
        .put(Key.CLUSTER, cluster)
        .put(Key.ROLE, role)
        .put(Key.ENVIRON, environ)
        .put(Key.TOPOLOGY_NAME, topologyName)
        .put(Key.VERBOSE, verbose);

    return commandLineConfig.build();
  }

  // Print usage options
  private static void usage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(HealthManager.class.getSimpleName(), options);
  }

  // Construct all required command line options
  private static Options constructCliOptions() {
    Options options = new Options();

    Option cluster = Option.builder("c")
        .desc("Cluster name in which the topology needs to run on")
        .longOpt("cluster")
        .hasArgs()
        .argName("cluster")
        .required()
        .build();

    Option role = Option.builder("r")
        .desc("Role under which the topology needs to run")
        .longOpt("role")
        .hasArgs()
        .argName("role")
        .required()
        .build();

    Option environment = Option.builder("e")
        .desc("Environment under which the topology needs to run")
        .longOpt("environment")
        .hasArgs()
        .argName("environment")
        .build();

    Option heronHome = Option.builder("d")
        .desc("Directory where heron is installed")
        .longOpt("heron_home")
        .hasArgs()
        .argName("heron home dir")
        .required()
        .build();

    Option configFile = Option.builder("p")
        .desc("Path of the config files")
        .longOpt("config_path")
        .hasArgs()
        .argName("config path")
        .required()
        .build();

    Option configOverrides = Option.builder("o")
        .desc("Command line override config path")
        .longOpt("override_config_file")
        .hasArgs()
        .argName("override config file")
        .build();

    Option topologyName = Option.builder("n")
        .desc("Name of the topology")
        .longOpt("topology_name")
        .hasArgs()
        .argName("topology name")
        .required()
        .build();

    Option trackerURL = Option.builder("t")
        .desc("Tracker url with port number")
        .longOpt("tracker_url")
        .hasArgs()
        .argName("tracker url")
        .build();

    Option verbose = Option.builder("v")
        .desc("Enable debug logs")
        .longOpt("verbose")
        .build();

    options.addOption(cluster);
    options.addOption(role);
    options.addOption(environment);
    options.addOption(heronHome);
    options.addOption(configFile);
    options.addOption(configOverrides);
    options.addOption(topologyName);
    options.addOption(trackerURL);
    options.addOption(verbose);

    return options;
  }

  // construct command line help options
  private static Options constructHelpOptions() {
    Options options = new Options();
    Option help = Option.builder("h")
        .desc("List all options and their description")
        .longOpt("help")
        .build();

    options.addOption(help);
    return options;
  }

  public static void main(String[] args) throws Exception {
    CommandLineParser parser = new DefaultParser();
    Options slaManagerCliOptions = constructCliOptions();

    // parse the help options first.
    Options helpOptions = constructHelpOptions();
    CommandLine cmd = parser.parse(helpOptions, args, true);
    if (cmd.hasOption("h")) {
      usage(slaManagerCliOptions);
      return;
    }

    try {
      cmd = parser.parse(slaManagerCliOptions, args);
    } catch (ParseException e) {
      usage(slaManagerCliOptions);
      throw new RuntimeException("Error parsing command line options: ", e);
    }

    Boolean verbose = false;
    Level logLevel = Level.INFO;
    if (cmd.hasOption("v")) {
      logLevel = Level.ALL;
      verbose = true;
    }

    // init log
    LoggingHelper.loggerInit(logLevel, false);

    String cluster = cmd.getOptionValue("cluster");
    String role = cmd.getOptionValue("role");
    String environ = cmd.getOptionValue("environment");
    String heronHome = cmd.getOptionValue("heron_home");
    String configPath = cmd.getOptionValue("config_path");
    String overrideConfigFile = cmd.getOptionValue("override_config_file");
    String releaseFile = cmd.getOptionValue("release_file");
    String topologyName = cmd.getOptionValue("topology_name");
    String trackerURL = cmd.getOptionValue("trackerURL", "http://localhost:8888");

    // build the final config by expanding all the variables
    Config config = Config.toLocalMode(Config.newBuilder()
        .putAll(ConfigLoader.loadConfig(heronHome, configPath, releaseFile, overrideConfigFile))
        .putAll(commandLineConfigs(cluster, role, environ, topologyName, verbose))
        .build());

    Config runtime = Config.newBuilder()
        .put(Key.TRACKER_URL, trackerURL)
        .build();

    LOG.info("Static config loaded successfully ");
    LOG.fine(config.toString());

    HealthManager healthManager = new HealthManager(config, runtime);
    healthManager.initialize();

    LOG.info("Starting the SLA manager");
    ScheduledFuture<?> future = healthManager.start();
    try {
      future.get();
    } catch (InterruptedException | ExecutionException e) {
      healthManager.executor.shutdownNow();
      throw e;
    }
  }

  private void initialize() throws ReflectiveOperationException {
    SchedulerStateManagerAdaptor adaptor = createStateMgrAdaptor();
    TopologyAPI.Topology topology = getTopology(adaptor);
    SinkVisitor sinkVisitor = new TrackerVisitor();
    PackingPlans.PackingPlan protoPackingPlan = adaptor.getPackingPlan(topology.getName());
    PackingPlanProtoDeserializer deserializer = new PackingPlanProtoDeserializer();
    PackingPlan packingPlan = deserializer.fromProto(protoPackingPlan);

    runtime = Config.newBuilder()
        .putAll(runtime)
        .put(Key.TOPOLOGY_DEFINITION, topology)
        .put(Key.SCHEDULER_STATE_MANAGER_ADAPTOR, adaptor)
        .put(Key.PACKING_PLAN, packingPlan)
        .put(Key.METRICS_READER_INSTANCE, sinkVisitor)
        .put(Key.HEALTH_MGR_DETECTOR_SERVICE, detectorService)
        .put(Key.HEALTH_MGR_RESOLVER_SERVICE, resolverService)
        .build();

    ISchedulerClient schedulerClient = new SchedulerClientFactory(config, runtime)
        .getSchedulerClient();

    runtime = Config.newBuilder()
        .putAll(runtime)
        .put(Key.SCHEDULER_CLIENT_INSTANCE, schedulerClient)
        .build();

    // TODO rename sinkvisitor
    sinkVisitor.initialize(config, runtime);

    detectorService.initialize(config, runtime);
    resolverService.initialize(config, runtime);

    healthPolicies = Context.healthPolicies(config);
  }

  private TopologyAPI.Topology getTopology(SchedulerStateManagerAdaptor adaptor) {
    String topologyName = Context.topologyName(config);
    LOG.log(Level.INFO, "Fetching topology from state manager: {0}", topologyName);
    TopologyAPI.Topology topology = adaptor.getTopology(topologyName);
    if (topology == null) {
      throw new RuntimeException(String.format("Failed to fetch topology: %s", topologyName));
    }
    return topology;
  }

  private SchedulerStateManagerAdaptor createStateMgrAdaptor() throws ReflectiveOperationException {
    String statemgrClass = Context.stateManagerClass(config);
    IStateManager statemgr = ReflectionUtils.newInstance(statemgrClass);
    statemgr.initialize(config);
    return new SchedulerStateManagerAdaptor(statemgr, 5000);
  }

  private ScheduledFuture<?> start() {
    // Create a single threaded executor to avoid concurrent policy execution
    executor = Executors.newSingleThreadScheduledExecutor();
    return executor.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        class PolicySchedulingInfo {
          long intervalMills;
          long lastExecutionTimeMills = 0;

          public long getDelay() {
            long currentTime = System.currentTimeMillis();
            long nextExecutionTime =
                lastExecutionTimeMills == 0 ? currentTime : lastExecutionTimeMills + intervalMills;
            return nextExecutionTime - currentTime;
          }
        }
        HashMap<HealthPolicy, PolicySchedulingInfo> policySchedule = new HashMap<>();

        // Load and initialize all the policies
        for (String healthPolicy : healthPolicies) {
          Map<String, String> policyConfigMap = config.getMapValue(healthPolicy);
          String policyClass = policyConfigMap.get(Key.HEALTH_POLICY_CLASS.value());
          long policyInterval = policyConfigMap.get(Key.HEALTH_POLICY_INTERVAL.value()) == null
              ? Long.MAX_VALUE : Long.valueOf(policyConfigMap.get(Key.HEALTH_POLICY_INTERVAL.value()));

          // provide all policy specific config to the policy for initialization
          Config.Builder policyConfig = Config.newBuilder().putAll(config);
          for (String policyConfKey : policyConfigMap.keySet()) {
            policyConfig.put(policyConfKey, policyConfigMap.get(policyConfKey));
          }

          HealthPolicy policy;
          try {
            policy = ReflectionUtils.newInstance(policyClass);
          } catch (ReflectiveOperationException e) {
            LOG.log(Level.WARNING, "Failed to instantiate HealthPolicy: " + policyClass, e);
            continue;
          }
          policy.initialize(policyConfig.build(), runtime);

          LOG.log(Level.INFO, "Policy {0} initialized and will be executed every {1} ms",
              new Object[]{policyClass, policyInterval});

          PolicySchedulingInfo policySchedulingInfo = new PolicySchedulingInfo();
          policySchedulingInfo.intervalMills = policyInterval;

          policySchedule.put(policy, policySchedulingInfo);
        }

        while (policySchedule.size() > 0) {
          for (HealthPolicy policy : policySchedule.keySet()) {
            PolicySchedulingInfo policySchedulingInfo = policySchedule.get(policy);
            if (policySchedulingInfo.getDelay() > 0) {
              continue;
            }

            LOG.info("Executing SLA Policy: " + policy.getClass().getSimpleName());
            policy.execute();
            policySchedulingInfo.lastExecutionTimeMills = System.currentTimeMillis();
          }
        }
      }
    }, 1, 1, TimeUnit.SECONDS);
  }
}
