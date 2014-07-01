/*
 * Copyright (c) 2013 Yahoo! Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.storm.perftest;

import java.util.Map;

import backtype.storm.generated.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import backtype.storm.utils.NimbusClient;
import backtype.storm.generated.TaskSummary;
import backtype.storm.generated.TaskStats;

public class Main {
  private static final Log LOG = LogFactory.getLog(Main.class);

  @Option(name="--help", aliases={"-h"}, usage="print help message")
  private boolean _help = false;
  
  @Option(name="--debug", aliases={"-d"}, usage="enable debug")
  private boolean _debug = false;
  
  @Option(name="--local", usage="run in local mode")
  private boolean _local = false;
  
  @Option(name="--messageSizeByte", aliases={"--messageSize"}, metaVar="SIZE",
      usage="size of the messages generated in bytes")
  private int _messageSize = 100;

  @Option(name="--numTopologies", aliases={"-n"}, metaVar="TOPOLOGIES",
      usage="number of topologies to run in parallel")
  private int _numTopologies = 1;
 
   @Option(name="--numLevels", aliases={"-l"}, metaVar="LEVELS",
      usage="number of levels of bolts per topolgy")
  private int _numLevels = 1;

  @Option(name="--spoutParallel", aliases={"--spout"}, metaVar="SPOUT",
      usage="number of spouts to run in parallel")
  private int _spoutParallel = 3;
  
  @Option(name="--boltParallel", aliases={"--bolt"}, metaVar="BOLT",
      usage="number of bolts to run in parallel")
  private int _boltParallel = 3;
  
  @Option(name="--numWorkers", aliases={"--workers"}, metaVar="WORKERS",
      usage="number of workers to use per topology")
  private int _numWorkers = 3;
  
  @Option(name="--ackers", metaVar="ACKERS", 
      usage="number of acker bolts to launch per topology")
  private int _ackers = 0;
  
  @Option(name="--maxSpoutPending", aliases={"--maxPending"}, metaVar="PENDING",
      usage="maximum number of pending messages per spout (only valid if acking is enabled)")
  private int _maxSpoutPending = 1000;
  
  @Option(name="--name", aliases={"--topologyName"}, metaVar="NAME",
      usage="base name of the topology (numbers may be appended to the end)")
  private String _name = "test";
  
  @Option(name="--ackEnabled", aliases={"--ack"}, usage="enable acking")
  private boolean _ackEnabled = false;
  
  @Option(name="--pollFreqSec", aliases={"--pollFreq"}, metaVar="POLL",
      usage="How often should metrics be collected")
  private int _pollFreqSec = 4;
  
  @Option(name="--testTimeSec", aliases={"--testTime"}, metaVar="TIME",
      usage="How long should the benchmark run for.")
  private int _testRunTimeSec = 2 * 60;

  private static class MetricsState {
    long transferred = 0;
    long lastTime = 0;
    int slotsUsed = 0;
    double lastThroughput = 0.00;
  }


  public void metrics(Nimbus.Client client, int poll, int total) throws Exception {
    System.out.println("status\ttopologies\ttotalSlots\tslotsUsed\ttargetTasks\ttargetTasksWithMetrics\ttime\ttime-diff ms\temitted\tthroughput (Kp/s)");
    MetricsState state = new MetricsState();
    long pollMs = poll * 1000;
    long now = System.currentTimeMillis();
    state.lastTime = now;
    long startTime = now;
    long cycle = 0;
    long sleepTime;
    long wakeupTime;
    while (metrics(client, now, state, "WAITING")) {
      now = System.currentTimeMillis();
      cycle = (now - startTime)/pollMs;
      wakeupTime = startTime + (pollMs * (cycle + 1));
      sleepTime = wakeupTime - now;
      if (sleepTime > 0) {
        Thread.sleep(sleepTime);
      }
      now = System.currentTimeMillis();
    }

    now = System.currentTimeMillis();
    cycle = (now - startTime)/pollMs;
    wakeupTime = startTime + (pollMs * (cycle + 1));
    sleepTime = wakeupTime - now;
    if (sleepTime > 0) {
      Thread.sleep(sleepTime);
    }
    now = System.currentTimeMillis();
    long end = now + (total * 1000);
    double sumThroughput = 0;
    int nPolls = 0;
    do {
      metrics(client, now, state, "RUNNING");
      sumThroughput += state.lastThroughput;
      nPolls ++;
      now = System.currentTimeMillis();
      cycle = (now - startTime)/pollMs;
      wakeupTime = startTime + (pollMs * (cycle + 1));
      sleepTime = wakeupTime - now;
      if (sleepTime > 0) {
        Thread.sleep(sleepTime);
      }
      now = System.currentTimeMillis();
    } while (now < end);
    double avgThroughput = sumThroughput / nPolls;
    System.out.println("RUNNING " + nPolls + " Polls, AVG_Throughput = " + avgThroughput + " Kp/s");
  }

  public boolean metrics(Nimbus.Client client, long now, MetricsState state, String message) throws Exception {
    ClusterSummary summary = client.getClusterInfo();
    int totalSlots = 0;
    int totalUsedSlots = 0;
    for (SupervisorSummary sup: summary.get_supervisors()) {
      totalSlots += sup.get_num_workers();
      totalUsedSlots += sup.get_num_used_workers();
    }
    int slotsUsedDiff = totalUsedSlots - state.slotsUsed;
    state.slotsUsed = totalUsedSlots;

    int numTopologies = summary.get_topologies_size();
    long totalTransferred = 0;
    int targetTasks = 0;
    int targetTasksWithMetrics = 0;
    for (TopologySummary ts: summary.get_topologies()) {
      String id = ts.get_id();
      TopologyInfo info = client.getTopologyInfo(id);
      for (TaskSummary taskSummary: info.get_tasks()) {
        if ("messageSpout".equals(taskSummary.get_component_id())) {
            targetTasks++;
            TaskStats stats = taskSummary.get_stats();
            if (stats != null) {
                Map<String,Map<String,Long>> emitted = stats.get_emitted();
                if ( emitted != null) {
                    Map<String, Long> e2 = emitted.get("All-time");
                    if (e2 != null && !e2.isEmpty()) {
                        targetTasksWithMetrics++;
                        //The SOL messages are always on the default stream, so just count those
                        Long dflt = e2.get("default");
                        if (dflt != null) {
                            totalTransferred += dflt;
                        }
                    }
                }
            }
        }
      }
    }
    long transferredDiff = totalTransferred - state.transferred;
    state.transferred = totalTransferred;
    long time = now - state.lastTime;
    state.lastTime = now;
    double throughput = (transferredDiff == 0 || time == 0) ? 0.0 : ((double)transferredDiff)/((double)time);
    state.lastThroughput = throughput;
    System.out.println(message+"\t"+numTopologies+"\t"+totalSlots+"\t"+totalUsedSlots+"\t"+targetTasks+"\t"+targetTasksWithMetrics+"\t"+now+"\t"+time+"\t"+transferredDiff+"\t"+throughput);

    return !(totalUsedSlots > 0 && slotsUsedDiff == 0 && targetTasks > 0 && targetTasksWithMetrics >= targetTasks);
  } 

 
  public void realMain(String[] args) throws Exception {
    Map clusterConf = Utils.readStormConfig();
    clusterConf.putAll(Utils.readCommandLineOpts());
    Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();

    CmdLineParser parser = new CmdLineParser(this);
    parser.setUsageWidth(80);
    try {
      // parse the arguments.
      parser.parseArgument(args);
    } catch( CmdLineException e ) {
      // if there's a problem in the command line,
      // you'll get this exception. this will report
      // an error message.
      System.err.println(e.getMessage());
      _help = true;
    }
    if(_help) {
      parser.printUsage(System.err);
      System.err.println();
      return;
    }
    if (_numWorkers <= 0) {
      throw new IllegalArgumentException("Need at least one worker");
    }
    if (_name == null || _name.isEmpty()) {
      throw new IllegalArgumentException("name must be something");
    }
    if (!_ackEnabled) {
      _ackers = 0;
    }

    try {
      for (int topoNum = 0; topoNum < _numTopologies; topoNum++) {
        TopologyBuilder builder = new TopologyBuilder();
        LOG.info("Adding in "+_spoutParallel+" spouts");
        builder.setSpout("messageSpout", 
            new SOLSpout(_messageSize, _ackEnabled), _spoutParallel);
        LOG.info("Adding in "+_boltParallel+" bolts");
        builder.setBolt("messageBolt1", new SOLBolt(), _boltParallel)
            .shuffleGrouping("messageSpout");
        for (int levelNum = 2; levelNum <= _numLevels; levelNum++) {
          LOG.info("Adding in "+_boltParallel+" bolts at level "+levelNum);
          builder.setBolt("messageBolt"+levelNum, new SOLBolt(), _boltParallel)
              .shuffleGrouping("messageBolt"+(levelNum - 1));
        }

        Config conf = new Config();
        conf.setDebug(_debug);
        conf.setNumWorkers(_numWorkers);
        conf.setNumAckers(_ackers);
        if (_maxSpoutPending > 0) {
          conf.setMaxSpoutPending(_maxSpoutPending);
        }

        StormSubmitter.submitTopology(_name+"_"+topoNum, conf, builder.createTopology());
      }
      metrics(client, _pollFreqSec, _testRunTimeSec);
    } finally {
      //Kill it right now!!!
      KillOptions killOpts = new KillOptions();
      killOpts.set_wait_secs(1);

      for (int topoNum = 0; topoNum < _numTopologies; topoNum++) {
        LOG.info("KILLING "+_name+"_"+topoNum);
        try {
          client.killTopologyWithOpts(_name+"_"+topoNum, killOpts);
        } catch (Exception e) {
          LOG.error("Error tying to kill "+_name+"_"+topoNum,e);
        }
      }
    }
  }
  
  public static void main(String[] args) throws Exception {
    new Main().realMain(args);
  }
}
