package com.twitter.vulture;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.mapred.ClientCache;
import org.apache.hadoop.mapred.ResourceMgrDelegate;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.mortbay.log.Log;

/**
 * Vulture Service
 * 
 * The idea is to have a daemon that kills the not well-behaving tasks/jobs.
 * 
 * The benefits are: (i) Protect the other jobs from the not well-behaving job,
 * (ii) Early notification to the user to fix the problem with its long-running,
 * problematic job, (iii) Taking advantage of non-determinism in execution path,
 * (iv) hoping that a restarted task has a good chance of succeeding.
 * 
 * Behavior is defined by a combination of: Execution time, Amount written, and
 * Progress.
 * 
 * Well Behavior can be specified (i) Statically, either by the job submitter or
 * by the cluster admin, or (ii) Dynamically, by identifying anomalies, having
 * normal behavior is defined by the history of the job
 * 
 */
public class VultureService {
  ScheduledExecutorService clusterCheckerExecutor;
  ExecutorService appCheckerExecutor;
  VultureConfiguration conf = new VultureConfiguration();
  ClientCache clientCache;
  ResourceMgrDelegate rmDelegate;

  public void init() {
    YarnConfiguration yConf = new YarnConfiguration();
    rmDelegate = new ResourceMgrDelegate(yConf);
    clientCache = new ClientCache(conf, rmDelegate);
    clusterCheckerExecutor =
        Executors
            .newSingleThreadScheduledExecutor(new ClusterStatusChecker.SimpleThreadFactory());
    appCheckerExecutor =
        Executors
            .newCachedThreadPool(new AppStatusChecker.SimpleThreadFactory());
  }

  public void start() {
    long intervalSec =
        conf.getLong(VultureConfiguration.NEW_APP_CHECKER_INTERVAL_SEC,
            VultureConfiguration.DEFAULT_NEW_APP_CHECKER_INTERVAL_SEC);
    clusterCheckerExecutor.scheduleAtFixedRate(new ClusterStatusChecker(conf,
        appCheckerExecutor, rmDelegate, clientCache), 0, intervalSec,
        TimeUnit.SECONDS);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        Log.warn("Shutting down rest client...");
        RestClient.getInstance().shutdown();
        Log.warn("...done");
      }
    });
  }

  public static void main(String[] args) {
    System.out.println("VultureService!");
    VultureService vultureService = new VultureService();
    vultureService.init();
    vultureService.start();

    // Other threads are daemon, so prevent the current thread from exiting
    while (true) {
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

  }
}
