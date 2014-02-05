/*
Copyright 2013 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.twitter.vulture;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.ResourceMgrDelegate;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.twitter.vulture.conf.AppConfCache;
import com.twitter.vulture.conf.VultureConfiguration;
import com.twitter.vulture.notification.Mail;
import com.twitter.vulture.notification.Notifier;
import com.twitter.vulture.rpc.ClientCache;
import com.twitter.vulture.rpc.RestClient;

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
  public static final Log LOG = LogFactory.getLog(VultureService.class);

  ScheduledExecutorService clusterCheckerExecutor;
  ExecutorService appCheckerExecutor;
  VultureConfiguration conf = new VultureConfiguration();
  ClientCache clientCache;
  ResourceMgrDelegate rmDelegate;

  public void init() {
    YarnConfiguration yConf = new YarnConfiguration();
    rmDelegate = new ResourceMgrDelegate(yConf);
    clientCache = new ClientCache(conf, rmDelegate);
    AppConfCache.init(conf);
    Mail.init(conf);
    Notifier.init(conf);
    clusterCheckerExecutor =
        Executors
            .newSingleThreadScheduledExecutor(new ClusterStatusChecker.SimpleThreadFactory());
    int concurrentAppCheckers =
        conf.getInt(VultureConfiguration.NEW_APP_CHECKER_CONCURRENCY,
            VultureConfiguration.DEFAULT_NEW_APP_CHECKER_CONCURRENCY);
    appCheckerExecutor =
        new BlockingExecutor(concurrentAppCheckers,
            new AppStatusChecker.SimpleThreadFactory());
  }

  public void start() {
    if (conf.isDryRun())
      System.err.println("========== DRYRUN ===========");
    long intervalSec =
        conf.getLong(VultureConfiguration.NEW_APP_CHECKER_INTERVAL_SEC,
            VultureConfiguration.DEFAULT_NEW_APP_CHECKER_INTERVAL_SEC);
    clusterCheckerExecutor.scheduleAtFixedRate(new ClusterStatusChecker(conf,
        appCheckerExecutor, rmDelegate, clientCache), 0, intervalSec,
        TimeUnit.SECONDS);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        LOG.warn("Shutting down rest client...");
        RestClient.getInstance().shutdown();
        LOG.warn("...done");
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

  /**
   * This is to limit the number of concurrent threads checking on applications.
   * This affects (i) the pressure on the RM and AMs as well as (ii) the
   * required memory space.
   */
  class BlockingExecutor extends ThreadPoolExecutor {

    public BlockingExecutor(int corePoolSize, ThreadFactory threadFactory) {
      super(corePoolSize, corePoolSize, 0L, TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue<Runnable>(), threadFactory);
    }

    protected void beforeExecute(Thread t, Runnable r) {
      LOG.debug("THREAD SLOT ACQUIRED " + this.getPoolSize());
      super.beforeExecute(t, r);
    }

    protected void afterExecute(Runnable r, Throwable t) {
      super.afterExecute(r, t);
      LOG.debug("THREAD SLOT RELEASED current: " + this.getPoolSize());
      if (t == null && r instanceof Future<?>) {
        try {
          ((Future<?>) r).get();
        } catch (CancellationException ce) {
          t = ce;
        } catch (ExecutionException ee) {
          t = ee.getCause();
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt(); // ignore/reset
        }
      }
      if (t != null)
        LOG.fatal("Error in running task", t);
    }
  }
}
