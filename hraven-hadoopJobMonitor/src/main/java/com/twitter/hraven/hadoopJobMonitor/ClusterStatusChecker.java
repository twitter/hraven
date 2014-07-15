/*
Copyright 2014 Twitter, Inc.

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
package com.twitter.hraven.hadoopJobMonitor;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.ResourceMgrDelegate;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import com.twitter.hraven.hadoopJobMonitor.conf.HadoopJobMonitorConfiguration;
import com.twitter.hraven.hadoopJobMonitor.jmx.WhiteList;
import com.twitter.hraven.hadoopJobMonitor.rpc.ClientCache;

/**
 * Check the status of the running apps in the cluster
 * 
 * This is a stateless implementation meaning that each time we retrieve the
 * entire list of running apps. This is in contrast with caching the list of old
 * apps and only ask for the list of newly started apps.
 */
public class ClusterStatusChecker implements Runnable {
  public static final Log LOG = LogFactory.getLog(ClusterStatusChecker.class);
  private ExecutorService appCheckerExecutor;
  private ResourceMgrDelegate rmDelegate;
  private HadoopJobMonitorConfiguration vConf;
  private Set<ApplicationId> runningAppCheckers = 
      Collections.newSetFromMap(new ConcurrentHashMap<ApplicationId, Boolean>());
  
  /**
   * The cache for connections to MRAppMasters
   */
  private ClientCache clientCache;

  public ClusterStatusChecker(HadoopJobMonitorConfiguration conf,
      ExecutorService appCheckerExecutor, ResourceMgrDelegate rmDelegate,
      ClientCache clientCache) {
    this.appCheckerExecutor = appCheckerExecutor;
    this.vConf = conf;
    this.clientCache = clientCache;
    this.rmDelegate = rmDelegate;
  }

  /**
   * Get the app list from RM and check status of each
   */
  @Override
  public void run() {
    // 1. get the list of running apps
    LOG.info("Running " + ClusterStatusChecker.class.getName());
    try {
      YarnConfiguration yConf = new YarnConfiguration();
      LOG.info(yConf.get(YarnConfiguration.RM_ADDRESS));
      LOG.info("Getting appList ...");
      // TODO: in future hadoop API we will be able to filter the app list
      List<ApplicationReport> appList = rmDelegate.getApplications();
      LOG.info("appList received. size is: " + appList.size());
      for (ApplicationReport appReport : appList)
        checkAppStatus(appReport);
    } catch (YarnRuntimeException e) {
      LOG.error("Error in getting application list from RM", e);
    } catch (YarnException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   * The pre-check performed on the app status. The idea is to reduce the avoid
   * paying the cost of the actual check, if it app does not need it.
   * 
   * @param appReport
   * @return true if app passes this pre-check
   */
  private boolean preAppStatusCheck(ApplicationReport appReport) {
    YarnApplicationState appState = appReport.getYarnApplicationState();
    LOG.debug("checking app " + appReport.getApplicationId() + " state is "
        + appState);
    return appState == YarnApplicationState.RUNNING;
  }

  /**
   * Check the status of an application.
   * 
   * @param appReport
   */
  private void checkAppStatus(ApplicationReport appReport) {
    final ApplicationId appId = appReport.getApplicationId();
    if (isInWhitelist(appReport)) {
      LOG.warn("Skip whitelisted app " + appId
          + " from user " + appReport.getUser());
      return;
    }
    if (runningAppCheckers.contains(appReport.getApplicationId())) {
      LOG.warn("Skip already-being-checked app " + appId
          + " from user " + appReport.getUser());
      return;
    } else {
      runningAppCheckers.add(appReport.getApplicationId());
    }
    boolean passPreCheck = preAppStatusCheck(appReport);
    if (!passPreCheck)
      return;
    // 2. spawn a new thread to check on each app
    LOG.debug("Spawning a thread to check on app " + appReport.getApplicationId());
    // if (!appReport.getUser().equals("myabandeh"))
    // return;
    AppCheckerProgress appCheckerProgress = new AppCheckerProgress() {
      @Override
      public void finished() {
        runningAppCheckers.remove(appId);
      }
    };
    appCheckerExecutor.execute(new AppStatusChecker(vConf, appReport,
        clientCache, rmDelegate, appCheckerProgress));
  }

  private boolean isInWhitelist(ApplicationReport appReport) {
    String appId = appReport.getApplicationId().toString();
    if (vConf.isUserInWhitelist(appReport.getUser()))
      return true;
    if (WhiteList.getInstance().isWhiteListed(appId))
      return true;
    return false;
  }

  /**
   * A thread factory for this thread
   */
  static class SimpleThreadFactory implements ThreadFactory {
    ThreadGroup threadGroup = new ThreadGroup(
        ClusterStatusChecker.class.getSimpleName());

    public Thread newThread(Runnable r) {
      Thread thread = new Thread(threadGroup, r);
      thread.setDaemon(true);
      thread.setName(ClusterStatusChecker.class.getSimpleName());
      return thread;
    }
  }

}
