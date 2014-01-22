package com.twitter.vulture;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.ClientCache;
import org.apache.hadoop.mapred.ClientServiceDelegate;
import org.apache.hadoop.mapred.ResourceMgrDelegate;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.YarnClient;
import org.apache.hadoop.yarn.client.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;

/**
 * Check the status of the running apps in the cluster
 * 
 * This is a stateless implementation meaning that each time we retrieve the
 * entire list of running apps. This is in contrast with caching the list of old
 * apps and only ask for the list of newly started apps.
 */
public class ClusterStatusChecker implements Runnable {
  public static final Log LOG = LogFactory.getLog(ClusterStatusChecker.class);
  ExecutorService appCheckerExecutor;
  ResourceMgrDelegate rmDelegate;
  VultureConfiguration conf;
  /**
   * The cache for connections to MRAppMasters
   * 1
   */
  ClientCache clientCache;

  public ClusterStatusChecker(VultureConfiguration conf,
      ExecutorService appCheckerExecutor, ResourceMgrDelegate rmDelegate,
      ClientCache clientCache) {
    this.appCheckerExecutor = appCheckerExecutor;
    this.conf = conf;
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
      List<ApplicationReport> appList = rmDelegate.getApplicationList();
      LOG.info("appList received. size is: " + appList.size());
      for (ApplicationReport appReport : appList)
        checkAppStatus(appReport);
    } catch (YarnRemoteException e) {
      LOG.error(e);
      e.printStackTrace();
    }
  }

  /**
   * Check the status of an application.
   * 
   * This method can be overridden to define new policies
   * 
   * @param appReport
   */
  protected void checkAppStatus(ApplicationReport appReport) {
    YarnApplicationState appState = appReport.getYarnApplicationState();
    switch (appState) {
    case RUNNING:
      break;
    case NEW:
    case SUBMITTED:
    case FINISHED:
    case FAILED:
    case KILLED:
    case ACCEPTED:
      LOG.info("Skip app " + appReport.getApplicationId() + " state is "
          + appState);
      return;
    default:
      LOG.warn("Unknown app state, app is " + appReport.getApplicationId()
          + " state is " + appState);
      return;
    }
    final long start = appReport.getStartTime();
    final long currTime = System.currentTimeMillis();
    final long duration = currTime - start;
    final long maxJobLenSec =
        conf.getLong(VultureConfiguration.MAX_JOB_LEN_SEC,
            VultureConfiguration.DEFAULT_MAX_JOB_LEN_SEC);
    if (duration > maxJobLenSec * 1000) {
      killApp(appReport);
      return;
    }
    checkOnTasks(appReport);
  }

  /**
   * Check on the tasks of a running app.
   * 
   * This method can be overridden to define new policies
   * 
   * @param appReport
   */
  protected void checkOnTasks(ApplicationReport appReport) {
    JobID jobId = TypeConverter.fromYarn(appReport.getApplicationId());
    LOG.info("getting a client connection for " + appReport.getApplicationId());
    ClientServiceDelegate clientService = clientCache.getClient(jobId);
    // 2. spawn a new thread to check on each app
    LOG.info("Spawning a tread to check on app " + appReport.getApplicationId());
    appCheckerExecutor.execute(new AppStatusChecker(conf, appReport, jobId,
        clientService));
  }

  /**
   * kill an application.
   * 
   * This method can be overridden to adapt to new/old hadoop APIs
   * 
   * @param appReport
   */
  protected void killApp(ApplicationReport appReport) {
    LOG.info("KILLING job " + appReport.getApplicationId());
    try {
      rmDelegate.killApplication(appReport.getApplicationId());
    } catch (YarnRemoteException e) {
      LOG.error(e);
      e.printStackTrace();
    }
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
