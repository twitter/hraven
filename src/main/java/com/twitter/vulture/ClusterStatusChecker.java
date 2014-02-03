package com.twitter.vulture;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.ResourceMgrDelegate;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;

import com.twitter.vulture.conf.VultureConfiguration;
import com.twitter.vulture.rpc.ClientCache;

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
  private VultureConfiguration vConf;
  /**
   * The cache for connections to MRAppMasters
   */
  private ClientCache clientCache;

  public ClusterStatusChecker(VultureConfiguration conf,
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
      List<ApplicationReport> appList = rmDelegate.getApplicationList();
      LOG.info("appList received. size is: " + appList.size());
      for (ApplicationReport appReport : appList)
        checkAppStatus(appReport);
    } catch (YarnRemoteException e) {
      LOG.error("Error in getting application list from RM", e);
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
    LOG.info("checking app " + appReport.getApplicationId() + " state is "
        + appState);
    return appState == YarnApplicationState.RUNNING;
  }

  /**
   * Check the status of an application.
   * 
   * @param appReport
   */
  private void checkAppStatus(ApplicationReport appReport) {
    if (isInWhitelist(appReport)) {
      LOG.info("Skip whitelisted app " + appReport.getApplicationId()
          + " from user " + appReport.getUser());
      return;
    }
    boolean passPreCheck = preAppStatusCheck(appReport);
    if (!passPreCheck)
      return;
    // 2. spawn a new thread to check on each app
    LOG.info("Spawning a tread to check on app " + appReport.getApplicationId());
    // if (!appReport.getUser().equals("myabandeh"))
    // return;
    appCheckerExecutor.execute(new AppStatusChecker(vConf, appReport,
        clientCache, rmDelegate));
  }

  private boolean isInWhitelist(ApplicationReport appReport) {
    if (vConf.isUserInWhitelist(appReport.getUser()))
      return true;
    if (vConf.isAppInWhitelist(appReport.getApplicationId().toString()))
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
