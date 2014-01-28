package com.twitter.vulture;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ResourceMgrDelegate;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;

import com.twitter.vulture.conf.AppConfCache;
import com.twitter.vulture.conf.AppConfiguraiton;
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
  private AppConfCache appConfCache = AppConfCache.getInstance();
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
      LOG.error(e);
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
    switch (appState) {
    case RUNNING:
      return true;
    case NEW:
    case SUBMITTED:
    case FINISHED:
    case FAILED:
    case KILLED:
    case ACCEPTED:
      LOG.info("Skip app " + appReport.getApplicationId() + " state is "
          + appState);
      return false;
    default:
      LOG.warn("Unknown app state, app is " + appReport.getApplicationId()
          + " state is " + appState);
      return false;
    }
  }

  /**
   * Check the status of an application.
   * 
   * @param appReport
   */
  private void checkAppStatus(ApplicationReport appReport) {
    boolean passPreCheck = preAppStatusCheck(appReport);
    if (!passPreCheck)
      return;
    AppConfiguraiton appConf = getAppConf(appReport);
    boolean passCheck =
        appConf.getAppPolicy().checkAppStatus(appReport, appConf);
    if (!passCheck)
      killApp(appReport);
    else
      checkOnTasks(appReport, appConf);
  }

  private AppConfiguraiton getAppConf(ApplicationReport appReport) {
    ApplicationId appId = appReport.getApplicationId();
    AppConfiguraiton appConf = appConfCache.get(appId);
    if (appConf != null)
      return appConf;
    String xmlUrlStr = buildXmlUrl(appReport);
    URL xmlUrl;
    try {
      xmlUrl = new URL(xmlUrlStr);
      Configuration origAppConf = new Configuration(false);
      origAppConf.addResource(xmlUrl);
      appConf = new AppConfiguraiton(origAppConf, vConf);
      appConfCache.put(appId, appConf);
    } catch (MalformedURLException e) {
      System.out.println(e);
      e.printStackTrace();
    }
    return appConf;
  }

  // e.g. URL: http://atla-bar-41-sr1.prod.twttr.net:50030/
  // proxy/application_1385075571494_429386/conf
  private String buildXmlUrl(ApplicationReport appReport) {
    String trackingUrl = appReport.getTrackingUrl();
    String xmlUrl = "http://" + trackingUrl + "conf";
    return xmlUrl;
  }

  /**
   * Check on the tasks of a running app.
   * 
   * @param appReport
   */
  private void checkOnTasks(ApplicationReport appReport,
      AppConfiguraiton appConf) {
    JobID jobId = TypeConverter.fromYarn(appReport.getApplicationId());
    // 2. spawn a new thread to check on each app
    LOG.info("Spawning a tread to check on app " + appReport.getApplicationId());
    appCheckerExecutor.execute(new AppStatusChecker(vConf, appConf, appReport,
        jobId, clientCache));
  }

  /**
   * kill an application.
   * 
   * @param appReport
   */
  private void killApp(ApplicationReport appReport) {
    LOG.info("KILLING job " + appReport.getApplicationId());
    if (vConf.isDryRun())
      return;
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
