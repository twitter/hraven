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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.concurrent.ThreadFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.mapred.ClientCache;
import org.apache.hadoop.mapred.ClientServiceDelegate;
import org.apache.hadoop.mapred.ResourceMgrDelegate;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.w3c.dom.Document;

import com.twitter.hraven.hadoopJobMonitor.conf.AppConfCache;
import com.twitter.hraven.hadoopJobMonitor.conf.AppConfiguraiton;
import com.twitter.hraven.hadoopJobMonitor.conf.HadoopJobMonitorConfiguration;
import com.twitter.hraven.hadoopJobMonitor.conf.AppConfiguraiton.ConfigurationAccessException;
import com.twitter.hraven.hadoopJobMonitor.jmx.WhiteList;
import com.twitter.hraven.hadoopJobMonitor.metrics.HadoopJobMonitorMetrics;
import com.twitter.hraven.hadoopJobMonitor.policy.TaskPolicy;
import com.twitter.hraven.hadoopJobMonitor.rpc.ClientCache;
import com.twitter.hraven.hadoopJobMonitor.rpc.RestClient;
import com.twitter.hraven.hadoopJobMonitor.rpc.RestClient.RestException;

/**
 * Check the status of a running app, including its task
 * 
 * This is a stateless implementation meaning that we each time retrieve the
 * entire list of running tasks. This is in contrast with caching the list of
 * old tasks and only ask for the list of newly started tasks.
 */
public class AppStatusChecker implements Runnable {
  public static final Log LOG = LogFactory.getLog(AppStatusChecker.class);
  private ApplicationId appId;
  private JobID jobId;
  private ApplicationReport appReport;
  private AppConfiguraiton appConf;
  private ResourceMgrDelegate rmDelegate;
  private AppCheckerProgress appCheckerProgress;
  /**
   * The interface to MRAppMaster
   */
  private ClientServiceDelegate clientService;
  private ClientCache clientCache;
  private AppConfCache appConfCache = AppConfCache.getInstance();
  private HadoopJobMonitorMetrics metrics = HadoopJobMonitorMetrics.getInstance();

  private HadoopJobMonitorConfiguration vConf;

  public AppStatusChecker(HadoopJobMonitorConfiguration conf,
      ApplicationReport appReport, ClientCache clientCache,
      ResourceMgrDelegate rmDelegate, AppCheckerProgress appCheckerProgress) {
    this.appReport = appReport;
    this.appId = appReport.getApplicationId();
    JobID jobId = TypeConverter.fromYarn(appReport.getApplicationId());
    this.jobId = jobId;
    this.clientCache = clientCache;
    LOG.debug("getting a client connection for " + appReport.getApplicationId());
    this.vConf = conf;
    this.rmDelegate = rmDelegate;
    this.appCheckerProgress = appCheckerProgress;
  }

  void init() throws ConfigurationAccessException {
    AppConfiguraiton appConf = getAppConf(appReport);
    this.appConf = appConf;
  }

  boolean checkApp() {
    metrics.inspectedApps.incr();
    String errMsg =
        appConf.getAppPolicy().checkAppStatus(appReport, appConf);
    if (errMsg != null)
      killApp(appReport, errMsg);
    return errMsg == null;
  }

  private boolean isInWhitelist(ApplicationReport appReport) {
    String appId = appReport.getApplicationId().toString();
    if (vConf.isUserInWhitelist(appReport.getUser()))
      return true;
    if (WhiteList.getInstance().isWhiteListed(appId))
      return true;
    return false;
  }

  @Override
  public void run() {
    // 0. set thread name
    setThreadName();
    if (isInWhitelist(appReport)) {
      LOG.warn("Skipping whitelisted app " + appId);
      return;
    }
    LOG.debug("Running " + Thread.currentThread().getName());
    try {
      try {
        init();
      } catch (Exception e) {
        LOG.error("Skipping app " + appId + " due to initialization error: "
            + e.getMessage());
        return;
      }
      if (!checkApp())
        return;
      loadClientService();
      checkTasks(TaskType.MAP);
      checkTasks(TaskType.REDUCE);
      clientCache.stopClient(jobId);
    } catch (YarnException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
      appCheckerProgress.finished();
    }
  }
  
  void loadClientService() throws YarnException {
    clientService = clientCache.getClient(jobId);
  }

  private AppConfiguraiton getAppConf(ApplicationReport appReport) throws ConfigurationAccessException {
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
      LOG.warn(e);
      throw new AppConfiguraiton.ConfigurationAccessException(e);
    }
    return appConf;
  }

  // e.g. URL: http://ip:50030/
  // proxy/application_1385075571494_429386/conf
  private String buildXmlUrl(ApplicationReport appReport) {
    String trackingUrl = appReport.getTrackingUrl();
    String xmlUrl = trackingUrl + "conf";
    return xmlUrl;
  }

  /**
   * kill an application.
   * 
   * @param appReport
   */
  private void killApp(ApplicationReport appReport, String errMsg) {
    LOG.info("App kill request " + appReport.getApplicationId());
    if (vConf.isDryRun())
      return;
    LOG.warn("KILLING job " + appReport.getApplicationId());
    try {
      metrics.killedApps.incr();
      //leave it for when the new API make it to a release of hadoop
//      rmDelegate.killApplication(appReport.getApplicationId(), "killed by hadoopJobMonitor: "+errMsg);
      rmDelegate.killApplication(appReport.getApplicationId());
    } catch (YarnRuntimeException e) {
      LOG.error("Error in killing job " + appReport.getApplicationId(), e);
    } catch (YarnException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   * Retrieve the tasks of the same kind and check their status.
   * 
   * @param taskType
   */
  private void checkTasks(TaskType taskType) {
    try {
      int maxBadTasks = this.appConf.getInt(
          HadoopJobMonitorConfiguration.MAX_BAD_TASKS_CHECKED, 
          HadoopJobMonitorConfiguration.DEFAULT_MAX_BAD_TASKS_CHECKED);
      // 1. get the list of running tasks
      TaskReport[] taskReports = clientService.getTaskReports(jobId, taskType);
      LOG.debug("taskType= " + taskType + " taskReport.size = "
          + taskReports.length);
      long currTime = System.currentTimeMillis();
      // 2. check task status
      int badTasks = 0;
      for (TaskReport taskReport : taskReports) {
        boolean isOk = checkTask(taskType, taskReport, currTime);
        if (!isOk)
          badTasks++;
        if (badTasks > maxBadTasks)
          break;
      }
    } catch (IOException e) {
      LOG.error("Error in getting task reports of job " + jobId, e);
    }
  }

  /**
   * Check the status of a task
   * 
   * @param taskReport
   * @param currTime
   * @return true if the task is well-behaved
   */
   boolean checkTask(TaskType taskType, TaskReport taskReport,
      long currTime) {
    metrics.inspectedTasks.incr();
    switch (taskType) {
    case MAP:
      metrics.inspectedMappers.incr();
      break;
    case REDUCE:
      metrics.inspectedReducers.incr();
      break;
    default:
    }
    boolean okTask =
        appConf.getTaskPolicy().checkTask(appReport, taskType, taskReport,
            appConf, currTime);
    if (!okTask)
      LOG.error(taskReport.getTaskId() + " identified as BAD");
    else
      LOG.debug(taskReport.getTaskId() + " passes the check");
    if (okTask)
      return true;
    //else

    // the task is potentially problematic, check the attempts to make sure
    Collection<TaskAttemptID> attemptIds =
        taskReport.getRunningTaskAttemptIds();
    LOG.debug(taskReport.getTaskId() + " has " + attemptIds.size()
        + " attempts, checking on them...");
    for (TaskAttemptID attemptId : attemptIds) {
      String xmlUrl = buildXmlUrl(taskReport, attemptId);
      Document taskAttemptXml;
      try {
        taskAttemptXml = RestClient.getInstance().getXml(xmlUrl);
      } catch (RestException e) {
        LOG.error("Error in connecting to REST api from " + xmlUrl, e);
        return false;
      }
      String errMsg =
          appConf.getTaskPolicy().checkTaskAttempt(appReport, taskType,
              taskReport, appConf, attemptId, taskAttemptXml, currTime);
      if (errMsg != null)
        killTaskAttempt(taskReport, taskType, attemptId, errMsg);
      else
        LOG.debug("LET the task " + attemptId + " run.");
    }
    return false;
  }

  // e.g. URL: http://ip:50030/
  // proxy/application_1389724922546_0058/
  // ws/v1/mapreduce/jobs/job_1389724922546_0058/
  // tasks/task_1389724922546_0058_m_000000/attempts/xxx
  private String buildXmlUrl(TaskReport taskReport, TaskAttemptID attemptId) {
    String trackingUrl = appReport.getTrackingUrl();
    TaskID taskId = taskReport.getTaskID();
    String xmlUrl =
        "http://" + trackingUrl + "ws/v1/mapreduce/jobs/" + jobId + "/tasks/"
            + taskId + "/attempts/" + attemptId;
    return xmlUrl;
  }

  /**
   * Kill a task attempt
   * 
   * @param taskReport
   * @param taskType 
   * @param taskAttemptId
   */
  private void killTaskAttempt(TaskReport taskReport,
      TaskType taskType, TaskAttemptID taskAttemptId, String errMsg) {
    LOG.warn("task kill request " + taskAttemptId);
    if (vConf.isDryRun())
      return;
    LOG.warn("KILLING " + taskAttemptId);
    try {
      metrics.killedTasks.incr();
      switch (taskType) {
      case MAP:
        metrics.killedMappers.incr();
        break;
      case REDUCE:
        metrics.killedReducers.incr();
        break;
      default:
      }
      //leave it for when the new API make it to a hadoop release
//      clientService.killTask(taskAttemptId, false, "killed by hadoopJobMonitor: " + errMsg);
      clientService.killTask(taskAttemptId, false);
    } catch (IOException e) {
      LOG.warn("Error in killing task " + taskAttemptId, e);
    }
  }

  private void setThreadName() {
    String tName =
        AppStatusChecker.class.getSimpleName() + "-" + appId.toString();
    Thread.currentThread().setName(tName);
  }

  /**
   * A simple thread factory for this thread
   */
  static class SimpleThreadFactory implements ThreadFactory {
    static ThreadGroup threadGroup = new ThreadGroup(
        AppStatusChecker.class.getSimpleName());

    public Thread newThread(Runnable r) {
      Thread thread = new Thread(threadGroup, r);
      thread.setDaemon(true);
      return thread;
    }
  }

}
