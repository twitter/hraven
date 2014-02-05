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
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.w3c.dom.Document;

import com.twitter.vulture.conf.AppConfCache;
import com.twitter.vulture.conf.AppConfiguraiton;
import com.twitter.vulture.conf.VultureConfiguration;
import com.twitter.vulture.rpc.ClientCache;
import com.twitter.vulture.rpc.RestClient;
import com.twitter.vulture.rpc.RestClient.RestException;

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
  /**
   * The interface to MRAppMaster
   */
  private ClientServiceDelegate clientService;
  private ClientCache clientCache;
  private AppConfCache appConfCache = AppConfCache.getInstance();

  private VultureConfiguration vConf;

  public AppStatusChecker(VultureConfiguration conf,
      ApplicationReport appReport, ClientCache clientCache,
      ResourceMgrDelegate rmDelegate) {
    this.appReport = appReport;
    this.appId = appReport.getApplicationId();
    JobID jobId = TypeConverter.fromYarn(appReport.getApplicationId());
    this.jobId = jobId;
    this.clientCache = clientCache;
    LOG.info("getting a client connection for " + appReport.getApplicationId());
    this.vConf = conf;
    this.rmDelegate = rmDelegate;
  }

  void init() {
    AppConfiguraiton appConf = getAppConf(appReport);
    this.appConf = appConf;
  }

  boolean checkApp() {
    boolean passCheck =
        appConf.getAppPolicy().checkAppStatus(appReport, appConf);
    if (!passCheck)
      killApp(appReport);
    return passCheck;
  }

  @Override
  public void run() {
    // 0. set thread name
    setThreadName();
    LOG.info("Running " + Thread.currentThread().getName());
    init();
    if (!checkApp())
      return;
    clientService = clientCache.getClient(jobId);
    checkTasks(TaskType.MAP);
    checkTasks(TaskType.REDUCE);
    clientCache.stopClient(jobId);
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
      LOG.error("Error in killing job " + appReport.getApplicationId(), e);
    }
  }

  /**
   * Retrieve the tasks of the same kind and check their status.
   * 
   * @param taskType
   */
  private void checkTasks(TaskType taskType) {
    try {
      // 1. get the list of running tasks
      TaskReport[] taskReports = clientService.getTaskReports(jobId, taskType);
      LOG.info("taskType= " + taskType + " taskReport.size = "
          + taskReports.length);
      long currTime = System.currentTimeMillis();
      // 2. check task status
      for (TaskReport taskReport : taskReports) {
        checkTask(taskType, taskReport, currTime);
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
   */
  private void checkTask(TaskType taskType, TaskReport taskReport,
      long currTime) {
    boolean okTask =
        appConf.getTaskPolicy().checkTask(appReport, taskType, taskReport,
            appConf, currTime);
    if (!okTask)
      LOG.error(taskReport.getTaskId() + " identified as BAD");
    else
      LOG.warn(taskReport.getTaskId() + " passes the check");
    if (okTask)
      return;

    // the task is potentially problematic, check the attempts to make sure
    Collection<TaskAttemptID> attemptIds =
        taskReport.getRunningTaskAttemptIds();
    LOG.info(taskReport.getTaskId() + " has " + attemptIds.size()
        + " attempts, checking on them...");
    for (TaskAttemptID attemptId : attemptIds) {
      String xmlUrl = buildXmlUrl(taskReport, attemptId);
      Document taskAttemptXml;
      try {
        taskAttemptXml = RestClient.getInstance().getXml(xmlUrl);
      } catch (RestException e) {
        LOG.error("Error in connecting to REST api from " + xmlUrl, e);
        return;
      }
      boolean okAttempt =
          appConf.getTaskPolicy().checkTaskAttempt(appReport, taskType,
              taskReport, appConf, attemptId, taskAttemptXml);
      if (!okAttempt)
        killTaskAttempt(taskReport, attemptId);
      else
        LOG.info("LET the task " + attemptId + " run.");
    }
  }

  // e.g. URL: http://atla-atz-03-sr1.prod.twttr.net:50030/
  // proxy/application_1389724922546_0058/
  // ws/v1/mapreduce/jobs/job_1389724922546_0058/
  // tasks/task_1389724922546_0058_m_000000/attempts/xxx
  private String buildXmlUrl(TaskReport taskReport, TaskAttemptID attemptId) {
    String trackingUrl = appReport.getTrackingUrl();
    String taskId = taskReport.getTaskId().toString();
    String xmlUrl =
        "http://" + trackingUrl + "ws/v1/mapreduce/jobs/" + jobId + "/tasks/"
            + taskId + "/attempts/" + attemptId;
    return xmlUrl;
  }

  /**
   * Kill a task attempt
   * 
   * @param taskReport
   * @param taskAttemptId
   */
  private void killTaskAttempt(TaskReport taskReport,
      TaskAttemptID taskAttemptId) {
    LOG.warn("KILLING " + taskAttemptId);
    if (vConf.isDryRun())
      return;
    try {
      clientService.killTask(taskAttemptId, true);
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
