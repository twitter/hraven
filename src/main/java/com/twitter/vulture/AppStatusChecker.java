package com.twitter.vulture;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ThreadFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.ClientServiceDelegate;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.w3c.dom.Document;

import com.twitter.vulture.RestClient.RestException;

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
  /**
   * The interface to MRAppMaster
   */
  private ClientServiceDelegate clientService;

  // private VultureConfiguration conf;

  public AppStatusChecker(VultureConfiguration conf, AppConfiguraiton appConf,
      ApplicationReport appReport, JobID jobId,
      ClientServiceDelegate clientService) {
    this.appReport = appReport;
    this.appId = appReport.getApplicationId();
    this.jobId = jobId;
    this.clientService = clientService;
    // this.conf = conf;
    this.appConf = appConf;
  }

  @Override
  public void run() {
    // 0. set thread name
    setThreadName();
    LOG.info("Running " + Thread.currentThread().getName());
    checkTasks(TaskType.MAP);
    checkTasks(TaskType.REDUCE);
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
      LOG.error(e);
      e.printStackTrace();
    }
  }

  /**
   * Check the status of a task
   * 
   * This method can be overridden to define new policies
   * 
   * @param taskReport
   * @param currTime
   */
  protected void checkTask(TaskType taskType, TaskReport taskReport,
      long currTime) {
    boolean okTask =
        appConf.getTaskPolicy().checkTask(taskType, taskReport, appConf,
            currTime);
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
        LOG.error(e);
        e.printStackTrace();
        return;
      }
      boolean okAttempt =
          appConf.getTaskPolicy().checkTaskAttempt(taskType, taskReport,
              appConf, attemptId, taskAttemptXml);
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
   * This method can be overridden to adapt to new/old hadoop APIs
   * 
   * @param taskReport
   * @param taskAttemptId
   */
  protected void killTaskAttempt(TaskReport taskReport,
      TaskAttemptID taskAttemptId) {
    LOG.warn("KILLING " + taskAttemptId);
    try {
      clientService.killTask(taskAttemptId, true);
    } catch (IOException e) {
      LOG.warn(e);
      e.printStackTrace();
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
