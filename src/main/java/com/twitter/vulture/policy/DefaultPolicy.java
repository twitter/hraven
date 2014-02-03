package com.twitter.vulture.policy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.twitter.vulture.conf.AppConfiguraiton;
import com.twitter.vulture.conf.VultureConfiguration;
import com.twitter.vulture.notification.Notifier;

public class DefaultPolicy implements AppPolicy, TaskPolicy {
  public static final Log LOG = LogFactory.getLog(DefaultPolicy.class);
  private static DefaultPolicy INSTANCE = new DefaultPolicy();

  public static DefaultPolicy getInstance() {
    return INSTANCE;
  }

  /**
   * Check the status of an application
   * 
   * @param appReport
   * @param appConf
   * @return true if the app is well-behaved
   */
  @Override
  public boolean checkAppStatus(ApplicationReport appReport,
      AppConfiguraiton appConf) {
    final long start = appReport.getStartTime();
    final long currTime = System.currentTimeMillis();
    final long duration = currTime - start;
    final long maxJobLenMs = appConf.getMaxJobLenMin() * 60 * 1000;
    if (duration > maxJobLenMs) {
      Notifier.tooLongApp(appConf, appReport, duration, maxJobLenMs);
      if (appConf.isEnforced(VultureConfiguration.JOB_MAX_LEN_MIN))
        return false;
    }
    return true;
  }

  /**
   * check the status of a task
   * 
   * @param taskType
   * @param taskReport
   * @param appConf
   * @param currTime
   * @return true if task is well-behaved
   */
  @Override
  public boolean checkTask(ApplicationReport appReport, TaskType taskType,
      TaskReport taskReport, AppConfiguraiton appConf, long currTime) {
    long startTime = taskReport.getStartTime();
    long runTime = currTime - startTime;
    long maxRunTimeMs = appConf.getMaxTaskLenMin(taskType) * 60 * 1000;
    TIPStatus tStatus = taskReport.getCurrentStatus();
    boolean badTask = (tStatus == TIPStatus.RUNNING && runTime > maxRunTimeMs);
    return !badTask;
  }

  /**
   * Check the status of an attempt of a task. This method is invoked only if
   * {@link checkTask} returns false.
   * 
   * @param taskType
   * @param taskReport
   * @param appConf
   * @param taskAttemptId
   * @param taskAttemptXml The task attempt detail in xml format
   * @return true if task attempt is well-behaved
   */
  public boolean checkTaskAttempt(ApplicationReport appReport,
      TaskType taskType, TaskReport taskReport, AppConfiguraiton appConf,
      TaskAttemptID taskAttemptId, Document taskAttemptXml) {
    long maxRunTimeMs = appConf.getMaxTaskLenMin(taskType) * 60 * 1000;
    // Iterating through the nodes and extracting the data.
    NodeList nodeList = taskAttemptXml.getDocumentElement().getChildNodes();
    for (int i = 0; i < nodeList.getLength(); i++) {
      Node node = nodeList.item(i);
      if (node instanceof Element) {
        String name = node.getNodeName();
        if (name.equals("elapsedTime")) {
          String timeStr = node.getTextContent();
          long timeMs = Long.parseLong(timeStr);
          LOG.info(name + " = " + timeMs + " ? " + maxRunTimeMs);
          boolean badTask = (timeMs > maxRunTimeMs);
          if (badTask) {
            Notifier.tooLongTaskAttempt(appConf, appReport, taskReport,
                taskAttemptId, timeMs, maxRunTimeMs);
            if (taskType == TaskType.MAP
                && appConf.isEnforced(VultureConfiguration.MAP_MAX_RUNTIME_MIN))
              return false;
            if (taskType == TaskType.REDUCE
                && appConf
                    .isEnforced(VultureConfiguration.REDUCE_MAX_RUNTIME_MIN))
              return false;
          }
          break;
        }
      }
    }
    return true;
  }
}
