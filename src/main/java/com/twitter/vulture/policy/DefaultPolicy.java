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

import com.twitter.vulture.AppConfiguraiton;

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
    final long maxJobLenSec = appConf.getMaxJobLenSec();
    if (duration > maxJobLenSec * 1000) {
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
  public boolean checkTask(TaskType taskType, TaskReport taskReport,
      AppConfiguraiton appConf, long currTime) {
    long startTime = taskReport.getStartTime();
    long runTime = currTime - startTime;
    long maxRunTime = appConf.getMaxTaskLenSec();
    TIPStatus tStatus = taskReport.getCurrentStatus();
    boolean badTask = (tStatus == TIPStatus.RUNNING && runTime > maxRunTime);
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
  public boolean checkTaskAttempt(TaskType taskType, TaskReport taskReport, AppConfiguraiton appConf, 
      TaskAttemptID taskAttemptId, Document taskAttemptXml) {
    long maxRunTime = appConf.getMaxTaskLenSec();
    // Iterating through the nodes and extracting the data.
    NodeList nodeList = taskAttemptXml.getDocumentElement().getChildNodes();
    for (int i = 0; i < nodeList.getLength(); i++) {
      Node node = nodeList.item(i);
      if (node instanceof Element) {
        String name = node.getNodeName();
        if (name.equals("elapsedTime")) {
          String timeStr = node.getTextContent();
          long timeMs = Long.parseLong(timeStr);
          LOG.info(name + " = " + timeMs + " ? " + maxRunTime);
          boolean badTask = (timeMs > maxRunTime);
          return !badTask;
        }
      }
    }
    return false;
  }

}




















