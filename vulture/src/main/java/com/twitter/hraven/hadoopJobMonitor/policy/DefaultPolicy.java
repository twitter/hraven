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
package com.twitter.hraven.hadoopJobMonitor.policy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.twitter.hraven.hadoopJobMonitor.conf.AppConfiguraiton;
import com.twitter.hraven.hadoopJobMonitor.conf.VultureConfiguration;
import com.twitter.hraven.hadoopJobMonitor.notification.Notifier;
import com.twitter.hraven.hadoopJobMonitor.policy.ProgressCache.Progress;

public class DefaultPolicy implements AppPolicy, TaskPolicy {
  public static final Log LOG = LogFactory.getLog(DefaultPolicy.class);
  private static final int MINtoMS = 60 * 1000;
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
  public String checkAppStatus(ApplicationReport appReport,
      AppConfiguraiton appConf) {
    final long start = appReport.getStartTime();
    final long currTime = System.currentTimeMillis();
    final long duration = currTime - start;
    final long maxJobLenMs = appConf.getMaxJobLenMin() * 60 * 1000;
    if (duration > maxJobLenMs) {
      String errMsg = Notifier.tooLongApp(appConf, appReport, duration, maxJobLenMs);
      if (appConf.isEnforced(VultureConfiguration.JOB_MAX_LEN_MIN))
        return errMsg;
    }
    return null;
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
    if (badTask)
      return !badTask;
    badTask =
        ! checkProgress(taskReport.getProgress(), appConf.getProgressThreshold(),
            maxRunTimeMs, taskReport.getTaskID(), currTime);
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
  @Override
  public String checkTaskAttempt(ApplicationReport appReport,
      TaskType taskType, TaskReport taskReport, AppConfiguraiton appConf,
      TaskAttemptID taskAttemptId, Document taskAttemptXml, long currTime) {
    long maxRunTimeMs = appConf.getMaxTaskLenMin(taskType) * MINtoMS;
    // Iterating through the nodes and extracting the data.
    NodeList nodeList = taskAttemptXml.getDocumentElement().getChildNodes();
    int checkedNodes = 0, maxRequiredNodes = 2;//elapsedtime and progress
    for (int i = 0; i < nodeList.getLength(); i++) {
      Node node = nodeList.item(i);
      if (node instanceof Element) {
        String name = node.getNodeName();
        //check duration
        if (name.equals("elapsedTime")) {
          checkedNodes++;
          String timeStr = node.getTextContent();
          long timeMs = Long.parseLong(timeStr);
          LOG.debug(name + " = " + timeMs + " ? " + maxRunTimeMs);
          boolean badTask = (timeMs > maxRunTimeMs);
          if (badTask) {
            String msg = Notifier.tooLongTaskAttempt(appConf, appReport, taskReport,
                taskType, taskAttemptId, timeMs, maxRunTimeMs);
            if (taskType == TaskType.MAP
                && appConf.isEnforced(VultureConfiguration.MAP_MAX_RUNTIME_MIN))
              return msg;
            if (taskType == TaskType.REDUCE
                && appConf
                    .isEnforced(VultureConfiguration.REDUCE_MAX_RUNTIME_MIN))
              return msg;
          }
          if (checkedNodes >= maxRequiredNodes)
            break;
          continue;
        }
        //check progress
        if (name.equals("progress")) {
          checkedNodes++;
          String progressStr = node.getTextContent();
          float progress = Float.parseFloat(progressStr);
          boolean badTask =
              ! checkProgress(progress, appConf.getProgressThreshold(),
                  maxRunTimeMs, taskAttemptId, currTime);
          if (badTask) {
            String msg = Notifier.tooLittleProgress(appConf, appReport, taskReport,
                taskType, taskAttemptId, progress,
                appConf.getProgressThreshold(), maxRunTimeMs);
            if (taskType == TaskType.MAP
                && appConf.isEnforced(VultureConfiguration.MAP_MAX_RUNTIME_MIN))
              return msg;
            if (taskType == TaskType.REDUCE
                && appConf
                    .isEnforced(VultureConfiguration.REDUCE_MAX_RUNTIME_MIN))
              return msg;
          }
          if (checkedNodes >= maxRequiredNodes)
            break;
          continue;
        }
      }
    }
    return null;
  }

  boolean checkProgress(float progress, float progressThreshold,
      long expectedRuntime, TaskID taskId, long currTime) {
    Progress lastProgress = ProgressCache.getTaskProgressCache().get(taskId);
    if (lastProgress == null) {
      ProgressCache.getTaskProgressCache().put(taskId,
          new Progress(progress, currTime));
      return true; // nothing to compare with
    }
    return checkProgress(progress, progressThreshold, expectedRuntime,
        lastProgress, currTime);
  }
  
  boolean checkProgress(float progress, float progressThreshold,
      long expectedRuntime, TaskAttemptID taskAttemptId, long currTime) {
    Progress lastProgress = ProgressCache.getAttemptProgressCache().get(taskAttemptId);
    if (lastProgress == null) {
      ProgressCache.getAttemptProgressCache().put(taskAttemptId,
          new Progress(progress, currTime));
      return true; // nothing to compare with
    }
    return checkProgress(progress, progressThreshold, expectedRuntime,
        lastProgress, currTime);
  }

  boolean checkProgress(float progress, float progressThreshold,
      long expectedRuntime, Progress lastProgress, long currTime) {
    try {
      progress = progress - lastProgress.value;
      if (progress < 0) // happens after failures
        return true;
      long duration = currTime - lastProgress.dateMs;
      float expectedProgress = duration / (float) expectedRuntime;
      float laggedProgress = expectedProgress - progress;
      LOG.debug(" totalProgress = " + progress + " laggedProgress = "
          + laggedProgress + " threshold = " + progressThreshold);
      return laggedProgress <= progressThreshold;
    } finally {
      synchronized (lastProgress) {
        lastProgress.dateMs = currTime;
        lastProgress.value = progress;
      }
    }
  }
  
/*
  public boolean checkProgress(TaskType taskType, TaskReport taskReport,
      AppConfiguraiton appConf, long currTime) {
    TIPStatus tStatus = taskReport.getCurrentStatus();
    // progress does not make much sense when the task is not running
    if (tStatus != TIPStatus.RUNNING)
      return true;
    float progress = taskReport.getProgress();
    Progress lastProgress =
        TaskProgressCache.getInstance().get(taskReport.getTaskId());
    if (lastProgress == null) {
      TaskProgressCache.getInstance().put(taskReport.getTaskId(),
          new Progress(progress, currTime));
      return true; // nothing to compare with
    }
    try {
      progress = progress - lastProgress.value;
      if (progress < 0) // happens after failures
        return true;
      long duration = currTime - lastProgress.dateMs;
      long expectedRuntime = appConf.getMaxTaskLenMin(taskType) * MINtoMS;
      float expectedProgress = duration / (float) expectedRuntime;
      float laggedProgress = expectedProgress - progress;
      return (laggedProgress <= appConf.getProgressThreshold());
    } finally {
      synchronized (lastProgress) {
        lastProgress.dateMs = currTime;
        lastProgress.value = progress;
      }
    }
  }
*/
}
