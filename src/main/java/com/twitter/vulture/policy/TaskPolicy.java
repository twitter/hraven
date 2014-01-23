package com.twitter.vulture.policy;

import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.w3c.dom.Document;

import com.twitter.vulture.AppConfiguraiton;

public interface TaskPolicy {

  /**
   * check the status of a task
   * 
   * @param taskType
   * @param taskReport
   * @param appConf
   * @param currTime
   * @return true if task is well-behaved
   */
  public boolean checkTask(TaskType taskType, TaskReport taskReport,
      AppConfiguraiton appConf, long currTime);

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
  public boolean checkTaskAttempt(TaskType taskType, TaskReport taskReport,
      AppConfiguraiton appConf, TaskAttemptID taskAttemptId,
      Document taskAttemptXml);

}
