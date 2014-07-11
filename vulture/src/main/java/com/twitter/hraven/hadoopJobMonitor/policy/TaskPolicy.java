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

import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.w3c.dom.Document;

import com.twitter.hraven.hadoopJobMonitor.conf.AppConfiguraiton;

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
  public boolean checkTask(ApplicationReport appReport, TaskType taskType, TaskReport taskReport,
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
   * @return null if task attempt is well-behaved, otherwise the error msg
   */
  public String checkTaskAttempt(ApplicationReport appReport, TaskType taskType, TaskReport taskReport,
      AppConfiguraiton appConf, TaskAttemptID taskAttemptId,
      Document taskAttemptXml, long currTime);
}
