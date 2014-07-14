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
package com.twitter.hraven.hadoopJobMonitor.notification;

import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.yarn.api.records.ApplicationReport;

import com.twitter.hraven.hadoopJobMonitor.conf.AppConfiguraiton;
import com.twitter.hraven.hadoopJobMonitor.conf.HadoopJobMonitorConfiguration;
import com.twitter.hraven.hadoopJobMonitor.metrics.HadoopJobMonitorMetrics;

public class Notifier {
  public static String DRYRUN_NOTE =
      "\n NOTE: THIS IS A DRY-RUN. NO TASK or JOB IS ACTUALLY KILLED.\n";
  public static String SUBJECT = "HadoopJobMonitor Notification!";
  public static boolean DRYRUN = true;
  private static HadoopJobMonitorMetrics metrics = HadoopJobMonitorMetrics.getInstance();

  public static void init(HadoopJobMonitorConfiguration vConf) {
    DRYRUN = vConf.isDryRun();
  }

  public static String tooLongApp(AppConfiguraiton appConf, ApplicationReport appReport, long duration,
      long max) {
    metrics.badBehavedApps.incr();
    String body = longAppMsg(appReport, duration, max);
    if (DRYRUN)
      body += DRYRUN_NOTE;
    if (appConf.getNotifyUser())
      Mail.send(SUBJECT, body, getUserEmail(appConf, appReport));
    else
      Mail.send(SUBJECT, body);
    return body;
  }

  public static String tooLongTaskAttempt(AppConfiguraiton appConf, ApplicationReport appReport,
      TaskReport taskReport, TaskType taskType, TaskAttemptID taskAttemptId, long duration,
      long max) {
    metrics.badBehavedTasks.incr();
    if (taskType.equals(TaskType.MAP))
      metrics.badBehavedMappers.incr();
    else if (taskType.equals(TaskType.REDUCE))
      metrics.badBehavedReducers.incr();
    String body =
        longTaskMsg(appReport, taskReport, taskAttemptId, duration, max);
    if (DRYRUN)
      body += "\n" + DRYRUN_NOTE;
    if (appConf.getNotifyUser())
      Mail.send(SUBJECT, body, getUserEmail(appConf, appReport));
    else
      Mail.send(SUBJECT, body);
    return body;
  }

  public static String tooLittleProgress(AppConfiguraiton appConf,
      ApplicationReport appReport, TaskReport taskReport, TaskType taskType,
      TaskAttemptID taskAttemptId, float progress, float progressThreshold,
      long max) {
    metrics.badBehavedTasks.incr();
    if (taskType.equals(TaskType.MAP))
      metrics.badBehavedMappers.incr();
    else if (taskType.equals(TaskType.REDUCE))
      metrics.badBehavedReducers.incr();
    String body =
        littleProgressMsg(appReport, taskReport, taskAttemptId, progress,
            progressThreshold, max);
    if (DRYRUN)
      body += "\n" + DRYRUN_NOTE;
    if (appConf.getNotifyUser())
      Mail.send(SUBJECT, body, getUserEmail(appConf, appReport));
    else
      Mail.send(SUBJECT, body);
    return body;
  }

  public static String littleProgressMsg(ApplicationReport appReport,
      TaskReport taskReport, TaskAttemptID taskAttemptId, float progress,
      float progressThreshold, long max) {
    String msg =
        "The task attempt " + taskAttemptId + " of  " + appReport.getName()
            + " run by " + appReport.getUser()
            + " did not have enough progress: total progress is " + progress
            + ", lagged progress threshold is " + progressThreshold
            + " and max run time is " + durationStr(max) + " hours\n";
    msg += "Tracking url is " + appReport.getTrackingUrl() + "\n";
    return msg;
  }

  public static String longTaskMsg(ApplicationReport appReport,
      TaskReport taskReport, TaskAttemptID taskAttemptId, long duration,
      long max) {
    String msg =
        "The task attempt " + taskAttemptId + " of  " + appReport.getName()
            + " run by " + appReport.getUser() + " did not finish after "
            + durationStr(duration) + " hours (Max is " + durationStr(max) + ")\n";
    msg += "Tracking url is " + appReport.getTrackingUrl() + "\n";
    return msg;
  }

  public static String longAppMsg(ApplicationReport appReport, long duration,
      long max) {
    String msg =
        "The app " + appReport.getApplicationId() + " of  "
            + appReport.getName() + " run by " + appReport.getUser()
            + " did not finish after " + durationStr(duration) + " hours (Max is "
            + durationStr(max) + ")\n";
    msg += "Tracking url is " + appReport.getTrackingUrl() + "\n";
    return msg;
  }
  
  public static String getUserEmail(AppConfiguraiton appConf, ApplicationReport appReport) {
    String userEmails = appConf.get(HadoopJobMonitorConfiguration.USER_EMAIL);
    if (userEmails != null)
      return userEmails;
    else
      return appReport.getUser();
  }
  
  public static String durationStr(long ms) {
    long s = ms / 1000;
    String duration = String.format("%d:%02d:%02d", s/3600, (s%3600)/60, (s%60));
    return duration;
  }

}
