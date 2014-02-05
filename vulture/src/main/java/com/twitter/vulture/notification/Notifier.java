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
package com.twitter.vulture.notification;

import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.yarn.api.records.ApplicationReport;

import com.twitter.vulture.conf.AppConfiguraiton;
import com.twitter.vulture.conf.VultureConfiguration;

public class Notifier {
  public static String DRYRUN_NOTE =
      "\n NOTE: THIS IS A DRY-RUN. NO TASK or JOB IS ACTUALLY KILLED.\n";
  public static String SUBJECT = "Vulture Notification!";
  public static boolean DRYRUN = true;

  public static void init(VultureConfiguration vConf) {
    DRYRUN = vConf.isDryRun();
  }

  public static void tooLongApp(AppConfiguraiton appConf, ApplicationReport appReport, long duration,
      long max) {
    String body = longAppMsg(appReport, duration, max);
    if (DRYRUN)
      body += DRYRUN_NOTE;
    if (appConf.getNotifyUser())
      Mail.send(SUBJECT, body, appReport.getUser());
    else
      Mail.send(SUBJECT, body);
  }

  public static void tooLongTaskAttempt(AppConfiguraiton appConf, ApplicationReport appReport,
      TaskReport taskReport, TaskAttemptID taskAttemptId, long duration,
      long max) {
    String body =
        longTaskMsg(appReport, taskReport, taskAttemptId, duration, max);
    if (DRYRUN)
      body += "\n" + DRYRUN_NOTE;
    if (appConf.getNotifyUser())
      Mail.send(SUBJECT, body, appReport.getUser());
    else
      Mail.send(SUBJECT, body);
  }

  public static String longTaskMsg(ApplicationReport appReport,
      TaskReport taskReport, TaskAttemptID taskAttemptId, long duration,
      long max) {
    String msg =
        "The task attempt " + taskAttemptId + " of  " + appReport.getName()
            + " run by " + appReport.getUser() + " did not finish after "
            + duration / 1000 + " seconds (Max is " + max / 1000 + ")\n";
    msg += "Tracking url is " + appReport.getTrackingUrl() + "\n";
    return msg;
  }

  public static String longAppMsg(ApplicationReport appReport, long duration,
      long max) {
    String msg =
        "The app " + appReport.getApplicationId() + " of  "
            + appReport.getName() + " run by " + appReport.getUser()
            + " did not finish after " + duration / 1000 + " seconds (Max is "
            + max / 1000 + ")\n";
    msg += "Tracking url is " + appReport.getTrackingUrl() + "\n";
    return msg;
  }

}
