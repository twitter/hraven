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

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ExitUtil;

import com.twitter.hraven.hadoopJobMonitor.conf.HadoopJobMonitorConfiguration;

public class Mail {
  public static final Log LOG = LogFactory.getLog(Mail.class);

  static String ADMIN;
  static String CC;

  public static void init(HadoopJobMonitorConfiguration vConf) {
    ADMIN =
        vConf.get(HadoopJobMonitorConfiguration.ADMIN_EMAIL,
            HadoopJobMonitorConfiguration.DEFAULT_ADMIN_EMAIL);
    CC = vConf.get(HadoopJobMonitorConfiguration.CC_EMAIL, ADMIN);
  }

  public static void send(String subject, String msg) {
    send(subject, msg, ADMIN);
  }

  public static void send(String subject, String msg, String to) {
    List<String> commands = new LinkedList<String>();
    commands.add("/bin/bash");
    commands.add("-c");
    commands.add("echo \"" + msg + "\" | mail -s \"" + subject + "\" " + to + ","
        + CC);
    //This option is not supported by all mail clients: + " -c " + CC);
    //This option is not supported by all mail clients: + " -- -f " + ADMIN);
    ProcessBuilder pb = new ProcessBuilder(commands);
    Process p;
    int exitValue = -1;
    try {
      p = pb.start();
      exitValue = p.waitFor();
      LOG.info("Send email to " + to + " exitValue is " + exitValue);
    } catch (IOException e) {
      LOG.error("Error in executing mail command", e);
    } catch (InterruptedException e) {
      LOG.error("Error in executing mail command", e);
    } finally {
      if (exitValue != 0) {
        LOG.fatal(commands);
//        ExitUtil.terminate(1, "Could not send mail: " + "exitValue was "
//            + exitValue);
      }
    }
  }

  public static void main(String[] args) {
    init(new HadoopJobMonitorConfiguration());
    if (args.length > 0) {
      ADMIN=args[0];
      CC = ADMIN;
    }
    Mail.send("HadoopJobMonitor Notification Test!", "nobody", ADMIN);
  }

}
