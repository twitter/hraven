package com.twitter.vulture.notification;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ExitUtil;

import com.twitter.vulture.conf.VultureConfiguration;

public class Mail {
  public static final Log LOG = LogFactory.getLog(Mail.class);

  static String ADMIN;
  static String CC;

  public static void init(VultureConfiguration vConf) {
    ADMIN =
        vConf.get(VultureConfiguration.ADMIN_EMAIL,
            VultureConfiguration.DEFAULT_ADMIN_EMAIL);
    CC = vConf.get(VultureConfiguration.CC_EMAIL, ADMIN);
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
      LOG.info("Send mail to " + to);
    } catch (IOException e) {
      LOG.error(e);
    } catch (InterruptedException e) {
      LOG.error(e);
    } finally {
      if (exitValue != 0) {
        LOG.info(commands);
        ExitUtil.terminate(1, "Could not send mail: " + "exitValue was "
            + exitValue);
      }
    }
  }

  public static void main(String[] args) {
    init(new VultureConfiguration());
    if (args.length > 0) {
      ADMIN=args[0];
      CC = ADMIN;
    }
    Mail.send("Vulture Notification Test!", "nobody", ADMIN);
  }

}
