package com.twitter.vulture;

import org.apache.hadoop.conf.Configuration;

public class VultureConfiguration extends Configuration {
  private static final String VULTURE_DEFAULT_XML_FILE = "vulture-default.xml";
  private static final String VULTURE_SITE_XML_FILE = "vulture-site.xml";

  static {
    Configuration.addDefaultResource(VULTURE_DEFAULT_XML_FILE);
    Configuration.addDefaultResource(VULTURE_SITE_XML_FILE);
  }

  public static final String VULTURE_PREFIX = "vulture.";

  public static final String NEW_APP_CHECKER_INTERVAL_SEC = VULTURE_PREFIX
      + "newAppChecker.interval.sec";
  public static final int DEFAULT_NEW_APP_CHECKER_INTERVAL_SEC = 5 * 60;

  public static final String TASK_MAX_RUNTIME_MS = VULTURE_PREFIX
      + "taks.max.runtime.ms";
  public static final int DEFAULT_TASK_MAX_RUNTIME_MS = 20 * 60 * 1000;

  public static final String MAX_JOB_LEN_SEC = VULTURE_PREFIX
      + "job.max.len.sec";
  public static final int DEFAULT_MAX_JOB_LEN_SEC = 12 * 60 * 60;

}
