package com.twitter.vulture.conf;

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

  public static final String NEW_APP_CHECKER_CONCURRENCY = VULTURE_PREFIX
      + "newAppChecker.concurrency";
  public static final int DEFAULT_NEW_APP_CHECKER_CONCURRENCY = 5;

  public static final String TASK_MAX_RUNTIME_MS = VULTURE_PREFIX
      + "task.max.runtime.ms";
  public static final int DEFAULT_TASK_MAX_RUNTIME_MS = 60 * 60 * 1000;

  public static final String MAX_JOB_LEN_SEC = VULTURE_PREFIX
      + "job.max.len.sec";
  public static final int DEFAULT_MAX_JOB_LEN_SEC = 12 * 60 * 60;

  public static final String MAX_CACHED_APP_CONFS = VULTURE_PREFIX
      + "confCache.max.size";
  public static final int DEFAULT_MAX_CACHED_APP_CONFS = 10000;

  public static final String APP_POLICY_CLASS = VULTURE_PREFIX + "app.policy";
  public static final String TASK_POLICY_CLASS = VULTURE_PREFIX + "task.policy";

  public static final String DRY_RUN = VULTURE_PREFIX + "dryRun";
  public static final boolean DEFAULT_DRY_RUN = true;

  public static final String ADMIN_EMAIL = VULTURE_PREFIX + "admin.email";
  public static final String DEFAULT_ADMIN_EMAIL = "myabandeh@twitter.com";

  public static final String CC_EMAIL = VULTURE_PREFIX + "cc.email";

  public boolean isDryRun() {
    boolean dryRun = 
        getBoolean(DRY_RUN, DEFAULT_DRY_RUN);
    return dryRun;
  }
}
