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
package com.twitter.vulture.conf;

import org.apache.hadoop.conf.Configuration;

public class VultureConfiguration extends Configuration {
  private static final String VULTURE_DEFAULT_XML_FILE = "vulture-default.xml";
  private static final String VULTURE_SITE_XML_FILE = "vulture-site.xml";

  static {
    Configuration.addDefaultResource(VULTURE_DEFAULT_XML_FILE);
    Configuration.addDefaultResource(VULTURE_SITE_XML_FILE);
  }

  private volatile String[] whitelistedUsers = null;
  private volatile String[] whitelistedApps = null;

  public static final String VULTURE_PREFIX = "vulture.";

  public static final String NEW_APP_CHECKER_INTERVAL_SEC = VULTURE_PREFIX
      + "newAppChecker.interval.sec";
  public static final int DEFAULT_NEW_APP_CHECKER_INTERVAL_SEC = 5 * 60;

  public static final String NEW_APP_CHECKER_CONCURRENCY = VULTURE_PREFIX
      + "newAppChecker.concurrency";
  public static final int DEFAULT_NEW_APP_CHECKER_CONCURRENCY = 5;

  public static final String JOB_MAX_LEN_MIN = "mapreduce.job.max.runtime.mins";
  public static final int DEFAULT_JOB_MAX_LEN_MIN = 24 * 60;

  public static final String MAP_MAX_RUNTIME_MIN =
      "mapreduce.map.max.runtime.mins";
  public static final int DEFAULT_MAP_MAX_RUNTIME_MIN = 5 * 60;
  public static final String REDUCE_MAX_RUNTIME_MIN =
      "mapreduce.reduce.max.runtime.mins";
  public static final int DEFAULT_REDUCE_MAX_RUNTIME_MIN = 5 * 60;

  public static final String MAX_CACHED_APP_CONFS = VULTURE_PREFIX
      + "confCache.max.size";
  public static final int DEFAULT_MAX_CACHED_APP_CONFS = 10000;

  public static final String APP_POLICY_CLASS = VULTURE_PREFIX + "app.policy";
  public static final String TASK_POLICY_CLASS = VULTURE_PREFIX + "task.policy";

  public static final String DRY_RUN = VULTURE_PREFIX + "dryRun";
  public static final boolean DEFAULT_DRY_RUN = true;

  public static final String ADMIN_EMAIL = VULTURE_PREFIX + "admin.email";
  public static final String DEFAULT_ADMIN_EMAIL = "myabandeh@twitter.com";

  public static final String NOTIFY_USER = VULTURE_PREFIX + "notify.user";
  public static final boolean DEFAULT_NOTIFY_USER = false;

  public static final String CC_EMAIL = VULTURE_PREFIX + "cc.email";

  public static final String WHITELIST_USERS = VULTURE_PREFIX
      + "whitelist.users";
  public static final String WHITELIST_APPS = VULTURE_PREFIX + "whitelist.apps";

  public static final boolean DEFAULT_ENFORCE = false;

  public static String enforced(String paramName) {
    return paramName + ".enforce";
  }

  public boolean isDryRun() {
    boolean dryRun = getBoolean(DRY_RUN, DEFAULT_DRY_RUN);
    return dryRun;
  }

  public synchronized boolean isUserInWhitelist(String user) {
    if (whitelistedUsers == null) {
      String listStr = get(WHITELIST_USERS);
      if (listStr == null)
        whitelistedUsers = new String[0];
      else
        whitelistedUsers = listStr.split(",");
    }
    for (String whiteListedUser : whitelistedUsers)
      if (whiteListedUser.trim().equals(user))
        return true;
    return false;
  }

  public synchronized boolean isAppInWhitelist(String appId) {
    if (whitelistedApps == null) {
      String listStr = get(WHITELIST_APPS);
      if (listStr == null)
        whitelistedApps = new String[0];
      else
        whitelistedApps = listStr.split(",");
    }
    for (String whitelistedApp : whitelistedApps)
      if (whitelistedApp.trim().equals(appId))
        return true;
    return false;
  }
}
