package com.twitter.vulture.policy;

import org.apache.hadoop.yarn.api.records.ApplicationReport;

import com.twitter.vulture.conf.AppConfiguraiton;

public interface AppPolicy {

  /**
   * Check the status of an application
   * @param appReport
   * @param appConf
   * @return true if the app is well-behaved
   */
  public boolean checkAppStatus(ApplicationReport appReport,
      AppConfiguraiton appConf);

}
