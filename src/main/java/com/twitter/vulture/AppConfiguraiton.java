package com.twitter.vulture;

import org.apache.hadoop.conf.Configuration;

/**
 * The conf objects can be quite big. Here we solicit the parts relevant to
 * Vulture.
 */
public class AppConfiguraiton {
  private int maxJobLenSec;
  private int maxTaskLenSec;
  private Configuration origAppConf;
  private Configuration vultureConf;

  public AppConfiguraiton(Configuration origAppConf,
      VultureConfiguration vultureConf) {
    init(origAppConf, vultureConf);
  }

  private void init(Configuration origAppConf, VultureConfiguration vultureConf) {
    this.origAppConf = origAppConf;
    this.vultureConf = vultureConf;
    maxJobLenSec =
        get(VultureConfiguration.MAX_JOB_LEN_SEC,
            VultureConfiguration.DEFAULT_MAX_JOB_LEN_SEC);
    maxTaskLenSec =
        get(VultureConfiguration.TASK_MAX_RUNTIME_MS,
            VultureConfiguration.DEFAULT_TASK_MAX_RUNTIME_MS);
  }

  private int get(String param, int defaultValue) {
    int vultureDefault = vultureConf.getInt(param, defaultValue);
    int value = origAppConf.getInt(param, vultureDefault);
    return value;
  }

  public int getMaxJobLenSec() {
    return maxJobLenSec;
  }

  public void setMaxJobLenSec(int maxJobLenSec) {
    this.maxJobLenSec = maxJobLenSec;
  }

  public int getMaxTaskLenSec() {
    return maxTaskLenSec;
  }

  public void setMaxTaskLenSec(int maxTaskLenSec) {
    this.maxTaskLenSec = maxTaskLenSec;
  }
}
