package com.twitter.vulture.conf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.twitter.vulture.policy.AppPolicy;
import com.twitter.vulture.policy.DefaultPolicy;
import com.twitter.vulture.policy.TaskPolicy;

/**
 * The conf objects can be quite big. Here we solicit the parts relevant to
 * Vulture.
 */
public class AppConfiguraiton {
  public static final Log LOG = LogFactory.getLog(AppConfiguraiton.class);
  private int maxJobLenSec;
  private int maxTaskLenSec;
  private AppPolicy appPolicy;
  private TaskPolicy taskPolicy;
  private HideExceptionConfiguration origAppConf;
  private Configuration vultureConf;

  public AppConfiguraiton(Configuration origAppConf,
      VultureConfiguration vultureConf) {
    init(new HideExceptionConfiguration(origAppConf), vultureConf);
  }

  private void init(HideExceptionConfiguration origAppConf,
      VultureConfiguration vultureConf) {
    this.origAppConf = origAppConf;
    this.vultureConf = vultureConf;
    maxJobLenSec =
        get(VultureConfiguration.MAX_JOB_LEN_SEC,
            VultureConfiguration.DEFAULT_MAX_JOB_LEN_SEC);
    maxTaskLenSec =
        get(VultureConfiguration.TASK_MAX_RUNTIME_MS,
            VultureConfiguration.DEFAULT_TASK_MAX_RUNTIME_MS);
    appPolicy = extractAppPolicy();
    taskPolicy = extractTaskPolicy();
    // Release the pointers
    this.origAppConf = null;
    this.vultureConf = null;
  }

  private AppPolicy extractAppPolicy() {
    String policyClassName =
        origAppConf.get(VultureConfiguration.APP_POLICY_CLASS);
    if (policyClassName == null)
      return DefaultPolicy.getInstance();
    Object policyObj = getPolicyObject(policyClassName);
    if (policyObj instanceof AppPolicy)
      return (AppPolicy) policyObj;
    LOG.error(policyClassName + " is not in instance of "
        + AppPolicy.class.getName());
    return DefaultPolicy.getInstance();
  }

  private TaskPolicy extractTaskPolicy() {
    String policyClassName =
        origAppConf.get(VultureConfiguration.TASK_POLICY_CLASS);
    if (policyClassName == null)
      return DefaultPolicy.getInstance();
    Object policyObj = getPolicyObject(policyClassName);
    if (policyObj instanceof TaskPolicy)
      return (TaskPolicy) policyObj;
    LOG.error(policyClassName + " is not in instance of "
        + TaskPolicy.class.getName());
    return DefaultPolicy.getInstance();
  }

  public Object getPolicyObject(String policyClassName) {
    try {
      Class<?> policyClass = Class.forName(policyClassName);
      return policyClass.newInstance();
    } catch (ClassNotFoundException e) {
      LOG.error(policyClassName + " is not in classpath!");
      e.printStackTrace();
    } catch (InstantiationException e) {
      LOG.error("Cannot instantiate " + policyClassName + "!");
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      LOG.error(policyClassName
          + " does not have a public, no-arg constructor!");
      e.printStackTrace();
    }
    LOG.warn("Using default policy instead of the configured policy: "
        + policyClassName);
    return DefaultPolicy.getInstance();
  }

  private int get(String param, int defaultValue) {
    int vultureDefault = vultureConf.getInt(param, defaultValue);
    int value = origAppConf.getInt(param, vultureDefault);
    return value;
  }

  public int getMaxJobLenSec() {
    return maxJobLenSec;
  }

  public int getMaxTaskLenSec() {
    return maxTaskLenSec;
  }

  public AppPolicy getAppPolicy() {
    return appPolicy;
  }

  public TaskPolicy getTaskPolicy() {
    return taskPolicy;
  }

  /**
   * If the source of the conf is a url, a possible scenario is that url is no
   * longer available due to app termination. To hide such exception, we wrap
   * access the original conf with this class.
   * 
   * @author myabandeh
   * 
   */
  private class HideExceptionConfiguration {
    Configuration conf;

    public HideExceptionConfiguration(Configuration conf) {
      this.conf = conf;
    }

    public String get(String param) {
      String value = null;
      try {
        value = conf.get(param);
      } catch (Exception e) {
        LOG.warn(e);
      }
      return value;
    }

    public int getInt(String param, int defaultVal) {
      int value = defaultVal;
      try {
        value = conf.getInt(param, defaultVal);
      } catch (Exception e) {
        LOG.warn(e);
      }
      return value;
    }
  }
}
