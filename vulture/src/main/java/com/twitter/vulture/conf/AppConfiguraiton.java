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

import java.util.Enumeration;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskType;

import com.twitter.vulture.policy.AppPolicy;
import com.twitter.vulture.policy.DefaultPolicy;
import com.twitter.vulture.policy.TaskPolicy;

import static com.twitter.vulture.conf.VultureConfiguration.*;

/**
 * The conf objects can be quite big. Here we solicit the parts relevant to
 * Vulture.
 */
public class AppConfiguraiton extends Configuration {
  public static final Log LOG = LogFactory.getLog(AppConfiguraiton.class);
  private AppPolicy appPolicy;
  private TaskPolicy taskPolicy;
  private Configuration vultureConf;

  public AppConfiguraiton(Configuration origAppConf,
      VultureConfiguration vultureConf) {
    super(false);// don't load default resources
    init(new HideExceptionConfiguration(origAppConf), vultureConf);
  }

  private void init(HideExceptionConfiguration origAppConf,
      VultureConfiguration vultureConf) {
    this.vultureConf = vultureConf;
    origAppConf.loadPropsTo(this, VultureConfiguration.VULTURE_PREFIX);
    origAppConf.loadPropsTo(this, VultureConfiguration.JOB_MAX_LEN_MIN);
    origAppConf.loadPropsTo(this, VultureConfiguration.MAP_MAX_RUNTIME_MIN);
    origAppConf.loadPropsTo(this, VultureConfiguration.REDUCE_MAX_RUNTIME_MIN);
    appPolicy = extractAppPolicy(origAppConf);
    taskPolicy = extractTaskPolicy(origAppConf);
  }

  private AppPolicy extractAppPolicy(HideExceptionConfiguration origAppConf) {
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

  private TaskPolicy extractTaskPolicy(HideExceptionConfiguration origAppConf) {
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

  public int getInt(String param, int defaultValue) {
    int vultureDefault = vultureConf.getInt(param, defaultValue);
    int value = super.getInt(param, vultureDefault);
    return value;
  }

  public boolean getBoolean(String param, boolean defaultValue) {
    boolean vultureDefault = vultureConf.getBoolean(param, defaultValue);
    boolean value = super.getBoolean(param, vultureDefault);
    return value;
  }

  public int getMaxJobLenMin() {
    return getInt(VultureConfiguration.JOB_MAX_LEN_MIN,
        VultureConfiguration.DEFAULT_JOB_MAX_LEN_MIN);
  }

  public int getMaxTaskLenMin(TaskType taskType) {
    switch (taskType) {
    case MAP:
      return getMaxMapLenMin();
    case REDUCE:
      return getMaxReduceLenMin();
    default:
      LOG.error("Unknow task type: " + taskType);
      return Integer.MAX_VALUE;
    }
  }

  public int getMaxMapLenMin() {
    return getInt(VultureConfiguration.MAP_MAX_RUNTIME_MIN,
        VultureConfiguration.DEFAULT_MAP_MAX_RUNTIME_MIN);
  }

  public int getMaxReduceLenMin() {
    return getInt(VultureConfiguration.REDUCE_MAX_RUNTIME_MIN,
        VultureConfiguration.DEFAULT_REDUCE_MAX_RUNTIME_MIN);
  }

  public boolean getNotifyUser() {
    return getBoolean(VultureConfiguration.NOTIFY_USER,
        VultureConfiguration.DEFAULT_NOTIFY_USER);
  }

  public AppPolicy getAppPolicy() {
    return appPolicy;
  }

  public TaskPolicy getTaskPolicy() {
    return taskPolicy;
  }

  public boolean isEnforced(String param) {
    boolean enforced =
        getBoolean(enforced(param), VultureConfiguration.DEFAULT_ENFORCE);
    return enforced;
  }

  /**
   * If the source of the conf is a url, a possible scenario is that url is no
   * longer available due to app termination. To hide such exception, we wrap
   * access the original conf with this class.
   */
  private class HideExceptionConfiguration {
    FilterableConfiguration conf;

    public HideExceptionConfiguration(Configuration conf) {
      this.conf = new FilterableConfiguration(conf);
    }

    void loadPropsTo(Configuration newConf, String prefix) {
      try {
        conf.loadPropsTo(newConf, prefix);
      } catch (Exception e) {
        LOG.warn("Error in accessing remote conf", e);
      }
    }

    public String get(String param) {
      String value = null;
      try {
        value = conf.get(param);
      } catch (Exception e) {
        LOG.warn("Error in accessing remote conf", e);
      }
      return value;
    }

  }

  private class FilterableConfiguration extends Configuration {
    FilterableConfiguration(Configuration conf) {
      super(conf);
    }

    void loadPropsTo(Configuration newConf, String prefix) {
      Properties props = this.getProps();
      Enumeration<?> iter = props.propertyNames();
      while (iter.hasMoreElements()) {
        String name = (String) iter.nextElement();
        if (name.startsWith(prefix)) {
          String value = props.getProperty(name);
          newConf.set(name, value);
        }
      }
    }
  }

}
