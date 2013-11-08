/*
Copyright 2012 Twitter, Inc.

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
package com.twitter.hraven;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.twitter.hraven.util.StringUtil;

/**
 * Provide functionality that is common to the various JobKeyFactory classes.
 */
public abstract class JobDescFactoryBase {

  /**
   * Default constructor.
   */
  public JobDescFactoryBase() {
  }

  /**
   * @param qualifiedJobId
   *          jobId qualified with cluster.
   * @param submitTimeMillis
   *          the job, script or flow submit time in milliseconds since January
   *          1, 1970 UTC
   * @param jobConf
   *          of the job.
   * @return the identifier for the job in the JobHistory table.
   */
  abstract JobDesc create(QualifiedJobId qualifiedJobId, long submitTimeMillis,
      Configuration jobConf);

  /**
   * Factory method to be used by subclasses.
   * 
   * @param qualifiedJobId
   *          Identifying the cluster and the jobId. Cannot be null;
   * @param jobConf
   *          the Job configuration of the job
   * @param appId
   *          The thing that identifies an application, such as Pig script
   *          identifier, or Scalding identifier.
   * @param version
   * @param framework used to launch the map-reduce job.
   * @param submitTimeMillis
   *          Identifying one single run of a version of an app.
   * @return a JobKey with the given parameters and the userName added.
   */
  protected JobDesc create(QualifiedJobId qualifiedJobId, Configuration jobConf,
      String appId, String version, Framework framework, long submitTimeMillis) {

    if (null == qualifiedJobId) {
      throw new IllegalArgumentException(
          "Cannot create a JobKey from a null qualifiedJobId.");
    }

    /**
     *  Add user name from the job conf
     *  check for hadoop2 config param, then hadoop1
     */
    String userName = jobConf.get(Constants.USER_CONF_KEY_HADOOP2);
    if (StringUtils.isBlank(userName)) {
      userName = jobConf.get(Constants.USER_CONF_KEY);
    }

    return new JobDesc(qualifiedJobId, userName, appId, version,
        submitTimeMillis, framework);
  }

  /**
   * @param jobConf
   *          from which to pull the properties
   * @return a non-empty non-null string with the jobId. If the jobId cannot be
   *         parsed, then {@link Constants#UNKNOWN} will be returned.
   */
  protected String getAppId(Configuration jobConf) {

    // Defensive coding
    if (jobConf == null) {
      return Constants.UNKNOWN;
    }

    String appId = jobConf.get(Constants.APP_NAME_CONF_KEY);

    // If explicit app name isn't set, try to parse it from mapred.job.name
    if (appId == null) {
      appId = jobConf.get(Constants.JOB_NAME_CONF_KEY);
      if (appId != null) {
        // Allow sub-classes to transform.
        appId = getAppIdFromJobName(appId);
      }
    }

    return cleanAppId(appId);
  }

  /**
   * Given a potential value for appId, return a string that is safe to use in
   * the jobKey
   * 
   * @param appId
   *          possibly null value.
   * @return non-null value stripped of separators that are used in the jobKey.
   */
  protected String cleanAppId(String appId) {
    return (appId != null) ? StringUtil.cleanseToken(appId) : Constants.UNKNOWN;
  }

  /**
   * Subclasses are to implement this method to strip the jobId from a jobName.
   * This allows separate implementations to treat the name differently.
   * 
   * @param jobName
   *          on-null name of the job
   * @return the AppId given the jobName. Note that delimiters and spaces will
   *         be stripped from the result to avoid clashes in the key.
   */
  abstract String getAppIdFromJobName(String jobName);

}
