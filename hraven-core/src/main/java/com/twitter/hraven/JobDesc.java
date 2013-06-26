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


/**
 * Describes a Job at a higher level of abstraction than individual JobConf
 * entries. Jobs run from different {@code Framework}s can interpret the same
 * JobConf entries differently, or conversely, use different JobConf entries to
 * denote the same concept.
 */
public class JobDesc {

  /**
   * The combined cluster and job ID
   */
  private final QualifiedJobId jobId;

  /**
   * Who ran the final map-reduce flow on Hadoop.
   */
  private final String userName;

  /**
   * Identifying an application, which can go through different versions.
   */
  private final String appId;

  /**
   * The version of this application. An app can go through slight
   * modifications.
   */
  private final String version;

  /**
   * Identifying one single run of a version of an app. Smaller values indicate
   * a later run. We're using an inverted timestamp Long.MAXVALUE -
   * timstampMillis (milliseconds since January 1, 1970 UTC)
   */
  private final long runId;

  /**
   * Used to launch the job on the Hadoop cluster.
   */
  private final Framework framework;

  /**
   * Constructor. Used within the class hierarchy. Consider using @ {code
   * JobDescFactory} factory instead.
   * 
   * @param cluster
   *          the Hadoop cluster on which the job ran.
   * @param userName
   *          the Hadoop user name that ran a job
   * @param appId
   *          The thing that identifies an application, such as Pig script
   *          identifier, or Scalding identifier.
   * @param version
   *          The verson of this application
   * @param runId
   *          The identifier that ties the various runs for this job together
   * @param jobId
   *          The Hadoop generated MapReduce JobID.
   * @param framework
   *          used to launch the map-reduce job.
   */
  JobDesc(String cluster, String userName, String appId, String version,
      long runId, String jobId, Framework framework) {
    // TODO: Change contract to allow for nulls and advertise strings.
    this(new QualifiedJobId(cluster, jobId), userName, appId, version, runId,
        framework);
  }

  JobDesc(QualifiedJobId jobId, String user, String appId, String version,
          long runId, Framework framework) {
    this.jobId = jobId;
    this.userName = (null == user) ? Constants.UNKNOWN : user.trim();
    this.appId = (null == appId) ? Constants.UNKNOWN : appId.trim();
    this.version = (null == version) ? Constants.UNKNOWN : version.trim();
    this.runId = runId;
    this.framework = framework;
  }
  /**
   * @return The fully qualified cluster + job ID
   */
  public QualifiedJobId getQualifiedJobId() {
    return jobId;
  }

  /**
   * @return The cluster on which the job ran.
   */
  public String getCluster() {
    return jobId.getCluster();
  }

  /**
   * @return Who ran the final map-reduce flow on Hadoop.
   */
  public String getUserName() {
    return userName;
  }

  /**
   * @return The thing that identifies an application, such as Pig script
   *         identifier, or Scalding identifier.
   */
  public String getAppId() {
    return appId;
  }

  /**
   * @return Identifying one single run of a version of an app. A smaller value
   *         should indicate a later run.
   */
  public long getRunId() {
    return runId;
  }

  /**
   * @return Identifying the version of an app.
   */
  public String getVersion() {
    return version;
  }

  /**
   * @return The Hadoop map-reduce Job identifier as run on the JobTracker.
   */
  public String getJobId() {
    return jobId.getJobIdString();
  }

  /**
   * @return the famework used to launch this job with on the Hadoop cluster.
   */
  public Framework getFramework() {
    return framework;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  public String toString() {
    return getCluster() + Constants.SEP + this.userName + Constants.SEP
        + this.appId + Constants.SEP + this.version + Constants.SEP
        + this.runId + Constants.SEP + this.jobId + Constants.SEP + this.framework;
  }

}
