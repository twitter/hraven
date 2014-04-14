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

import org.apache.hadoop.conf.Configuration;

/**
 * Deal with {@link JobDesc} implementations.
 */
public class JobDescFactory {
  /** Key used to identify the jobtracker host in job configurations. */
  public static final String JOBTRACKER_KEY = "mapred.job.tracker";
  public static final String RESOURCE_MANAGER_KEY = "yarn.resourcemanager.address";

  public static JobDescFactoryBase getJobDescFactory(Configuration jobConf) {
    Framework framework = getFramework(jobConf);

    switch (framework) {
    case PIG:
      return new PigJobDescFactory();
    case SCALDING:
      return new ScaldingJobDescFactory();
    default:
      return new MRJobDescFactory();
    }
  }

  /**
   * @param submitTimeMillis
   * @param qualifiedJobId
   *          Identifier for the job for the given {@link Configuration}
   * @param jobConf
   *          the jobConf for the given job.
   * @return the job description for the given JobConfiguration.
   */
  public static JobDesc createJobDesc(QualifiedJobId qualifiedJobId,
      long submitTimeMillis, Configuration jobConf) {
    return getJobDescFactory(jobConf).create(qualifiedJobId, submitTimeMillis,
        jobConf);
  }

  /**
   * @param jobConf
   *          a given job configuration.
   * @return which framerwork was used to launch that configuration.
   */
  public static Framework getFramework(Configuration jobConf) {
    // Check if this is a pig job
    boolean isPig = jobConf.get(Constants.PIG_CONF_KEY) != null;
    if (isPig) {
      return Framework.PIG;
    } else {
      String flowId = jobConf.get(Constants.CASCADING_FLOW_ID_CONF_KEY);
      if ((flowId == null) || (flowId.length() == 0)) {
        return Framework.NONE;
      } else {
        return Framework.SCALDING;
      }
    }
  }

  /**
   * Returns the cluster that a give job was run on by mapping the jobtracker hostname to an
   * identifier.
   * @param jobConf
   * @return
   */
  public static String getCluster(Configuration jobConf) {
    String jobtracker = jobConf.get(JOBTRACKER_KEY);
    if (jobtracker == null) {
      jobtracker = jobConf.get(RESOURCE_MANAGER_KEY);
    }
    String cluster = null;
    if (jobtracker != null) {
      // strip any port number
      int portIdx = jobtracker.indexOf(':');
      if (portIdx > -1) {
        jobtracker = jobtracker.substring(0, portIdx);
      }
      // An ExceptionInInitializerError may be thrown to indicate that an exception occurred during
      // evaluation of Cluster class' static initialization
      cluster = Cluster.getIdentifier(jobtracker);
    }
    return cluster;
  }

}
