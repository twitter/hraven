/*
Copyright 2014 Twitter, Inc.

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
 * Used to {@link JobKey} instances that can deal with {@link Configuration}
 * file (contents) for {@link Framework#SPARK}
 */
public class SparkJobDescFactory extends JobDescFactoryBase {

  /**
   * creates a JobDesc obj based on the framework
   */
  @Override
  JobDesc create(QualifiedJobId qualifiedJobId, long submitTimeMillis,
      Configuration jobConf) {

    // for spark jobs, app id is the same as jobname
    String appId =  jobConf.get(Constants.JOB_NAME_HADOOP2_CONF_KEY,
        Constants.UNKNOWN);

    if (Constants.UNKNOWN.equals(appId)) {
      // Fall back to job id, should not happen
      appId = qualifiedJobId.getJobIdString();
    }

    String version = jobConf.get(Constants.SPARK_VERSION_CONF_KEY,
        Constants.UNKNOWN);

    return create(qualifiedJobId, jobConf, appId, version,
        Framework.SPARK, submitTimeMillis);
  }

  @Override
  String getAppIdFromJobName(String jobName) {
    // for spark jobs, app id is the same as jobname
    return jobName;
  }
}

