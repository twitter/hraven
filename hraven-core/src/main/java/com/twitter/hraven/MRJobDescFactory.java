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
 * Used to create {@link JobKey} instances that can deal with
 * {@link Configuration} file (contents) for {@link Framework#NONE}
 * 
 */
public class MRJobDescFactory extends JobDescFactoryBase {

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.twitter.hraven.JobKeyFactoryBase#create(com.twitter.corestorage
   * .rhaven.QualifiedJobId, long, org.apache.hadoop.conf.Configuration)
   */
  JobDesc create(QualifiedJobId qualifiedJobId, long submitTimeMillis,
      Configuration jobConf) {
    // TODO: Get the actual values appropriate for the plain Hadoop jobs.

    String appId = getAppId(jobConf);

    long appSubmitTimeMillis = jobConf.getLong(Constants.MR_RUN_CONF_KEY,
        submitTimeMillis);


    return create(qualifiedJobId, jobConf, appId, Constants.UNKNOWN,
        Framework.NONE, appSubmitTimeMillis);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.twitter.hraven.JobDescFactoryBase#getAppIdFromJobName(java.lang.String)
   */
  String getAppIdFromJobName(String jobName) {
    int firstOpenBracketPos = jobName.indexOf("[");
    if (firstOpenBracketPos > -1) {
      return jobName.substring(0, firstOpenBracketPos);
    }
    return jobName;
  }

}
