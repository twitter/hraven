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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;

/**
 * Used to {@link JobKey} instances that can deal with {@link Configuration}
 * file (contents) for {@link Framework#PIG}
 */
public class PigJobDescFactory extends JobDescFactoryBase {

  private static Pattern scheduledJobnamePattern = Pattern
      .compile(Constants.PIG_SCHEDULED_JOBNAME_PATTERN_REGEX);
  private static Pattern pigLogfilePattern = Pattern
      .compile(Constants.PIG_LOGFILE_PATTERN_REGEX);

  // TODO: Make this configurable
  public static final String SCHEDULED_PREFIX = "oink ";

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.twitter.hraven.JobKeyFactoryBase#create(com.twitter.corestorage
   * .rhaven.QualifiedJobId, long, org.apache.hadoop.conf.Configuration)
   */
  public JobDesc create(QualifiedJobId qualifiedJobId, long submitTimeMillis,
      Configuration jobConf) {
    String appId = getAppId(jobConf);
    String version = jobConf.get(Constants.PIG_VERSION_CONF_KEY,
        Constants.UNKNOWN);
    long pigSubmitTimeMillis = jobConf.getLong(Constants.PIG_RUN_CONF_KEY, 0);

    // This means that Constants.PIG_RUN_CONF_KEY was not present (for jobs
    // launched with an older pig version).
    if (pigSubmitTimeMillis == 0) {
      String pigLogfile = jobConf.get(Constants.PIG_LOG_FILE_CONF_KEY);
      if (pigLogfile == null) {
        // Should be rare, but we're seeing this happen occasionally
        // Give up on grouping the jobs within the run together, and treat these as individual runs.
        pigSubmitTimeMillis = submitTimeMillis;
      } else {
        pigSubmitTimeMillis = getScriptStartTimeFromLogfileName(pigLogfile);
      }
    }

    return create(qualifiedJobId, jobConf, appId, version, Framework.PIG,
        pigSubmitTimeMillis);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.twitter.hraven.JobDescFactoryBase#getAppIdFromJobName(java.lang.String)
   */
  String getAppIdFromJobName(String jobName) {
    if (jobName == null) {
      return null;
    }

    Matcher matcher = scheduledJobnamePattern.matcher(jobName);

    // TODO: Externalize patterns to make them configurable
    if (matcher.matches()) {
      jobName = SCHEDULED_PREFIX + matcher.group(1);
    }

    return jobName;
  }

  /**
   * @param pigLogfile
   *          as obtained from the JobConfig
   * @return
   */
  public static long getScriptStartTimeFromLogfileName(String pigLogfile) {
    long pigSubmitTimeMillis = 0;

    if (pigLogfile == null) {
      return pigSubmitTimeMillis;
    }

    Matcher matcher = pigLogfilePattern.matcher(pigLogfile);
    if (matcher.matches()) {
      String submitTimeMillisString = matcher.group(1);
      pigSubmitTimeMillis = Long.parseLong(submitTimeMillisString);
    }
    return pigSubmitTimeMillis;
  }

}
