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
package com.twitter.hraven;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;

import com.twitter.hraven.util.DateUtil;

/**
 * Used to create {@link JobKey} instances that can deal with
 * {@link Configuration} file (contents) for {@link Framework#SCALDING}
 * 
 */
public class ScaldingJobDescFactory extends JobDescFactoryBase {

  /** Regex to clear out any portion of the job name contained in square brackets */
  private Pattern stripBracketsPattern = Pattern.compile("\\[.*\\]\\s*");
  /** Regex to strip any remaining job sequence information from the app ID */
  private Pattern stripSequencePattern = Pattern.compile("^(.*)/\\(\\d+/\\d+\\).*$");

  @Override
  JobDesc create(QualifiedJobId qualifiedJobId, long submitTimeMillis,
      Configuration jobConf) {

    String appId = getAppId(jobConf);
    if (Constants.UNKNOWN.equals(appId)) {
      // Fall back to cascading.app.id, it's not readable but should be correct
      appId = cleanAppId(jobConf.get(Constants.CASCADING_APP_ID_CONF_KEY));
    }

    String version = jobConf.get(Constants.CASCADING_VERSION_CONF_KEY);
    // TODO: What to put for older flows that do not contain this?

    // TODO: Figure out how to get a proper flow submit time for Scalding jobs.
    // For now, hack something together from the flowId
    long scaldingSubmitTimeMillis = getFlowSubmitTimeMillis(jobConf,
        submitTimeMillis);

    return create(qualifiedJobId, jobConf, appId, version,
        Framework.SCALDING, scaldingSubmitTimeMillis);
  }

  @Override
  String getAppIdFromJobName(String jobName) {
    return stripAppId(jobName);
  }

  /**
   * Strips out metadata in brackets to get a clean app name. There are multiple job name formats
   * used by various frameworks. This method attempts to normalize these job names into a somewhat
   * human readable appId format.
   */
  String stripAppId(String origId) {
    if (origId == null || origId.isEmpty()) {
      return "";
    }
    Matcher m = stripBracketsPattern.matcher(origId);
    String cleanedAppId = m.replaceAll("");
    Matcher tailMatcher = stripSequencePattern.matcher(cleanedAppId);
    if (tailMatcher.matches()) {
      cleanedAppId = tailMatcher.group(1);
    }
    return cleanedAppId;
  }

  /**
   * Returns the flow submit time for this job or a computed substitute that
   * will at least be consistent for all jobs in a flow.
   *
   * The time is computed according to:
   * <ol>
   *   <li>use "scalding.flow.submitted.timestamp" if present</li>
   *   <li>otherwise use "cascading.flow.id" as a substitute</li>
   * </ol>
   * 
   * @param jobConf
   *          The job configuration
   * @param submitTimeMillis
   *          of a individual job in the flow
   * @return when the entire flow started, or else at least something that binds
   *         all jobs in a flow together.
   */
  static long getFlowSubmitTimeMillis(Configuration jobConf,
      long submitTimeMillis) {
    // TODO: Do some parsing / hacking on this.
    // Grab the year/month component and add part of the flowId turned into long
    // kind of a thing.

    long cascadingSubmitTimeMillis = jobConf.getLong(
        Constants.CASCADING_RUN_CONF_KEY, 0);

    if (cascadingSubmitTimeMillis == 0) {
      // Convert hex encoded flow ID (128-bit MD5 hash) into long as a substitute
      String flowId = jobConf.get(Constants.CASCADING_FLOW_ID_CONF_KEY);
      if (flowId != null && !flowId.isEmpty()) {
        if (flowId.length() > 16) {
          flowId = flowId.substring(0, 16);
        }
        try {
          long tmpFlow = Long.parseLong(flowId, 16);
          // need to prevent the computed run ID from showing up in the future,
          // so we don't "mask" jobs later submitted with the correct property

          // make this show up within the job submit month
          long monthStart = DateUtil.getMonthStart(submitTimeMillis);
          // this still allows these jobs to show up in the "future", but at least
          // constrains to current month
          cascadingSubmitTimeMillis = monthStart + (tmpFlow % DateUtil.MONTH_IN_MILLIS);
        } catch (NumberFormatException nfe) {
          // fall back to the job submit time
          cascadingSubmitTimeMillis = submitTimeMillis;
        }
      } else {
        // fall back to the job submit time
        cascadingSubmitTimeMillis = submitTimeMillis;
      }
    }

    return cascadingSubmitTimeMillis;
  }

}
