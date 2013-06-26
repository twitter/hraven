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
package com.twitter.hraven.etl;

import org.apache.hadoop.fs.FileStatus;

import com.twitter.hraven.JobId;

/**
 * Used to track the min and max JobId as well as file modification time of
 * files encountered
 */
public class MinMaxJobFileTracker {

  /**
   * Keep track of the minimum job ID that we have seen to later update the
   * processing record.
   */
  private JobId minJobId = null;

  /**
   * Keep track of the maximum job ID that we have seen to later update the
   * processing record.
   */
  private JobId maxJobId = null;

  /**
   * The minimum modification time of a file in milliseconds since January 1,
   * 1970 UTC tracked so far.
   */
  private long minModificationTimeMillis;

  /**
   * The maximum modification time of a file in milliseconds since January 1,
   * 1970 UTC tracked so far.
   */
  private long maxModificationTimeMillis;

  /**
   * Default constructor.
   */
  public MinMaxJobFileTracker() {
    // Initialize to guarantee that whatever value we see is the new min.
    minModificationTimeMillis = Long.MAX_VALUE;
    // Initialize to guarantee that whatever value we see is the new max.
    maxModificationTimeMillis = 0L;
  }

  /**
   * Converts a jobFileStatus to a JobFile and tracks the min and max
   * modification times and JobIds.
   * 
   * @param jobFileStatus
   *          of a jobfile, must be a proper JobFile. Cannot be null.
   * @return a JobFile for the given jobFileStatus.
   */
  public JobFile track(FileStatus jobFileStatus) {

    String jobfileName = jobFileStatus.getPath().getName();
    JobFile jobFile = new JobFile(jobfileName);

    // Extra check, caller should already have taken care of this.
    if (jobFile.isJobConfFile() || jobFile.isJobHistoryFile()) {
      track(jobFile.getJobid());

      long modificationTimeMillis = jobFileStatus.getModificationTime();
      if (modificationTimeMillis < minModificationTimeMillis) {
        minModificationTimeMillis = modificationTimeMillis;
      }
      if (modificationTimeMillis > maxModificationTimeMillis) {
        maxModificationTimeMillis = modificationTimeMillis;
      }
    }
    return jobFile;
  }

  /**
   * @param jobIdString
   *          to be tracked.
   */
  public void track(String jobIdString) {
    JobId jobId = new JobId(jobIdString);

    // If the previous minimum is not set, or is larger than the new value,
    // the new id is the min
    if (minJobId == null || (minJobId.compareTo(jobId) > 0)) {
      minJobId = jobId;
    }
    // Ditto for the max
    if (maxJobId == null || (maxJobId.compareTo(jobId) < 0)) {
      maxJobId = jobId;
    }
  }

  /**
   * @return The minimum job ID that we have processed so far.
   */
  public String getMinJobId() {
    return minJobId.getJobIdString();
  }

  /**
   * @return The maximum job ID that we have processed so far.
   */
  public String getMaxJobId() {
    return maxJobId.getJobIdString();
  }

  /**
   * @return The minimum modification time of a file in milliseconds since
   *         January 1, 1970 UTC tracked so far.
   */
  public long getMinModificationTimeMillis() {
    return minModificationTimeMillis;
  }

  /**
   * @return The maximum modification time of a file in milliseconds since
   *         January 1, 1970 UTC tracked so far.
   */
  public long getMaxModificationTimeMillis() {
    return maxModificationTimeMillis;
  }

}
