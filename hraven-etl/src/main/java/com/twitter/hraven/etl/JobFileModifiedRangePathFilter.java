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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Pathfilter that allows only files that are named correctly and are modified
 * within a certain time range.
 * 
 */
public class JobFileModifiedRangePathFilter extends JobFilePathFilter {

  /**
   * The minimum modification time of a file to be accepted in milliseconds
   * since January 1, 1970 UTC (excluding).
   */
  private final long minModificationTimeMillis;

  /**
   * The maximum modification time of a file to be accepted in milliseconds
   * since January 1, 1970 UTC (including).
   */
  private final long maxModificationTimeMillis;

  /**
   * The configuration of this processing job (not the files we are processing).
   */
  private final Configuration myConf;
  private static Log LOG = LogFactory.getLog(JobFileModifiedRangePathFilter.class);

  /**
   * Constructs a filter that accepts only JobFiles with lastModification time
   * in the specified range.
   * 
   * @param myConf
   *          used to be able to go from a path to a FileStatus.
   * @param minModificationTimeMillis
   *          The minimum modification time of a file to be accepted in
   *          milliseconds since January 1, 1970 UTC (excluding).
   * @param maxModificationTimeMillis The
   *          maximum modification time of a file to be accepted in milliseconds
   *          since January 1, 1970 UTC (including).
   */
  public JobFileModifiedRangePathFilter(Configuration myConf,
      long minModificationTimeMillis, long maxModificationTimeMillis) {
    this.myConf = myConf;
    this.minModificationTimeMillis = minModificationTimeMillis;
    this.maxModificationTimeMillis = maxModificationTimeMillis;
  }

  /**
   * Constructs a filter that accepts only JobFiles with lastModification time
   * as least the specified minumum.
   * 
   * @param myConf
   *          used to be able to go from a path to a FileStatus.
   * @param minModificationTimeMillis
   *          The minimum modification time of a file to be accepted in
   *          milliseconds since January 1, 1970 UTC (excluding).
   */
  public JobFileModifiedRangePathFilter(Configuration myConf,
      long minModificationTimeMillis) {
    this(myConf, minModificationTimeMillis, Long.MAX_VALUE);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.twitter.hraven.etl.JobFilePathFilter#accept(org.apache
   * .hadoop.fs.Path)
   */
  @Override
  public boolean accept(Path path) {
    if (!super.accept(path)) {
      return false;
    }

    JobFile jobFile = new JobFile(path.getName());
    if (jobFile.isJobConfFile() || jobFile.isJobHistoryFile()) {
      try {
        FileSystem fs = path.getFileSystem(myConf);
        FileStatus fileStatus = fs.getFileStatus(path);
        long fileModificationTimeMillis = fileStatus.getModificationTime();
        return accept(fileModificationTimeMillis);
      } catch (IOException e) {
        throw new ImportException("Cannot determine file modification time of "
            + path.getName(), e);
      }
    } else {
      // Reject anything that does not match a job conf filename.
      LOG.info(" Not a valid job conf / job history file "+ path.getName());
      return false;
    }
  }

  /**
   * @param fileModificationTimeMillis
   *          in milliseconds since January 1, 1970 UTC
   * @return whether a file with such modification time is to be accepted.
   */
  public boolean accept(long fileModificationTimeMillis) {
    return ((minModificationTimeMillis < fileModificationTimeMillis) && (fileModificationTimeMillis <= maxModificationTimeMillis));
  }

  /**
   * @return the minModificationTimeMillis used in for this filter.
   */
  public long getMinModificationTimeMillis() {
    return minModificationTimeMillis;
  }

  /**
   * @return the maxModificationTimeMillis used for this filter
   */
  public long getMaxModificationTimeMillis() {
    return maxModificationTimeMillis;
  }

}
