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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.twitter.hraven.Constants;

/**
 * Class that handles Job files, whether confs files or history files.
 * 
 */
public class JobFile implements Writable {

  private static final Pattern PATTERN = Pattern
      .compile(Constants.JOB_FILENAME_PATTERN_REGEX);

  private static final Pattern CONF_PATTERN = Pattern
      .compile(Constants.JOB_CONF_FILE_END);

  private static final Log LOG = LogFactory.getLog(JobFile.class);
  private String filename;
  private String jobid = null;
  private boolean isJobConfFile = false;
  private boolean isJobHistoryFile = false;

  /**
   * Default constructor. Used by reflection utils in combination with {@link #readFields(DataInput)}
   */
  public JobFile() {
    
  }
  
  /**
   * Constructor
   * 
   * @param name
   *          of the file (that may or may not be a job file).
   */
  public JobFile(String filename) {
    if (null == filename) {
      this.filename = "";
    } else {
      this.filename = filename;
    }
    parseFilename();
  }

  /**
   * Parse the filename and pull the jobtracker and jobid out of it.
   */
  private void parseFilename() {

    // Add additional filtering to discard empty files, or files ending in .crc
    if ((filename != null) && (filename.length() > 0)
        && (!filename.endsWith(".crc"))) {

      Matcher matcher = PATTERN.matcher(filename);

      if (matcher.matches()) {
    	//  jobTracker = "";
    	  jobid = matcher.group(1);
    	  Matcher confMatcher = CONF_PATTERN.matcher(filename);
    	  if (confMatcher.matches()) {
        	isJobConfFile = true;
        	LOG.debug("Job Conf file  " + filename + " with job id: " + jobid);
        	} else {
        		isJobHistoryFile = true;
        		LOG.debug("Job History file " + filename + " with job id: " + jobid);
        		}
    	  }
      else {
          LOG.info(" file does not match any format: " + filename);
      }
    }
  }

  /**
   * @return the name of this file.
   */
  public String getFilename() {
    return filename;
  }

  /**
   * @return the job ID for this job as parsed through the filename or null if
   *         this is not a valid job file.
   */
  public String getJobid() {
    return jobid;
  }

  /**
   * @return whether this file is a JobConfFile
   */
  public boolean isJobConfFile() {
    return isJobConfFile;
  }

  /**
   * @return whether this file is a JobHistoryFile
   */
  public boolean isJobHistoryFile() {
    return isJobHistoryFile;
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, filename);
    Text.writeString(out, jobid);
    out.writeBoolean(isJobConfFile);
    out.writeBoolean(isJobHistoryFile);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    this.filename = Text.readString(in);
    this.jobid = Text.readString(in);
    this.isJobConfFile = in.readBoolean();
    this.isJobHistoryFile = in.readBoolean();
  }

}
