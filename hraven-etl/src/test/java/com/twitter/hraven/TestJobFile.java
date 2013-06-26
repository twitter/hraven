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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.twitter.hraven.etl.JobFile;

/**
 * Test the {@link JobFile} class.
 * 
 */
public class TestJobFile {

  final static String VALID_JOB_CONF_FILENAME = "hostname1.example.com_1333569494142_job_201204041958_150125_conf.xml";
  final static String VALID_JOB_HISTORY_FILENAME = "hostname1.example.com_1333569494142_job_201204041958_1599_hadoop_App1%3Asomething%3Axyz%2F04%2F03-00%3A00%3A";
  final static String VALID_JOB_CONF_FILENAME2 = "hostname2.example.com_1334279672946_job_201204130114_0020_conf.xml";
  final static String VALID_JOB_HISTORY_FILENAME2 = "hostname2.example.com_1334279672946_job_201204130114_0020_user1_JobConfParser";
 
  final static String INVALID_JOB_FILENAME = "jabbedabbedoo.txt";

  /**
   * Test the conf file.
   */
  @Test
  public void testJobConfFile() {
 
    JobFile jobFile = new JobFile(VALID_JOB_CONF_FILENAME);
    assertTrue("This should be a valid jobfile", jobFile.isJobConfFile());
    assertFalse("this should not be a job history file",
        jobFile.isJobHistoryFile());
    assertEquals("job_201204041958_150125", jobFile.getJobid());
    assertEquals("hostname1.example.com", jobFile.getJobTracker());

    jobFile = new JobFile(VALID_JOB_HISTORY_FILENAME);
    assertFalse("This should not be a valid jobfile", jobFile.isJobConfFile());
    assertTrue("this should be a job history file", jobFile.isJobHistoryFile());
    assertEquals("job_201204041958_1599", jobFile.getJobid());
    assertEquals("hostname1.example.com", jobFile.getJobTracker());

    jobFile = new JobFile(VALID_JOB_CONF_FILENAME2);
    assertTrue("This should be a valid jobfile", jobFile.isJobConfFile());
    assertFalse("this should not be a job history file",
        jobFile.isJobHistoryFile());
    assertEquals("job_201204130114_0020", jobFile.getJobid());
    assertEquals("hostname2.example.com", jobFile.getJobTracker());

    jobFile = new JobFile(VALID_JOB_HISTORY_FILENAME2);
    assertFalse("This should not be a valid jobfile", jobFile.isJobConfFile());
    assertTrue("this should be a job history file", jobFile.isJobHistoryFile());
    assertEquals("job_201204130114_0020", jobFile.getJobid());
    assertEquals("hostname2.example.com", jobFile.getJobTracker());

    jobFile = new JobFile(INVALID_JOB_FILENAME);
    assertFalse("This should not be a valid jobfile", jobFile.isJobConfFile());
    assertFalse("this should not be a job history file",
        jobFile.isJobHistoryFile());

  }

}
