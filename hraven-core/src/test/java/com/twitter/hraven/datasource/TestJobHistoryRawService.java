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
package com.twitter.hraven.datasource;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.twitter.hraven.JobId;
import com.twitter.hraven.Range;
import com.twitter.hraven.datasource.JobHistoryRawService;
import com.twitter.hraven.util.BatchUtil;

public class TestJobHistoryRawService {

  private static final String JOB_HISTORY_FILE_NAME = "src/test/resources/done/something.example.com_1337787092259_job_201205231531_256984_userName1_App1";

  /**
   * Normal example
   */
  private static final String JOB_HISTORY = "Meta VERSION=\"1\" ."
      + "Job JOBID=\"job_201206061540_11222\""
      + "JOBNAME=\"App1:some_project_one_day\""
      + "USER=\"someone\" SUBMIT_TIME=\"1339063492288\"" + "JOBCONF=\"";

  /**
   * Submit time at the end of the string, but still includes quote
   */
  private static final String JOB_HISTORY2 = "Meta VERSION=\"1\" ."
      + "Job JOBID=\"job_201206061540_11222\""
      + "JOBNAME=\"App1:some_project_one_day\""
      + "USER=\"someone\" SUBMIT_TIME=\"1339063492288\"";

  /**
   * Submit time is the only thing in the string.
   */
  private static final String JOB_HISTORY3 = "SUBMIT_TIME=\"1339063492288\"";

  private static final String BAD_JOB_HISTORY = "SUBMIT_TIME=\"";

  private static final String BAD_JOB_HISTORY2 = "SUBMIT_TIME=\"\"";

  /**
   * Missing quote at the end
   */
  private static final String BAD_JOB_HISTORY3 = "Meta VERSION=\"1\" ."
      + "Job JOBID=\"job_201206061540_11222\""
      + "JOBNAME=\"App1:some_project_one_day\""
      + "USER=\"someone2\" SUBMIT_TIME=\"1339063492288";

  /**
   * Missing start quote
   */
  private static final String BAD_JOB_HISTORY4 = "Meta VERSION=\"1\" ."
      + "Job JOBID=\"job_201206061540_11222\""
      + "JOBNAME=\"App1:some_project_one_day\""
      + "USER=\"someone3\" SUBMIT_TIME=1339063492288\"";

  /**
   * Confirm that we can properly find the submit timestamp.
   */
  @Test
  public void testGetSubmitTimeMillisFromJobHistory() {
    byte[] jobHistoryBytes = Bytes.toBytes(JOB_HISTORY);
    long submitTimeMillis = JobHistoryRawService
        .getSubmitTimeMillisFromJobHistory(jobHistoryBytes);
    assertEquals(1339063492288L, submitTimeMillis);

    jobHistoryBytes = Bytes.toBytes(JOB_HISTORY2);
    submitTimeMillis = JobHistoryRawService
        .getSubmitTimeMillisFromJobHistory(jobHistoryBytes);
    assertEquals(1339063492288L, submitTimeMillis);

    jobHistoryBytes = Bytes.toBytes(JOB_HISTORY3);
    submitTimeMillis = JobHistoryRawService
        .getSubmitTimeMillisFromJobHistory(jobHistoryBytes);
    assertEquals(1339063492288L, submitTimeMillis);

    // Now some cases where we should not be able to find any timestamp.
    jobHistoryBytes = Bytes.toBytes("");
    submitTimeMillis = JobHistoryRawService
        .getSubmitTimeMillisFromJobHistory(jobHistoryBytes);
    assertEquals(0L, submitTimeMillis);

    jobHistoryBytes = Bytes.toBytes(BAD_JOB_HISTORY);
    submitTimeMillis = JobHistoryRawService
        .getSubmitTimeMillisFromJobHistory(jobHistoryBytes);
    assertEquals(0L, submitTimeMillis);

    jobHistoryBytes = Bytes.toBytes(BAD_JOB_HISTORY2);
    submitTimeMillis = JobHistoryRawService
        .getSubmitTimeMillisFromJobHistory(jobHistoryBytes);
    assertEquals(0L, submitTimeMillis);

    jobHistoryBytes = Bytes.toBytes(BAD_JOB_HISTORY3);
    submitTimeMillis = JobHistoryRawService
        .getSubmitTimeMillisFromJobHistory(jobHistoryBytes);
    assertEquals(0L, submitTimeMillis);

    jobHistoryBytes = Bytes.toBytes(BAD_JOB_HISTORY4);
    submitTimeMillis = JobHistoryRawService
        .getSubmitTimeMillisFromJobHistory(jobHistoryBytes);
    assertEquals(0L, submitTimeMillis);
  }

  /**
   * Confirm that we can properly find the submit timestamp.
   * 
   * @throws IOException
   */
  @Test
  public void testGetSubmitTimeMillisFromJobHistoryFile() throws IOException {
    byte[] jobHistoryBytes = null;

    File jobHistoryfile = new File(JOB_HISTORY_FILE_NAME);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();

    FileInputStream fis = new FileInputStream(jobHistoryfile);
    try {
      byte[] buffer = new byte[1024];
      int length = 0;
      while ((length = fis.read(buffer)) > 0) {
        bos.write(buffer, 0, length);
      }
      jobHistoryBytes = bos.toByteArray();
    } finally {
      fis.close();
    }

    long submitTimeMillis = JobHistoryRawService
        .getSubmitTimeMillisFromJobHistory(jobHistoryBytes);
    assertEquals(1338958320124L, submitTimeMillis);
  }

  /**
   * Does not test a specific method, but tests the algorithm used to get
   * ranges.
   */
  @Test
  public void testGetJobIdRanges() {

    long aEpoch = 123456;
    long bEpoch = 234567;

    JobId aOne = new JobId(aEpoch, 1);
    JobId aTwo = new JobId(aEpoch, 2);
    JobId aThree = new JobId(aEpoch, 3);
    JobId aSeven = new JobId(aEpoch, 7);
    
    JobId aThirteen = new JobId(aEpoch, 13);
    JobId aHundredOne = new JobId(aEpoch, 101);
    JobId bOne = new JobId(bEpoch, 1);
    JobId bTwo = new JobId(bEpoch, 2);
    
    JobId bThree = new JobId(bEpoch, 3);
    JobId bSeven = new JobId(bEpoch, 7);
    JobId bThirteen = new JobId(bEpoch, 13);
    JobId bHundredOne = new JobId(bEpoch, 101);

    SortedSet<JobId> orderedJobIds = new TreeSet<JobId>();
    // Add in scrambled order
    orderedJobIds.add(bSeven);
    orderedJobIds.add(aSeven);
    orderedJobIds.add(aThree);
    orderedJobIds.add(bThree);
    orderedJobIds.add(bThirteen);
    orderedJobIds.add(aThirteen);
    orderedJobIds.add(aOne);
    orderedJobIds.add(bOne);
    orderedJobIds.add(aHundredOne);
    orderedJobIds.add(bHundredOne);
    orderedJobIds.add(aTwo);
    orderedJobIds.add(bTwo);

    // And for good measure add these again, set should take them out.
    orderedJobIds.add(aTwo);
    orderedJobIds.add(bTwo);
    
    assertEquals(12, orderedJobIds.size());

    List<Range<JobId>> ranges = BatchUtil.getRanges(orderedJobIds, 4);
    assertEquals(3, ranges.size());
    assertEquals(1, ranges.get(0).getMin().getJobSequence());
    assertEquals(7, ranges.get(0).getMax().getJobSequence());
    assertEquals(aEpoch, ranges.get(0).getMin().getJobEpoch());
    assertEquals(aEpoch, ranges.get(0).getMax().getJobEpoch());
    
    assertEquals(13, ranges.get(1).getMin().getJobSequence());
    assertEquals(2, ranges.get(1).getMax().getJobSequence());
    assertEquals(aEpoch, ranges.get(1).getMin().getJobEpoch());
    assertEquals(bEpoch, ranges.get(1).getMax().getJobEpoch());
    
    assertEquals(3, ranges.get(2).getMin().getJobSequence());
    assertEquals(101, ranges.get(2).getMax().getJobSequence());
    assertEquals(bEpoch, ranges.get(2).getMin().getJobEpoch());
    assertEquals(bEpoch, ranges.get(2).getMax().getJobEpoch());
    
    long cEpoch = 345678;
    long triangular = 1000405;
    JobId cTriangular = new JobId(cEpoch, triangular);
    orderedJobIds.add(cTriangular);
    
    assertEquals(13, orderedJobIds.size());
    ranges = BatchUtil.getRanges(orderedJobIds, 4);
    assertEquals(4, ranges.size());
    assertEquals(triangular, ranges.get(3).getMin().getJobSequence());
    assertEquals(triangular, ranges.get(3).getMax().getJobSequence());
    assertEquals(cEpoch, ranges.get(3).getMin().getJobEpoch());
    assertEquals(cEpoch, ranges.get(3).getMax().getJobEpoch());
    
    ranges = BatchUtil.getRanges(orderedJobIds, 1000);
    assertEquals(1, ranges.size());
    assertEquals(1, ranges.get(0).getMin().getJobSequence());
    assertEquals(triangular, ranges.get(0).getMax().getJobSequence());
    assertEquals(aEpoch, ranges.get(0).getMin().getJobEpoch());
    assertEquals(cEpoch, ranges.get(0).getMax().getJobEpoch());
  }
}
