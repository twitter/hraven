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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import org.junit.Test;

import com.twitter.hraven.HistoryFileType;

/**
 * Test {@link JobHistoryFileParserFactory}
 * 
 */
public class TestJobHistoryFileParserFactory {

	/**
	 * check the versions in history files across hadoop 1 and hadoop 2
	 */
  @Test
  public void testGetVersion() {
    String jHist1 = "Meta VERSION=\"1\" .\n" + "Job JOBID=\"job_201301010000_12345\"";
    HistoryFileType version1 = JobHistoryFileParserFactory.getHistoryFileType(null,
      jHist1.getBytes());
    // confirm that we get back hadoop 1.0 version as history file type
    assertEquals(JobHistoryFileParserFactory.getHistoryFileVersion1(), version1);

    String jHist2 = "Avro-Json\n"
            + "{\"type\":\"record\",\"name\":\"Event\", "
            + "\"namespace\":\"org.apache.hadoop.mapreduce.jobhistory\",\"fields\":[]\"";
    HistoryFileType version2 = JobHistoryFileParserFactory.getHistoryFileType(null,
      jHist2.getBytes());
    // confirm that we get back hadoop 2.0 version as history file type
    assertEquals(JobHistoryFileParserFactory.getHistoryFileVersion2(), version2);
  }

  /**
   * confirm that exception is thrown on incorrect input
   */
  @Test(expected = IllegalArgumentException.class)
  public void testGetHistoryFileTypeIncorrect2() {
    String jHist2 =
        "Avro-HELLO-Json\n" + "{\"type\":\"record\",\"name\":\"Event\", "
            + "\"namespace\":\"org.apache.hadoop.mapreduce.jobhistory\",\"fields\":[]\"";
    JobHistoryFileParserFactory.getHistoryFileType(null, jHist2.getBytes());
  }

  /**
   * confirm that exception is thrown on incorrect input
   */
  @Test(expected = IllegalArgumentException.class)
  public void testGetHistoryFileTypeIncorrect1() {
    String jHist1 = "Meta HELLO VERSION=\"1\" .\n" + "Job JOBID=\"job_201301010000_12345\"";
    JobHistoryFileParserFactory.getHistoryFileType(null, jHist1.getBytes());
  }

  /**
	 * confirm that exception is thrown on null input
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testCreateJobHistoryFileParserNullCreation() {
		JobHistoryFileParser historyFileParser = JobHistoryFileParserFactory
				.createJobHistoryFileParser(null, null, null);
		assertNull(historyFileParser);
	}
}
