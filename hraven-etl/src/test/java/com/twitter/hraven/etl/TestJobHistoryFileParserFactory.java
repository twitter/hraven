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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * Test {@link JobHistoryFileParserFactory}
 * 
 */
public class TestJobHistoryFileParserFactory {

	@Test
	public void testCreateJobHistoryFileParserCorrectCreation() {

		String jHist = "Meta VERSION=\"1\" .\n"
				+ "Job JOBID=\"job_201301010000_12345\"";
		JobHistoryFileParser historyFileParser = JobHistoryFileParserFactory
				.createJobHistoryFileParser(jHist.getBytes());

		assertNotNull(historyFileParser);

		/*
		 * confirm that we get back an object that can parse hadoop 1.0 files
		 */
		assertTrue(historyFileParser instanceof JobHistoryFileParserHadoop1);

	}

	/**
	 * confirm that exception is thrown on null input
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testCreateJobHistoryFileParserNullCreation() {
		JobHistoryFileParser historyFileParser = JobHistoryFileParserFactory
				.createJobHistoryFileParser(null);
		assertNull(historyFileParser);
	}
}
