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
import java.io.ByteArrayInputStream;
import org.junit.Test;
import com.twitter.hraven.JobKey;

/**
 * Test {@link JobHistoryDecipherFactory}
 * 
 */
public class TestJobHistoryDecipherFactory {

	@Test
	public void testCreateJobHistoryFileDecipherCorrectCreation() {
		JobHistoryFileDecipherFactory dFactory = new JobHistoryFileDecipherFactory();

		JobKey jk = new JobKey("cluster1@identifier1", "userName1", "appId1",
				1, "job_12345_6789");
		byte[] fileContents = "job history file contents".getBytes();
		JobHistoryFileDecipherBase historyFileParser = dFactory
				.createJobHistoryFileDecipher(new ByteArrayInputStream(
						fileContents), jk);

		assertNotNull(historyFileParser);

		/*
		 * confirm that we get back an object that can parse hadoop 1.0 files
		 */
		assertTrue(historyFileParser instanceof JobHistoryFileDecipherHadoop1);
		
	}

	/**
	 * confirm that null is returned on null input
	 */
	@Test
	public void testCreateJobHistoryFileDecipherNullCreation() {
		JobHistoryFileDecipherFactory dFactory = new JobHistoryFileDecipherFactory();

		JobKey jk = new JobKey("cluster1@identifier1", "userName1", "appId1",
				1, "job_12345_6789");

		JobHistoryFileDecipherBase historyFileParser1 = dFactory
				.createJobHistoryFileDecipher(null, jk);

		assertNull(historyFileParser1);
	
		byte[] fileContents = "job history file contents".getBytes();
		JobHistoryFileDecipherBase historyFileParser2 = dFactory
				.createJobHistoryFileDecipher(new ByteArrayInputStream(
						fileContents), null);
		assertNull(historyFileParser2);		
	}
}