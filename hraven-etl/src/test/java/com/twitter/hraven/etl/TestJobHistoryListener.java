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

package com.twitter.hraven.etl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.twitter.hraven.Constants;
import com.twitter.hraven.JobHistoryKeys;
import com.twitter.hraven.JobHistoryRecord;
import com.twitter.hraven.JobHistoryRecordCollection;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.HadoopVersion;
import com.twitter.hraven.mapreduce.JobHistoryListener;


/**
 * Test {@link TestJobHistoryListener}
 * Presently tests only the hadoop version Put
 * 
 */
public class TestJobHistoryListener {
	
	/**
	 * test the hadoop version Put
	 */
	@Test
	public void checkHadoopVersionSet() {
		JobKey jobKey = new JobKey("cluster1", "user", "Sleep", 1,
				"job_1329348432655_0001");
		JobHistoryListener jobHistoryListener = new JobHistoryListener(jobKey);
		assertEquals(jobHistoryListener.getJobRecords().size(), 0);
		
		JobHistoryFileParserHadoop1 jh = new JobHistoryFileParserHadoop1(null);

		JobHistoryRecord versionRecord = jh.getHadoopVersionRecord(
				HadoopVersion.ONE,
				jobHistoryListener.getJobKey());
		jobHistoryListener.includeHadoopVersionRecord(versionRecord);
		assertEquals(jobHistoryListener.getJobRecords().size(), 1);

		// check hadoop version
		boolean foundVersion1 = false;
		for (JobHistoryRecord p : (JobHistoryRecordCollection)jobHistoryListener.getJobRecords()) {
		  if (!p.getDataKey().get(0).equals(JobHistoryKeys.hadoopversion.toString().toLowerCase())) {
			// we are interested in hadoop version put only
			// hence continue
			continue;
		  }
		  assert(p.getDataValue() != null);
		  
		  // ensure we have a hadoop2 version as the value
		  assertEquals(p.getDataValue(),
					HadoopVersion.ONE.toString());
		  // ensure we don't see the same put twice
		  assertFalse(foundVersion1);
		  // now set this to true
		  foundVersion1 = true;
		}
		// ensure that we got the hadoop1 version put
		assertTrue(foundVersion1);
	}
}