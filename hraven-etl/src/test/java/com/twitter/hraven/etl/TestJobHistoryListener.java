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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.twitter.hraven.Constants;
import com.twitter.hraven.JobHistoryKeys;
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
		assertEquals(jobHistoryListener.getJobPuts().size(), 0);
		
		JobHistoryFileParserHadoop1 jh = new JobHistoryFileParserHadoop1(null);

		Put versionPut = jh.getHadoopVersionPut(
				HadoopVersion.ONE,
				jobHistoryListener.getJobKeyBytes());
		jobHistoryListener.includeHadoopVersionPut(versionPut);
		assertEquals(jobHistoryListener.getJobPuts().size(), 1);

		// check hadoop version
		boolean foundVersion1 = false;
		for (Put p : jobHistoryListener.getJobPuts()) {
		  List<Cell> kv2 = p.get(Constants.INFO_FAM_BYTES,
				  Bytes.toBytes(JobHistoryKeys.hadoopversion.toString()));
		  if (kv2.size() == 0) {
			// we are interested in hadoop version put only
			// hence continue
			continue;
		  }
		  assertEquals(1, kv2.size());
		  Map<byte[], List<KeyValue>> d = p.getFamilyMap();
		  for (List<KeyValue> lkv : d.values()) {
			for (KeyValue kv : lkv) {
			  // ensure we have a hadoop2 version as the value
			  assertEquals(Bytes.toString(kv.getValue()),
						HadoopVersion.ONE.toString() );
 			  // ensure we don't see the same put twice
			  assertFalse(foundVersion1);
			  // now set this to true
			  foundVersion1 = true;
			  }
		   }
		}
		// ensure that we got the hadoop1 version put
		assertTrue(foundVersion1);
	}
}