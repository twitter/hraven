/*
Copyright 2014 Twitter, Inc.

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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Round-trip testing for storage and retrieval of data in job_history table.
 *
 */
public class TestHdfsStatsService {
  private static Log LOG = LogFactory.getLog(TestHdfsStatsService.class);
  @SuppressWarnings("unused")
  private static HBaseTestingUtility UTIL;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
   // UTIL = new HBaseTestingUtility();
    //UTIL.startMiniCluster();
    //HRavenTestUtil.createSchema(UTIL);
  }

  @Test
  public void TestGetLastHourInvertedTimestamp() throws IOException {
    long ts = 1391896800L;
    long expectedTop = Long.MAX_VALUE - ts ;
    long actualTop = HdfsStatsService.getLastHourInvertedTimestamp(ts);
    LOG.info(" expected inv ts " + expectedTop);
    assertEquals(actualTop, expectedTop);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    //UTIL.shutdownMiniCluster();
  }
}
