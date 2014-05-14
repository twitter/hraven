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
import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.twitter.hraven.App;
import com.twitter.hraven.Constants;
import com.twitter.hraven.GenerateFlowTestData;
import com.twitter.hraven.datasource.HRavenTestUtil;

public class TestAppService {

  @SuppressWarnings("unused")
  private static Log LOG = LogFactory.getLog(TestAppService.class);
  private static HBaseTestingUtility UTIL;
  private static JobHistoryByIdService idService;
  private static GenerateFlowTestData flowDataGen ;
  private static HTable historyTable;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    UTIL = new HBaseTestingUtility();
    UTIL.startMiniCluster();
    HRavenTestUtil.createSchema(UTIL);
    historyTable = new HTable(UTIL.getConfiguration(), Constants.HISTORY_TABLE_BYTES);
    idService = new JobHistoryByIdService(UTIL.getConfiguration());
    flowDataGen = new GenerateFlowTestData();
  }

  @Test
  public void testNewJobs() throws Exception {

    Configuration c = UTIL.getConfiguration();

    String cluster1 = "newJobsClusterName";
    String appId = "getNewJobs";
    String appId2 = "getNewJobs2";
    String user = "testuser";

    JobHistoryService jhs = new JobHistoryService(UTIL.getConfiguration());
    AppVersionService service = new AppVersionService(c);
    AppService as = new AppService(c);
    try {
      // check adding versions in order
      service.addVersion(cluster1, user, appId, "v1", 10L);
      flowDataGen.loadFlow(cluster1, user, appId, 10L, "v1", 3, 10, idService, historyTable);
      service.addVersion(cluster1, user, appId, "v2", 20L);
      flowDataGen.loadFlow(cluster1, user, appId, 20L, "v2", 3, 10, idService, historyTable);
      // Since flow has its own unit tests
      // we don't test the get flow series as part of testing this new jobs call
      List<App> fl = as.getNewApps(jhs, cluster1, user, 0L, 15L, 100);
      assertNotNull(fl);
      assertEquals(1, fl.size());
      service.addVersion(cluster1, user, appId, "v3", 30L);
      flowDataGen.loadFlow(cluster1, user, appId, 30L, "v3", 3, 10, idService, historyTable);
      service.addVersion(cluster1, user, appId, "v2.5", 25L);
      flowDataGen.loadFlow(cluster1, user, appId, 25L, "v2.5", 3, 10, idService, historyTable);
      fl = as.getNewApps(jhs, cluster1, user, 20L, 35L, 100);
      assertNotNull(fl);
      // check that nothing is returned since this app first showed up at timestamp 10
      assertEquals(0, fl.size());
      service.addVersion(cluster1, user, appId2, "v102", 23L);
      flowDataGen.loadFlow(cluster1, user, appId, 23L, "v102", 3, 10, idService, historyTable);
      fl = as.getNewApps(jhs, cluster1, user, 20L, 35L, 5);
      assertNotNull(fl);
      // check that appId2 is returned
      assertEquals(1, fl.size());
      assertEquals(appId2, fl.get(0).getKey().getAppId());
      // now check for entire time range
      fl = as.getNewApps(jhs, cluster1, user, 0L, 35L, 5);
      assertNotNull(fl);
      // check that both apps are returned
      assertEquals(2, fl.size());
      for (int i = 0; i < fl.size(); i++) {
        String anAppId = fl.get(i).getKey().getAppId();
        if(!(appId.equals(anAppId)) && !(appId2.equals(anAppId))) {
          throw new AssertionError("Could not find the right apps as expected");
        }
      }
    } finally {
      service.close();
     }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }
}

