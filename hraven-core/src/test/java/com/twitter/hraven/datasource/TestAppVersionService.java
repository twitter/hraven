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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.twitter.hraven.CapacityDetails;
import com.twitter.hraven.Constants;
import com.twitter.hraven.Flow;
import com.twitter.hraven.datasource.AppVersionService;
import com.twitter.hraven.datasource.VersionInfo;

/**
 * Test class for {@link AppVersionService}
 */
public class TestAppVersionService {
  private static HBaseTestingUtility UTIL;

  private String cluster = "test@local";
  private byte[] clusterBytes = Bytes.toBytes(cluster);
  private String user = "testuser";
  private byte[] userBytes = Bytes.toBytes(user);

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    UTIL = new HBaseTestingUtility();
    UTIL.startMiniCluster();
    HRavenTestUtil.createAppVersionTable(UTIL);
  }

  @Test
  public void testAddVersion() throws Exception {
    Configuration c = UTIL.getConfiguration();
    AppVersionService service = new AppVersionService(c);
    String appId = "addVersion";
    byte[] appIdBytes = Bytes.toBytes(appId);

    byte[] appRow = Bytes.add(Bytes.add(clusterBytes, Constants.SEP_BYTES),
        Bytes.add(userBytes, Constants.SEP_BYTES),
        appIdBytes);
    HTable versionTable = new HTable(c, Constants.HISTORY_APP_VERSION_TABLE);

    try {
      service.addVersion(cluster, user, appId, "v1", 1);
      Result r = versionTable.get(new Get(appRow));
      assertNotNull(r);
      // should have 1 version
      assertEquals(r.list().size(), 1);
      assertArrayEquals(
          r.getValue(Constants.INFO_FAM_BYTES, Bytes.toBytes("v1")),
          Bytes.toBytes(1L));

      service.addVersion(cluster, user, appId, "v2", 10);
      r = versionTable.get(new Get(appRow));
      assertNotNull(r);
      assertEquals(r.list().size(), 2);
      assertArrayEquals(
          r.getValue(Constants.INFO_FAM_BYTES, Bytes.toBytes("v1")),
          Bytes.toBytes(1L));
      assertArrayEquals(
          r.getValue(Constants.INFO_FAM_BYTES, Bytes.toBytes("v2")),
          Bytes.toBytes(10L));

      // add v2 with earlier timestamp
      service.addVersion(cluster, user, appId, "v2", 5);
      r = versionTable.get(new Get(appRow));
      assertNotNull(r);
      assertEquals(r.list().size(), 2);
      assertArrayEquals(
          r.getValue(Constants.INFO_FAM_BYTES, Bytes.toBytes("v2")),
          Bytes.toBytes(5L));

      // re-add v1 with later timestamp, should ignore
      service.addVersion(cluster, user, appId, "v1", 11);
      r = versionTable.get(new Get(appRow));
      assertNotNull(r);
      assertEquals(r.list().size(), 2);
      assertArrayEquals(
          r.getValue(Constants.INFO_FAM_BYTES, Bytes.toBytes("v1")),
          Bytes.toBytes(1L));
    } finally {
      try {
        service.close();
      } catch (Exception ignore) {
      }
      try {
        versionTable.close();
      } catch (Exception ignore) {
      }
    }
  }

  @Test
  public void testGetLatestVersion() throws Exception {
    Configuration c = UTIL.getConfiguration();

    String appId = "getLatestVersion";

    AppVersionService service = new AppVersionService(c);
    try {
      // check adding versions in order
      service.addVersion(cluster, user, appId, "v1", 10);
      String latest = service.getLatestVersion(cluster, user, appId);
      assertEquals("v1", latest);
      service.addVersion(cluster, user, appId, "v2", 20);
      latest = service.getLatestVersion(cluster, user, appId);
      assertEquals("v2", latest);
      service.addVersion(cluster, user, appId, "v3", 30);
      latest = service.getLatestVersion(cluster, user, appId);
      assertEquals("v3", latest);
      // latest should not change
      service.addVersion(cluster, user, appId, "v2.5", 25);
      latest = service.getLatestVersion(cluster, user, appId);
      assertEquals("v3", latest);
    } finally {
      service.close();
    }
  }

  @Test
  public void testGetDistinctVersions() throws Exception {
    Configuration c = UTIL.getConfiguration();

    { /*
       * TEST1 check that empty list is returned when no versions exist
       */

      String appId = "getDistinctVersions";
      AppVersionService service = new AppVersionService(c);
      List<VersionInfo> latest = service.getDistinctVersions(cluster, user, appId);
      // expecting nothing (0 versions)
      assertEquals(latest.size(), 0);
    }

    { /*
       * TEST2 Check that only distinct versions are returned, when Multiple
       * Versions Exist
       */
      String appId = "getDistinctVersions";
      AppVersionService service = new AppVersionService(c);
      try {
        service.addVersion(cluster, user, appId, "v1", 10);
        service.addVersion(cluster, user, appId, "v2", 30);
        service.addVersion(cluster, user, appId, "v1", 8390);
        service.addVersion(cluster, user, appId, "v1", 90);
        service.addVersion(cluster, user, appId, "v1", 80);

        List<VersionInfo> latest = service.getDistinctVersions(cluster, user, appId);
        // expecting two distinct versions
        assertEquals(latest.size(), 2);
        HashSet<String> expVersions = new HashSet<String>();
        expVersions.add("v1");
        expVersions.add("v2");
        for (int i =0 ; i < latest.size(); i++ ) {
          assertTrue( expVersions.contains( latest.get(i).getVersion()) ) ;
        }

      } finally {
        service.close();
      }
    }
  }

  @Test
  public void testNewJobs() throws Exception {

    Configuration c = UTIL.getConfiguration();

    String cluster1 = "newJobsClusterName";
    String appId = "getNewJobs";
    String appId2 = "getNewJobs2";
    Map<String, CapacityDetails> capacityInfo = new HashMap<String, CapacityDetails>();

    AppVersionService service = new AppVersionService(c);
    try {
      // check adding versions in order
      service.addVersion(cluster1, user, appId, "v1", 10L);
      service.addVersion(cluster1, user, appId, "v2", 20L);
      // Since flow and capacity details classes' have their own unit tests
      // we don't test the get flow series as part of testing this new jobs call in
      // AppVersionService
      // hence job history service argument is passed in as null and capacityInfo map is empty
      // nor do we test the capacity related data return (through fair scheduler loading etc)
      List<Flow> fl = service.getNewJobs(cluster1, user, 0L, 15L, 5, capacityInfo, null);
      assertNotNull(fl);
      assertEquals(1, fl.size());
      assertEquals("v1", fl.get(0).getVersion());
      service.addVersion(cluster1, user, appId, "v3", 30L);
      service.addVersion(cluster1, user, appId, "v2.5", 25L);
      fl = service.getNewJobs(cluster1, user, 20L, 35L, 5, capacityInfo, null);
      assertNotNull(fl);
      // check that nothing is returned since this app first showed up at timestamp 10
      assertEquals(0, fl.size());
      service.addVersion(cluster1, user, appId2, "v102", 23L);
      fl = service.getNewJobs(cluster1, user, 20L, 35L, 5, capacityInfo, null);
      assertNotNull(fl);
      // check that appId2 is returned
      assertEquals(1, fl.size());
      assertEquals(appId2, fl.get(0).getAppId());
      // now check for entire time range
      fl = service.getNewJobs(cluster1, user, 0L, 35L, 5, capacityInfo, null);
      assertNotNull(fl);
      // check that both apps are returned
      assertEquals(2, fl.size());
      for (int i = 0; i < fl.size(); i++) {
        String anAppId = fl.get(i).getAppId();
        if (appId.equals(anAppId)) {
          assertEquals("v1", fl.get(i).getVersion());
        } else if (appId2.equals(anAppId)) {
          assertEquals("v102", fl.get(i).getVersion());
        } else {
          throw new AssertionError("Could not find the right apps with versions as expected");
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
