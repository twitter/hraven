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

import java.io.IOException;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.twitter.hraven.Constants;
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
    Table t = HRavenTestUtil.createAppVersionTable(UTIL);
  }

  @Test
  public void testAddVersion() throws Exception {
    Configuration c = UTIL.getConfiguration();
    String appId = "addVersion";
    byte[] appIdBytes = Bytes.toBytes(appId);

    byte[] appRow = Bytes.add(Bytes.add(clusterBytes, Constants.SEP_BYTES),
        Bytes.add(userBytes, Constants.SEP_BYTES),
        appIdBytes);

    AppVersionService service = null;
    Connection conn = null;
    Table versionTable = null;
    try {
      service = new AppVersionService(c);
      conn = ConnectionFactory.createConnection(c);
      versionTable = conn.getTable(TableName.valueOf(Constants.HISTORY_APP_VERSION_TABLE));

      service.addVersion(cluster, user, appId, "v1", 1);
      Result r = versionTable.get(new Get(appRow));
      assertNotNull(r);
      // should have 1 version
      assertEquals(r.listCells().size(), 1);
      assertArrayEquals(
          r.getValue(Constants.INFO_FAM_BYTES, Bytes.toBytes("v1")),
          Bytes.toBytes(1L));

      service.addVersion(cluster, user, appId, "v2", 10);
      r = versionTable.get(new Get(appRow));
      assertNotNull(r);
      assertEquals(r.listCells().size(), 2);
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
      assertEquals(r.listCells().size(), 2);
      assertArrayEquals(
          r.getValue(Constants.INFO_FAM_BYTES, Bytes.toBytes("v2")),
          Bytes.toBytes(5L));

      // re-add v1 with later timestamp, should ignore
      service.addVersion(cluster, user, appId, "v1", 11);
      r = versionTable.get(new Get(appRow));
      assertNotNull(r);
      assertEquals(r.listCells().size(), 2);
      assertArrayEquals(
          r.getValue(Constants.INFO_FAM_BYTES, Bytes.toBytes("v1")),
          Bytes.toBytes(1L));
    } finally {
      try {
        if (service != null) {
          service.close();
        }
      } catch (IOException ignore) {
      }
      try {
        if (versionTable != null) {
          versionTable.close();
        }
      } catch (IOException ignore) {
      }
      try {
        if (conn != null) {
          conn.close();
        }
      } catch (IOException ignore) {
      }
    }
  }

  @Test
  public void testGetLatestVersion() throws Exception {
    Configuration c = UTIL.getConfiguration();

    String appId = "getLatestVersion";

    AppVersionService service = null;
    try {
      service = new AppVersionService(c);
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
      try {
        if (service != null) {
          service.close();
        }
      } catch (IOException ignore) {
      }
    }
  }

  @Test
  public void testGetDistinctVersions() throws Exception {
    Configuration c = UTIL.getConfiguration();

    { /*
       * TEST1 check that empty list is returned when no versions exist
       */

      String appId = "getDistinctVersions";
      AppVersionService service = null;
      try {
        service = new AppVersionService(c);

        List<VersionInfo> latest = service.getDistinctVersions(cluster, user, appId);
        // expecting nothing (0 versions)
        assertEquals(latest.size(), 0);
      } finally {
        try {
          if (service != null) {
            service.close();
          }
        } catch (IOException ignore) {
        }
      }
    }

    { /*
       * TEST2 Check that only distinct versions are returned, when Multiple
       * Versions Exist
       */
      String appId = "getDistinctVersions";
      AppVersionService service = null;
      try {
        service = new AppVersionService(c);
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
        try {
          if (service != null) {
            service.close();
          }
        } catch (IOException ignore) {
        }
      }
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }
}
