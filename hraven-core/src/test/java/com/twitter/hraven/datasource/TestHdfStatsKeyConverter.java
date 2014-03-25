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

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.twitter.hraven.HdfsConstants;
import com.twitter.hraven.HdfsStatsKey;
import com.twitter.hraven.util.ByteUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * tests the {@link HdfsStatsKeyConverter} class
 */
public class TestHdfStatsKeyConverter {

  @Test
  public void testHdfsStatsKeyConversion() throws Exception {
    HdfsStatsKeyConverter conv = new HdfsStatsKeyConverter();

    long now = System.currentTimeMillis();
    HdfsStatsKey key1 = new HdfsStatsKey("cluster1", "/dir1/dir2", now);

    byte[] key1Bytes = conv.toBytes(key1);
    HdfsStatsKey key2 = conv.fromBytes(key1Bytes);
    assertNotNull(key2);
    assertEquals(key1.getEncodedRunId(), key2.getEncodedRunId());
    assertEquals(key1.getQualifiedPathKey().getCluster(), key2.getQualifiedPathKey().getCluster());
    assertEquals(key1.getQualifiedPathKey().getPath(), key2.getQualifiedPathKey().getPath());
    assertEquals(key1.getQualifiedPathKey(), key2.getQualifiedPathKey());
  }

  @Test
  public void testHdfsStatsKeyConversionNS() throws Exception {
    HdfsStatsKeyConverter conv = new HdfsStatsKeyConverter();

    long now = System.currentTimeMillis();
    HdfsStatsKey key1 = new HdfsStatsKey("cluster1", "/dir1/dir2", "namespace1", now);

    byte[] key1Bytes = conv.toBytes(key1);
    HdfsStatsKey key2 = conv.fromBytes(key1Bytes);
    assertNotNull(key2);
    assertEquals(key1.getEncodedRunId(), key2.getEncodedRunId());
    assertEquals(key1.getQualifiedPathKey().getCluster(), key2.getQualifiedPathKey().getCluster());
    assertEquals(key1.getQualifiedPathKey().getPath(), key2.getQualifiedPathKey().getPath());
    assertEquals(key1.getQualifiedPathKey().getNamespace(), key2.getQualifiedPathKey().getNamespace());
    assertEquals(key1.getQualifiedPathKey(), key2.getQualifiedPathKey());
  }

  @Test
  public void testNullEmptyToBytes() {
    HdfsStatsKeyConverter conv = new HdfsStatsKeyConverter();
    byte[] keyNullBytes = conv.toBytes(null);
    assertEquals(keyNullBytes, HdfsConstants.EMPTY_BYTES);

    long now = System.currentTimeMillis();
    byte[] keyNullPathBytes = conv.toBytes(new HdfsStatsKey(null, now));
    assertEquals(keyNullPathBytes, HdfsConstants.EMPTY_BYTES);
  }

  @Test
  public void testToBytes() {
    HdfsStatsKeyConverter conv = new HdfsStatsKeyConverter();
    long now = System.currentTimeMillis();
    HdfsStatsKey key1 = new HdfsStatsKey("cluster1", "/dir1/dir2", now);
    byte[] key1Bytes = conv.toBytes(key1);
    byte[] key1ExpectedBytes =
        ByteUtil.join(HdfsConstants.SEP_BYTES, Bytes.toBytes(Long.toString(now)),
          Bytes.toBytes("cluster1"), Bytes.toBytes("/dir1/dir2"));
    assertTrue(Bytes.equals(key1Bytes, key1ExpectedBytes));
  }

  @Test
  public void testToBytesNS() {
    HdfsStatsKeyConverter conv = new HdfsStatsKeyConverter();
    long now = System.currentTimeMillis();
    HdfsStatsKey key1 = new HdfsStatsKey("cluster1", "/dir1/dir2", "namespace1", now);
    byte[] key1Bytes = conv.toBytes(key1);
    byte[] key1ExpectedBytes =
        ByteUtil.join(HdfsConstants.SEP_BYTES, Bytes.toBytes(Long.toString(now)),
          Bytes.toBytes("cluster1"), Bytes.toBytes("/dir1/dir2"), Bytes.toBytes("namespace1"));
    assertTrue(Bytes.equals(key1Bytes, key1ExpectedBytes));
  }

  @Test
  public void testSplitHdfsStatsKey() {
    long now = System.currentTimeMillis();
    byte[] nowBytes = Bytes.toBytes(Long.toString(now));
    byte[] clusterBytes = Bytes.toBytes("cluster1");
    byte[] pathBytes = Bytes.toBytes("/dir1/dir2");

    byte[] key = ByteUtil.join(HdfsConstants.SEP_BYTES, nowBytes, clusterBytes, pathBytes);
    byte[][] splits = HdfsStatsKeyConverter.splitHdfsStatsKey(key);
    assertEquals(splits.length, 3);
    assertTrue(Bytes.equals(splits[0], nowBytes));
    assertTrue(Bytes.equals(splits[1], clusterBytes));
    assertTrue(Bytes.equals(splits[2], pathBytes));
  }

  @Test
  public void testSplitHdfsStatsKeyNS() {
    long now = System.currentTimeMillis();
    byte[] nowBytes = Bytes.toBytes(Long.toString(now));
    byte[] clusterBytes = Bytes.toBytes("cluster1");
    byte[] nsBytes = Bytes.toBytes("namespace1");
    byte[] pathBytes = Bytes.toBytes("/dir1/dir2");

    byte[] key = ByteUtil.join(HdfsConstants.SEP_BYTES, nowBytes, clusterBytes, pathBytes, nsBytes);
    byte[][] splits = HdfsStatsKeyConverter.splitHdfsStatsKey(key);
    assertEquals(splits.length, HdfsConstants.NUM_HDFS_USAGE_ROWKEY_COMPONENTS);
    assertTrue(Bytes.equals(splits[0], nowBytes));
    assertTrue(Bytes.equals(splits[1], clusterBytes));
    assertTrue(Bytes.equals(splits[2], pathBytes));
    assertTrue(Bytes.equals(splits[3], nsBytes));
  }

}
