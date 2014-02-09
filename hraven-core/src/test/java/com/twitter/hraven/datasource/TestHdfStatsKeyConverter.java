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

import com.twitter.hraven.HdfsStatsKey;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 */
public class TestHdfStatsKeyConverter {
  @Test
  public void testHdfsStatsKey() throws Exception {
    HdfsStatsKeyConverter conv = new HdfsStatsKeyConverter();

    long now = System.currentTimeMillis();
    HdfsStatsKey key1 = new HdfsStatsKey("cluster1", "/dir1/dir2", now);
    System.out.println(" key1 :" + key1.toString());

    byte[] key1Bytes = conv.toBytes(key1);
    System.out.println(" after toBytes " + Bytes.toString(key1Bytes));
    HdfsStatsKey key2 = conv.fromBytes(key1Bytes);
    assertNotNull(key2);
    System.out.println(" key :" + key2.toString());
    assertEquals(key1.getEncodedRunId(), key2.getEncodedRunId());
    assertEquals(key1.getQualifiedPathKey().getCluster(), key2.getQualifiedPathKey().getCluster());
    assertEquals(key1.getQualifiedPathKey().getPath(), key2.getQualifiedPathKey().getPath());
    assertEquals(key1.getQualifiedPathKey(), key2.getQualifiedPathKey());
  }
}
