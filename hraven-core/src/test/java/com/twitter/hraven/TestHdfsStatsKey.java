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
package com.twitter.hraven;

import org.junit.Test;

import com.twitter.hraven.HdfsStatsKey;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * tests the {@link HdfsStatsKeyConverter} class
 */
public class TestHdfsStatsKey {

  private static final String cluster1 = "cluster1";
  private static final String path1 = "path1";
  private static final long now1 =  Long.MAX_VALUE - System.currentTimeMillis()/1000;

  @Test
  public void testConstructor1() throws Exception {

    HdfsStatsKey key1 = new HdfsStatsKey(cluster1, path1, now1);
    testKeyComponents(key1);
    assertEquals(key1.getQualifiedPathKey(), new QualifiedPathKey(cluster1, path1));
  }

  private void testKeyComponents( HdfsStatsKey key1) {
    assertNotNull(key1);
    assertEquals(key1.getEncodedRunId(), now1);
    assertEquals(key1.getQualifiedPathKey().getCluster(), cluster1);
    assertEquals(key1.getQualifiedPathKey().getPath(), path1);
  }

  @Test
  public void testConstructor2() throws Exception {
    QualifiedPathKey qk = new QualifiedPathKey(cluster1, path1);
    HdfsStatsKey key1 = new HdfsStatsKey(qk, now1);
    testKeyComponents(key1);
    assertEquals(key1.getQualifiedPathKey(), qk);
  }

  @Test
  public void testGetRunId() {
    long ts = 1392217200L;
    HdfsStatsKey key1 = new HdfsStatsKey(cluster1, path1, (Long.MAX_VALUE - ts));
    assertEquals(key1.getRunId(), ts);
  }

  @Test
  public void testCompareTo() {
    long ts = 1392217200L;
    HdfsStatsKey key1 = new HdfsStatsKey(cluster1, path1, (Long.MAX_VALUE - ts));
    HdfsStatsKey key2 = new HdfsStatsKey(cluster1, path1, (Long.MAX_VALUE - ts - 10000));
    assertTrue(key1.compareTo(key2) > 0);

    key2 = new HdfsStatsKey(cluster1, path1, (Long.MAX_VALUE - ts + 10000));
    assertTrue(key1.compareTo(key2) < 0);

    key2 = new HdfsStatsKey(cluster1, path1, (Long.MAX_VALUE - ts));
    assertTrue(key1.compareTo(key2) == 0);
  }

  @Test
  public void testEqualsHashCode() {
    long ts = 1392217200L;
    HdfsStatsKey key1 = new HdfsStatsKey(cluster1, path1, (Long.MAX_VALUE - ts));
    HdfsStatsKey key2 = new HdfsStatsKey(cluster1, path1, (Long.MAX_VALUE - ts - 10000));
    assertFalse(key1.equals(key2));
    assertTrue(key1.hashCode() != key2.hashCode());

    key2 = new HdfsStatsKey(cluster1, path1, (Long.MAX_VALUE - ts));
    assertTrue(key1.equals(key2));
    assertEquals(key1.hashCode(), key2.hashCode());
  }
}