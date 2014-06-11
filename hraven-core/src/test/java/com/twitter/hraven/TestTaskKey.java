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
package com.twitter.hraven;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.twitter.hraven.datasource.TaskKeyConverter;

import static org.junit.Assert.assertEquals;

/**
 * Test usage and serialization of TaskKey
 */
public class TestTaskKey {
  private static Log LOG = LogFactory.getLog(TestTaskKey.class);

  @Test
  public void testSerialization() {
    TaskKeyConverter conv = new TaskKeyConverter();

    TaskKey key1 = new TaskKey(
        new JobKey("test@local", "testuser", "app", 1234L, "job_20120101000000_1111"), "m_001");
    assertEquals("test@local", key1.getCluster());
    assertEquals("testuser", key1.getUserName());
    assertEquals("app", key1.getAppId());
    assertEquals(1234L, key1.getRunId());
    assertEquals("job_20120101000000_1111", key1.getJobId().getJobIdString());
    assertEquals("m_001", key1.getTaskId());

    byte[] key1Bytes = conv.toBytes(key1);
    TaskKey key2 = conv.fromBytes(key1Bytes);
    assertKey(key1, key2);

    TaskKey key3 = conv.fromBytes( conv.toBytes(key2) );
    assertKey(key1, key3);

    // test with a run ID containing the separator
    long now = System.currentTimeMillis();
    byte[] encoded = Bytes.toBytes(Long.MAX_VALUE - now);
    // replace last byte with separator and reconvert to long
    Bytes.putBytes(encoded, encoded.length-Constants.SEP_BYTES.length,
        Constants.SEP_BYTES, 0, Constants.SEP_BYTES.length);
    long badId = Long.MAX_VALUE - Bytes.toLong(encoded);
    LOG.info("Bad run ID is " + badId);

    TaskKey badKey1 = new TaskKey(
        new JobKey(key1.getQualifiedJobId(), key1.getUserName(), key1.getAppId(), badId),
        key1.getTaskId());
    byte[] badKeyBytes = conv.toBytes(badKey1);
    TaskKey badKey2 = conv.fromBytes(badKeyBytes);
    assertKey(badKey1, badKey2);
  }

  @Test
  public void testToString() {
    JobKey jKey = new JobKey("test@local", "testuser", "app", 1234L, "job_20120101000000_1111");
    TaskKey key = new TaskKey(jKey, "m_001");
    String expected = jKey.toString() + Constants.SEP + "m_001";
    assertEquals(expected, key.toString());
  }

  private void assertKey(TaskKey expected, TaskKey actual) {
    assertEquals(expected.getCluster(), actual.getCluster());
    assertEquals(expected.getUserName(), actual.getUserName());
    assertEquals(expected.getAppId(), actual.getAppId());
    assertEquals(expected.getRunId(), actual.getRunId());
    assertEquals(expected.getJobId(), actual.getJobId());
    assertEquals(expected.getTaskId(), actual.getTaskId());
    assertEquals(expected.hashCode(),actual.hashCode());
  }
}
