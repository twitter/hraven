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

import com.twitter.hraven.datasource.JobIdConverter;
import com.twitter.hraven.datasource.JobKeyConverter;
import com.twitter.hraven.util.ByteUtil;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

/**
 * Test the JobKey class.
 */
public class TestJobKey {
  private static Log LOG = LogFactory.getLog(TestJobKey.class);

  /**
   * Confirm that we can properly serialize and deserialize a JobKey.
   */
  @Test
  public void testKeySerialization() {
    JobKeyConverter conv = new JobKeyConverter();
    JobKey key = new JobKey("cluster1@identifier1", "user1", "app1", 13, "job_20120101235959_0001");
    byte[] keyBytes = conv.toBytes(key);
    JobKey key2 = conv.fromBytes(keyBytes);
    assertEquals(key.getCluster(), key2.getCluster());
    assertEquals(key.getUserName(), key2.getUserName());
    assertEquals(key.getAppId(), key2.getAppId());
    assertEquals(key.getRunId(), key2.getRunId());
    assertEquals(key.getJobId(), key2.getJobId());

    // also verify that the runId gets inverted in the serialized byte
    // representation
    byte[][] keyParts = ByteUtil.split(keyBytes, Constants.SEP_BYTES);
    assertEquals(5, keyParts.length);
    long encodedRunId = Bytes.toLong(keyParts[3]);
    assertEquals(key.getRunId(), Long.MAX_VALUE - encodedRunId);

    // test partial keys
    key = new JobKey("c1@local", "user1", "app1", 15, (String)null);
    keyBytes = conv.toBytes(key);
    key2 = conv.fromBytes(keyBytes);
    assertEquals(key.getCluster(), key2.getCluster());
    assertEquals(key.getUserName(), key2.getUserName());
    assertEquals(key.getAppId(), key2.getAppId());
    assertEquals(key.getRunId(), key2.getRunId());
    assertEquals(key.getJobId(), key2.getJobId());

    // key with no trailing job Id
    keyBytes =
        ByteUtil.join(Constants.SEP_BYTES, Bytes.toBytes("c1@local"), Bytes.toBytes("user1"),
          Bytes.toBytes("app1"), Bytes.toBytes(Long.MAX_VALUE - 15L));
    key2 = conv.fromBytes(keyBytes);
    assertEquals("c1@local", key2.getCluster());
    assertEquals("user1", key2.getUserName());
    assertEquals("app1", key2.getAppId());
    assertEquals(15L, key2.getRunId());
    assertEquals(0L, key2.getJobId().getJobEpoch());
    assertEquals(0L, key2.getJobId().getJobSequence());


    // key with empty appId
    key = new JobKey("c1@local", "user1", "", 1234L, "job_201206201718_1941");
    keyBytes = conv.toBytes(key);
    key2 = conv.fromBytes(keyBytes);
    assertKey(key, key2);
  }

  /**
   * Confirm that we can properly serialize and deserialize
   * a JobKey for spark jobs
   * Important since "spark" prefix will be stored in keys
   */
  @Test
  public void testKeySerializationSpark() {
    JobKeyConverter conv = new JobKeyConverter();
    JobKey key = new JobKey("clusterS1@identifierS1",
      "userS1",
      "appSpark1",
      13,
      "spark_1413515656084_3051855");
    byte[] keyBytes = conv.toBytes(key);
    JobKey key2 = conv.fromBytes(keyBytes);
    assertEquals(key.getCluster(), key2.getCluster());
    assertEquals(key.getUserName(), key2.getUserName());
    assertEquals(key.getAppId(), key2.getAppId());
    assertEquals(key.getRunId(), key2.getRunId());
    assertEquals(key.getJobId(), key2.getJobId());

    // also verify that the runId gets inverted in the serialized byte
    // representation
    byte[][] keyParts = ByteUtil.split(keyBytes, Constants.SEP_BYTES);
    assertEquals(5, keyParts.length);
    long encodedRunId = Bytes.toLong(keyParts[3]);
    assertEquals(key.getRunId(), Long.MAX_VALUE - encodedRunId);

    // key with empty appId
    key = new JobKey("c1@local", "user1", "", 1234L, "spark_1413515656084_3051855");
    keyBytes = conv.toBytes(key);
    key2 = conv.fromBytes(keyBytes);
    assertKey(key, key2);

    JobKey jobKey = new JobKey("cluster1",
      "user1",
      "com.example.spark_program.simple_example.Main$",
      13,
      // this job id has the seperator in its byte representation
      // hence check for this id
      "spark_1413515656084_305185");
    keyBytes = conv.toBytes(jobKey);
    key2 = conv.fromBytes(keyBytes);
    assertEquals(jobKey.getCluster(), key2.getCluster());
    assertEquals(jobKey.getUserName(), key2.getUserName());
    assertEquals(jobKey.getAppId(), key2.getAppId());
    assertEquals(jobKey.getRunId(), key2.getRunId());
    assertEquals(jobKey.getJobId(), key2.getJobId());

    }

  @Test
  public void checkOldFormatKey() {
    // key with old format job id bytes stored, i.e. without the "job_" prefix
    long epoch = 1415332804102L;
    long seq = 10223L;
    byte[] jobidarr = ByteUtil.join(Constants.EMPTY_BYTES,
                Bytes.toBytes(epoch),
                Bytes.toBytes(seq));
    byte[] keyBytes = ByteUtil.join(Constants.SEP_BYTES,
          Bytes.toBytes("c1@local"),
          Bytes.toBytes("user1"),
          Bytes.toBytes("app1"),
          Bytes.toBytes(Long.MAX_VALUE - 15L),
          jobidarr);
    JobKeyConverter conv = new JobKeyConverter();

    JobKey key2 = conv.fromBytes(keyBytes);
    assertEquals("c1@local", key2.getCluster());
    assertEquals("user1", key2.getUserName());
    assertEquals("app1", key2.getAppId());
    assertEquals(15L, key2.getRunId());
    assertEquals(epoch, key2.getJobId().getJobEpoch());
    assertEquals(seq, key2.getJobId().getJobSequence());

  }

  public void assertKey(JobKey expected, JobKey actual) {
    assertEquals(expected.getCluster(), actual.getCluster());
    assertEquals(expected.getUserName(), actual.getUserName());
    assertEquals(expected.getAppId(), actual.getAppId());
    assertEquals(expected.getRunId(), actual.getRunId());
    assertEquals(expected.getJobId(), actual.getJobId());
    assertEquals(expected.hashCode(),actual.hashCode());
  }

  /**
   * Confirm that leading and trailing spaces get ripped off.
   */
  @Test
  public void testPlainConstructor() {
    JobKeyConverter conv = new JobKeyConverter();
    JobKey key = new JobKey("cluster2@identifier2 ", "user2 ", "appSpace ", 17,
        " job_20120101235959_1111 ");
    byte[] keyBytes = conv.toBytes(key);
    JobKey key2 = conv.fromBytes(keyBytes);
    assertEquals(key.getUserName(), key2.getUserName());
    assertEquals(key.getAppId(), key2.getAppId());
    assertEquals(key.getRunId(), key2.getRunId());
    assertEquals(key.getJobId(), key2.getJobId());

    assertEquals("cluster2@identifier2", key.getCluster());
    assertEquals("user2", key.getUserName());
    assertEquals("appSpace", key.getAppId());
    assertEquals(17, key.getRunId());
    assertEquals("job_20120101235959_1111", key.getJobId().getJobIdString());

    // test for spark job keys
    key = new JobKey("clusterS2@identifierS2 ", "userS2 ", "appSpaceS ", 17,
        " spark_20120101235959_1111 ");
    keyBytes = conv.toBytes(key);
    key2 = conv.fromBytes(keyBytes);
    assertEquals(key.getUserName(), key2.getUserName());
    assertEquals(key.getAppId(), key2.getAppId());
    assertEquals(key.getRunId(), key2.getRunId());
    assertEquals(key.getJobId(), key2.getJobId());

    assertEquals("clusterS2@identifierS2", key.getCluster());
    assertEquals("userS2", key.getUserName());
    assertEquals("appSpaceS", key.getAppId());
    assertEquals(17, key.getRunId());
    assertEquals("spark_20120101235959_1111", key.getJobId().getJobIdString());

  }

  /**
   * Confirm that leading and trailing spaces get ripped off.
   */
  @Test
  public void testJobDescConstructor() {
    JobKeyConverter conv = new JobKeyConverter();
    JobDesc jobDesc = new JobDesc("cluster2@identifier3 ", "user3 ",
        "appSpace ", "spaceVersion3 ", 19, " job_20120101235959_1111 ", Framework.NONE);
    JobKey key = new JobKey(jobDesc);
    byte[] keyBytes = conv.toBytes(key);
    JobKey key2 = conv.fromBytes(keyBytes);
    assertEquals(key.getUserName(), key2.getUserName());
    assertEquals(key.getAppId(), key2.getAppId());
    assertEquals(key.getRunId(), key2.getRunId());
    assertEquals(key.getJobId(), key2.getJobId());

    assertEquals("user3", key.getUserName());
    assertEquals("cluster2@identifier3", key.getCluster());
    assertEquals("appSpace", key.getAppId());
    assertEquals(19, key.getRunId());
    assertEquals("job_20120101235959_1111", key.getJobId().getJobIdString());
  }

  /**
   * Confirm that leading and trailing spaces get ripped off for
   * spark job keys as well
   */
  @Test
  public void testJobDescConstructorSparkJobKeys() {
    JobKeyConverter conv = new JobKeyConverter();
    JobDesc jobDesc = new JobDesc("cluster2@identifier3 ",
        "user3Spark ",
        "appSpaceSpark     ",
        "spaceVersion3 ", 19,
        "      spark_1413515656084_3051855 ",
        Framework.SPARK);
    JobKey key = new JobKey(jobDesc);
    byte[] keyBytes = conv.toBytes(key);
    JobKey key2 = conv.fromBytes(keyBytes);
    assertEquals(key.getUserName(), key2.getUserName());
    assertEquals(key.getAppId(), key2.getAppId());
    assertEquals(key.getRunId(), key2.getRunId());
    assertEquals(key.getJobId(), key2.getJobId());

    assertEquals("user3Spark", key.getUserName());
    assertEquals("cluster2@identifier3", key.getCluster());
    assertEquals("appSpaceSpark", key.getAppId());
    assertEquals(19, key.getRunId());
    assertEquals("spark_1413515656084_3051855", key.getJobId().getJobIdString());
  }

  /**
   * Checks for correct parsing of job key when run ID may contain the byte
   * representation of the separator character.
   */
  @Test
  public void testEncodedRunId() {
    JobKeyConverter conv = new JobKeyConverter();
    long now = System.currentTimeMillis();
    byte[] encoded = Bytes.toBytes(Long.MAX_VALUE - now);
    // replace last byte with separator and reconvert to long
    Bytes.putBytes(encoded, encoded.length-Constants.SEP_BYTES.length,
        Constants.SEP_BYTES, 0, Constants.SEP_BYTES.length);
    long badId = Long.MAX_VALUE - Bytes.toLong(encoded);
    LOG.info("Bad run ID is "+badId);

    // assemble a job key with the bad run ID
    JobIdConverter idConv = new JobIdConverter();
    byte[] encodedKey = ByteUtil.join(Constants.SEP_BYTES,
      Bytes.toBytes("c1S@local"),
      Bytes.toBytes("userS31"),
      Bytes.toBytes("spark_app1"),
      encoded,
      idConv.toBytes(new JobId("spark_1413515656084_51855")));

    JobKey key = conv.fromBytes(encodedKey);
    assertEquals("c1S@local", key.getCluster());
    assertEquals("userS31", key.getUserName());
    assertEquals("spark_app1", key.getAppId());
    assertEquals("spark_1413515656084_51855", key.getJobId().getJobIdString());

    // assemble a spark job key with the bad run ID
    encodedKey = ByteUtil.join(Constants.SEP_BYTES,
      Bytes.toBytes("c1S@local"),
      Bytes.toBytes("userS31"),
      Bytes.toBytes("spark_app1"),
      encoded,
      idConv.toBytes(new JobId("spark_1413515656084_51855")));

    key = conv.fromBytes(encodedKey);
    assertEquals("c1S@local", key.getCluster());
    assertEquals("userS31", key.getUserName());
    assertEquals("spark_app1", key.getAppId());
    assertEquals("spark_1413515656084_51855", key.getJobId().getJobIdString());

  }

  @Test
  public void testEncodedRunIdDifferentFormats() {
    JobKeyConverter conv = new JobKeyConverter();
    long now = System.currentTimeMillis();
    byte[] encoded = Bytes.toBytes(Long.MAX_VALUE - now);
    // replace last byte with separator and reconvert to long
    Bytes.putBytes(encoded, encoded.length-Constants.SEP_BYTES.length,
        Constants.SEP_BYTES, 0, Constants.SEP_BYTES.length);
    long badId = Long.MAX_VALUE - Bytes.toLong(encoded);
    LOG.info("Bad run ID is "+badId);

    // create a packed byte array for
    // jobid without the "job_" prefix
    long epoch = 1415332804102L;
    long seq = 10223L;
    byte[] jobidarr = ByteUtil.join(Constants.EMPTY_BYTES,
                Bytes.toBytes(epoch),
                Bytes.toBytes(seq));

    // assemble a job key with the bad run ID
    byte[] encodedKey = ByteUtil.join(Constants.SEP_BYTES,
        Bytes.toBytes("c1@local"),
        Bytes.toBytes("user1"),
        Bytes.toBytes("app1"),
        encoded,
        jobidarr);

    JobKey key = conv.fromBytes(encodedKey);
    assertEquals("c1@local", key.getCluster());
    assertEquals("user1", key.getUserName());
    assertEquals("app1", key.getAppId());
    assertEquals("job_1415332804102_10223", key.getJobId().getJobIdString());

    // create a packed byte array for
    // jobid with "spark" prefix
    epoch = 1415332804102L;
    seq = 10223L;
    jobidarr = ByteUtil.join(Constants.EMPTY_BYTES,
                Bytes.toBytes("spark"),
                Bytes.toBytes(epoch),
                Bytes.toBytes(seq));

    // assemble a job key with the bad run ID
    encodedKey = ByteUtil.join(Constants.SEP_BYTES,
        Bytes.toBytes("sc1@local"),
        Bytes.toBytes("spark_user1"),
        Bytes.toBytes("sparkApp1"),
        encoded,
        jobidarr);

    key = conv.fromBytes(encodedKey);
    assertEquals("sc1@local", key.getCluster());
    assertEquals("spark_user1", key.getUserName());
    assertEquals("sparkApp1", key.getAppId());
    assertEquals("spark_1415332804102_10223", key.getJobId().getJobIdString());
  }

  @Test
  public void testOrdering() {
    JobKey key1 = new JobKey("c1@local", "auser", "app", 1234L, "job_20120101000000_0001");
    JobKey key2 = new JobKey("c1@local", "auser", "app", 1234L, "job_20120101000000_2222");
    JobKey key3 = new JobKey("c1@local", "auser", "app", 1234L, "job_20120101000000_11111");
    JobKey key4 = new JobKey("c1@local", "auser", "app", 1345L, "job_20120101000000_0001");

    JobKeyConverter conv = new JobKeyConverter();
    byte[] key1Bytes = conv.toBytes(key1);
    byte[] key2Bytes = conv.toBytes(key2);
    byte[] key3Bytes = conv.toBytes(key3);
    byte[] key4Bytes = conv.toBytes(key4);

    // highest run ID should sort first
    assertTrue(Bytes.compareTo(key4Bytes, key1Bytes) < 0);
    // job IDs should sort in numeric order
    assertTrue(Bytes.compareTo(key1Bytes, key2Bytes) < 0);
    assertTrue(Bytes.compareTo(key2Bytes, key3Bytes) < 0);
  }

  @Test
  public void testToString() {
    JobKey key = new JobKey("c1@local", "auser", "app", 1345L, "job_20120101000000_0001");
    String expected = "c1@local" + Constants.SEP + "auser"
        + Constants.SEP + "app" + Constants.SEP + 1345L
        + Constants.SEP +  "job_20120101000000_0001";
    assertEquals(expected, key.toString());
  }
}
