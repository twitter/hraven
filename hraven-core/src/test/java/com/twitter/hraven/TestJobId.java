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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.twitter.hraven.JobId;
import com.twitter.hraven.datasource.JobIdConverter;

/**
 * Tests for JobId and JobIdConverter
 */
public class TestJobId {

  /**
   * Validates job ID parsing and serialization/deserialization to and from bytes.
   */
  @Test
  public void testSerialization() {
    long epoch = 20120101000000L;
    String job1 = "job_"+epoch+"_0001";
    String job2 = "job_"+epoch+"_1111";
    String job3 = "job_"+epoch+"_2222";
    String job4 = "job_"+epoch+"_11111";
    String job5 = "spark_"+epoch+"_11112";
    String job6 = "spark_"+epoch+"_11113";

    JobId jobId1 = new JobId(job1);
    assertEquals(epoch, jobId1.getJobEpoch());
    assertEquals(1L, jobId1.getJobSequence());
    assertEquals(job1, jobId1.getJobIdString());

    JobId jobId2 = new JobId(job2);
    assertEquals(epoch, jobId2.getJobEpoch());
    assertEquals(1111L, jobId2.getJobSequence());
    assertEquals(job2, jobId2.getJobIdString());
    // check Comparable implementation
    assertTrue(jobId1.compareTo(jobId2) < 0);

    JobId jobId3 = new JobId(job3);
    assertEquals(epoch, jobId3.getJobEpoch());
    assertEquals(2222L, jobId3.getJobSequence());
    assertEquals(job3, jobId3.getJobIdString());
    // check Comparable implementation
    assertTrue(jobId2.compareTo(jobId3) < 0);

    JobId jobId4 = new JobId(job4);
    assertEquals(epoch, jobId4.getJobEpoch());
    assertEquals(11111L, jobId4.getJobSequence());
    assertEquals(job4, jobId4.getJobIdString());
    // check Comparable implementation
    assertTrue(jobId3.compareTo(jobId4) < 0);

    JobId jobId5 = new JobId(job5);
    assertEquals(epoch, jobId5.getJobEpoch());
    assertEquals(11112L, jobId5.getJobSequence());
    assertEquals(job5, jobId5.getJobIdString());

    JobId jobId6 = new JobId(job6);
    assertEquals(epoch, jobId6.getJobEpoch());
    assertEquals(11113L, jobId6.getJobSequence());
    assertEquals(job6, jobId6.getJobIdString());
    // check Comparable implementation
    assertTrue(jobId5.compareTo(jobId6) < 0);

    JobIdConverter conv = new JobIdConverter();
    JobId tmp = conv.fromBytes( conv.toBytes(jobId1) );
    assertEquals(jobId1, tmp);
    // check hasCode
    assertEquals(jobId1.hashCode(), tmp.hashCode());
    tmp = conv.fromBytes( conv.toBytes(jobId2) );
    assertEquals(jobId2, tmp);
    assertEquals(jobId2.hashCode(), tmp.hashCode());
    tmp = conv.fromBytes( conv.toBytes(jobId3) );
    assertEquals(jobId3, tmp);
    assertEquals(jobId3.hashCode(), tmp.hashCode());
    tmp = conv.fromBytes( conv.toBytes(jobId4) );
    assertEquals(jobId4, tmp);
    assertEquals(jobId4.hashCode(), tmp.hashCode());
    tmp = conv.fromBytes( conv.toBytes(jobId5) );
    assertEquals(jobId5, tmp);
    assertEquals(jobId5.hashCode(), tmp.hashCode());
    tmp = conv.fromBytes( conv.toBytes(jobId6) );
    assertEquals(jobId6, tmp);
    assertEquals(jobId6.hashCode(), tmp.hashCode());
  }

  /**
   * Verifies that JobKey comparisons and byte[] encoded job keys order
   * correctly.
   */
  @Test
  public void testJobIdOrdering() {
    String job1 = "job_20120101000000_0001";
    String job2 = "job_20120101000000_1111";
    String job3 = "job_20120101000000_2222";
    String job4 = "job_20120101000000_11111";
    String job5 = "job_20120201000000_0001";
    String job6 = "spark_20120101000000_11111";
    String job7 = "spark_20120201000000_0001";

    JobId jobId1 = new JobId(job1);
    JobId jobId2 = new JobId(job2);
    JobId jobId3 = new JobId(job3);
    JobId jobId4 = new JobId(job4);
    JobId jobId5 = new JobId(job5);
    JobId jobId6 = new JobId(job6);
    JobId jobId7 = new JobId(job7);

    JobIdConverter conv = new JobIdConverter();
    byte[] job1Bytes = conv.toBytes(jobId1);
    byte[] job2Bytes = conv.toBytes(jobId2);
    byte[] job3Bytes = conv.toBytes(jobId3);
    byte[] job4Bytes = conv.toBytes(jobId4);
    byte[] job5Bytes = conv.toBytes(jobId5);
    byte[] job6Bytes = conv.toBytes(jobId6);
    byte[] job7Bytes = conv.toBytes(jobId7);

    assertTrue(Bytes.compareTo(job1Bytes, job2Bytes) < 0);
    assertTrue(Bytes.compareTo(job2Bytes, job3Bytes) < 0);
    assertTrue(Bytes.compareTo(job3Bytes, job4Bytes) < 0);
    assertTrue(Bytes.compareTo(job4Bytes, job5Bytes) < 0);
    assertTrue(Bytes.compareTo(job6Bytes, job7Bytes) < 0);
  }
}
