package com.twitter.hraven;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TestSparkJobDescFactory {

  @Test
  public void testCreation() {
    String username = "testuser";
    String jobName1 = "sample spark job";

    Configuration c = new Configuration();
    c.set("mapreduce.job.name", jobName1);
    c.set("mapreduce.job.user.name", username);

    SparkJobDescFactory factory = new SparkJobDescFactory();
    QualifiedJobId jobId = new QualifiedJobId("test@local", "spark_201206010000_0001");
    long runId = 1415407334L;
    JobDesc desc = factory.create(jobId, runId, c);
    assertNotNull(desc);
    assertEquals(jobId, desc.getQualifiedJobId());
    
    assertEquals("spark_201206010000_0001", desc.getJobId());
    assertEquals(username, desc.getUserName());
    assertEquals(jobName1, desc.getAppId());
    assertEquals("", desc.getVersion());
    assertEquals(Framework.SPARK, desc.getFramework());
    assertEquals(runId, desc.getRunId());
  }
}
