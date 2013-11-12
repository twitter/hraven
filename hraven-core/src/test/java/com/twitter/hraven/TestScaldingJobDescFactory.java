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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.twitter.hraven.Constants;
import com.twitter.hraven.Framework;
import com.twitter.hraven.JobDesc;
import com.twitter.hraven.QualifiedJobId;
import com.twitter.hraven.ScaldingJobDescFactory;
import com.twitter.hraven.util.DateUtil;

/**
 * Tests for parsing out {@link JobDesc} fields from Scalding/Cascading/Cascalog
 * job configurations.
 */
public class TestScaldingJobDescFactory {
  private static Log LOG = LogFactory.getLog(TestScaldingJobDescFactory.class);

  private String jobName1 = "[(2/5) ...\"SequenceFile[['?key', '?long-struct']]\"][c0480258-4c53-4750-9af7-3/35621/]]";
  private String expectedAppId1 = "";

  private String jobName2 = "null[(5/10) ...fs[\"SequenceFile[['x_bin', 'group', 'y', 'size']]\"][_pipe_0__pipe_1/77800/]]";
  private String expectedAppId2 = "null";

  private String jobName3 = "[CCF12D90878A253056D8EBA5B683E0F6/195CB9890845DBD83E1236468413498B] com.example.somepkg.abc/(12/18) ...Out/done_marker.summ.temp";
  private String expectedAppId3 = "com.example.somepkg.abc";

  private String jobName4 = "[B2488AD568D8E966B3A4436326B9D7C9/9647E82ED91D4745BD9E19E5CFA22D1D] com.example.xyz.AbcPqr/(5/8)";
  private String expectedAppId4 = "com.example.xyz.AbcPqr";

  private String jobName5 = "com.example.xyz.AbcPqrst[(1/1) ...tuvwxyz/12345678/features/../verification_NEGATIVE\"]\"]]";
  private String expectedAppId5 = "com.example.xyz.AbcPqrst";

  @Test
  public void testAppIdParsing() throws Exception {
    /*
     * Sample config that was producing an empty app ID
     *
     *  i:c!cascading.app.id                     value=93543E9B29A4CD14E0556849A93E171B
     *  i:c!cascading.app.name                   value=abcde
     *  i:c!cascading.app.version                value=1.2.0-SNAPSHOT-standalone
     *  i:c!cascading.flow.id                    value=0A2CEB3C5DFB905802EB96D1AE0C04D5
     *  i:c!mapred.job.name                      value=[(1/5) ...ms', '?xyzid', '?normal', '?domain']]"]]
     *  i:c!user.name                            value=userName1
     */
    String username = "testuser";
    Configuration c = new Configuration();
    c.set("mapred.job.name", jobName1);
    c.set("user.name", username);
    c.set("cascading.app.id", "93543E9B29A4CD14E0556849A93E171B");
    c.set("cascading.flow.id", "0A2CEB3C5DFB905802EB96D1AE0C04D5");

    ScaldingJobDescFactory factory = new ScaldingJobDescFactory();
    QualifiedJobId jobId = new QualifiedJobId("test@local", "job_201206010000_0001");
    long now = System.currentTimeMillis();
    JobDesc desc = factory.create(jobId, now, c);
    assertJobDesc(desc, jobId, username, "93543E9B29A4CD14E0556849A93E171B");

    c.set("mapred.job.name", jobName2);
    desc = factory.create(jobId, now, c);
    assertJobDesc(desc, jobId, username, expectedAppId2);

    c.set("mapred.job.name", jobName3);
    desc = factory.create(jobId, now, c);
    assertJobDesc(desc, jobId, username, expectedAppId3);

    c.set("mapred.job.name", jobName4);
    desc = factory.create(jobId, now, c);
    assertJobDesc(desc, jobId, username, expectedAppId4);

    c.set("mapred.job.name", jobName5);
    desc = factory.create(jobId, now, c);
    assertJobDesc(desc, jobId, username, expectedAppId5);
  }

  private void assertJobDesc(JobDesc desc, QualifiedJobId expectedJobId, String expectedUser, String expectedAppId) {
    assertNotNull(desc);
    assertEquals(expectedJobId, desc.getQualifiedJobId());
    assertEquals(expectedUser, desc.getUserName());
    assertEquals(expectedAppId, desc.getAppId());
    assertEquals("", desc.getVersion());
    assertEquals(Framework.SCALDING, desc.getFramework());
    // TODO: test setting of run ID
  }

  @Test
  public void testStripAppId() throws Exception {

    ScaldingJobDescFactory factory = new ScaldingJobDescFactory();
    assertEquals(expectedAppId1, factory.stripAppId(jobName1));
    assertEquals(expectedAppId2, factory.stripAppId(jobName2));
    assertEquals(expectedAppId3, factory.stripAppId(jobName3));
    assertEquals(expectedAppId4, factory.stripAppId(jobName4));
    assertEquals(expectedAppId5, factory.stripAppId(jobName5));
  }

  @Test
  public void testRunIdParsing() throws Exception {
 //   String username = "testuser";
    Configuration c = new Configuration();
    c.set("mapred.job.name", jobName1);
    c.set("cascading.app.id", "93543E9B29A4CD14E0556849A93E171B");
    c.set("cascading.flow.id", "0A2CEB3C5DFB905802EB96D1AE0C04D5");
    c.set("user.name", "testuser");

    ScaldingJobDescFactory factory = new ScaldingJobDescFactory();
    QualifiedJobId jobId = new QualifiedJobId("test@local", "job_201206010000_0001");
    long now = System.currentTimeMillis();
    LOG.info("Using "+now+" as submit time");

    // check runId setting based on flow.id alone
    long expectedMonthStart = DateUtil.getMonthStart(now);
    LOG.info("Month start set to "+expectedMonthStart);
    JobDesc desc = factory.create(jobId, now, c);
    LOG.info("Set runId to "+desc.getRunId());
    assertTrue(expectedMonthStart <= desc.getRunId());
    long monthEnd = expectedMonthStart + DateUtil.MONTH_IN_MILLIS;
    assertTrue("runId should be less than "+monthEnd, monthEnd > desc.getRunId());

    // check runId setting based on set start time
    Configuration c2 = new Configuration(c);
    c2.set(Constants.CASCADING_RUN_CONF_KEY, Long.toString(now));
    desc = factory.create(jobId, now, c2);
    assertEquals(now, desc.getRunId());
  }
}
