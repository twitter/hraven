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
package com.twitter.hraven.etl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.google.common.io.Files;
import com.twitter.hraven.Constants;
import com.twitter.hraven.JobDesc;
import com.twitter.hraven.JobDescFactory;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.QualifiedJobId;
import com.twitter.hraven.datasource.JobHistoryService;

public class TestJobHistoryFileParserHadoop1 {

  @Test
  public void testPostProcessedPuts() throws IOException {

    final String JOB_HISTORY_FILE_NAME =
        "src/test/resources/job_201311192236_3583_1386370578196_user1_Sleep+job";

    File jobHistoryfile = new File(JOB_HISTORY_FILE_NAME);
    byte[] contents = Files.toByteArray(jobHistoryfile);
    JobHistoryFileParser historyFileParser =
        JobHistoryFileParserFactory.createJobHistoryFileParser(contents);
    assertNotNull(historyFileParser);

    // confirm that we get back an object that can parse hadoop 1.0 files
    assertTrue(historyFileParser instanceof JobHistoryFileParserHadoop1);

    JobKey jobKey = new JobKey("cluster1", "user1", "Sleep", 1, "job_201311192236_3583");
    historyFileParser.parse(contents, jobKey);

    List<Put> jobPuts = historyFileParser.getJobPuts();
    assertNotNull(jobPuts);

    // check post processing puts
    // first with empty job conf puts
    List<Put> postPuts = historyFileParser.generatePostProcessedPuts(new LinkedList<Put>());
    assertNull(postPuts);

    // now load the conf file and check
    final String JOB_CONF_FILENAME = "src/test/resources/job_1329348432655_0001_conf.xml";

    Configuration jobConf = new Configuration();
    jobConf.addResource(new Path(JOB_CONF_FILENAME));
    QualifiedJobId qualifiedJobId = new QualifiedJobId("cluster1",
        "job_1329348432655_0001");
    Long submitTimeMillis = 1329348443227L;
    JobDesc jobDesc = JobDescFactory.createJobDesc(qualifiedJobId,
        submitTimeMillis, jobConf);

    List<Put> jobConfPuts = JobHistoryService.getHbasePuts(jobDesc, jobConf);
    assertNotNull(jobConfPuts);
    int expSize = 1;
    assertEquals(expSize, jobConfPuts.size());
    postPuts = historyFileParser.generatePostProcessedPuts(jobConfPuts);
    assertNotNull(postPuts);
    assertEquals(expSize, postPuts.size());
    Put p = postPuts.get(0);
    byte[] qualifier = Constants.MEGABYTEMILLIS_BYTES;
    Long actualValue = 0L;
    Long expValue = 2981062L;
    assertTrue(p.has(Constants.INFO_FAM_BYTES, qualifier));
    List<KeyValue> kv = p.get(Constants.INFO_FAM_BYTES, qualifier);
    assertEquals(1, kv.size());
    actualValue = Bytes.toLong(kv.get(0).getValue());
    assertEquals(expValue, actualValue);
  }


  @Test
  public void testGetXmxValue(){
    Put p = new Put(Bytes.toBytes("key1"));
    byte[] family = Constants.INFO_FAM_BYTES;
    String qualifier = Constants.JOB_CONF_COLUMN_PREFIX + Constants.SEP +
          Constants.JAVA_CHILD_OPTS_CONF_KEY;
    String xmxValue = "-Xmx500m";
    long expValue = 500;
    p.add(family, Bytes.toBytes(qualifier), Bytes.toBytes(xmxValue));
    List<Put> list1 = new LinkedList<Put>();
    list1.add(p);
    JobHistoryFileParserHadoop1 historyFileParser1 = new JobHistoryFileParserHadoop1();
    long actualValue = historyFileParser1.getXmxValue(list1);
    assertEquals(expValue, actualValue);
    long totalValue = historyFileParser1.getXmxTotal(actualValue);
    assertEquals(666L, totalValue);

    list1.clear();
    p = new Put(Bytes.toBytes("key2"));
    p.add(family, Bytes.toBytes(qualifier), Bytes.toBytes("-Xmx2048"));
    list1.add(p);
    long val = historyFileParser1.getXmxValue(list1);
    long expVal = 2L;
    assertEquals(expVal, val);
    long totalVal = historyFileParser1.getXmxTotal(val);
    long expTotalVal = 2L;
    assertEquals(expTotalVal, totalVal);

    list1.clear();
    p = new Put(Bytes.toBytes("key3"));
    p.add(family, Bytes.toBytes(qualifier), Bytes.toBytes("-XmxSOMETHINGWRONG"));
    list1.add(p);
    val = historyFileParser1.getXmxValue(list1);
    expVal = 0L;
    assertEquals(expVal, val);
    totalVal = historyFileParser1.getXmxTotal(val);
    expTotalVal = 0L;
    assertEquals(expTotalVal, totalVal);
  }
}