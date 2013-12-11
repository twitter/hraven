/*
Copyright 2013 Twitter, Inc.

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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Test;

import com.google.common.io.Files;
import com.twitter.hraven.JobDesc;
import com.twitter.hraven.JobDescFactory;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.QualifiedJobId;
import com.twitter.hraven.datasource.JobHistoryService;
import com.twitter.hraven.datasource.ProcessingException;

public class TestJobHistoryFileParserHadoop1 {

  @Test(expected = ProcessingException.class)
  public void testNullMegaByteMills() {
    JobHistoryFileParserHadoop1 historyFileParser1 = new JobHistoryFileParserHadoop1();
    long mbMillis = historyFileParser1.getMegaByteMillis(null);
    assertNull(mbMillis);
  }

  @Test
  public void testMegaByteMillis() throws IOException {

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

    // now load the conf file and check
    final String JOB_CONF_FILENAME = "src/test/resources/job_1329348432655_0001_conf.xml";

    Configuration jobConf = new Configuration();
    jobConf.addResource(new Path(JOB_CONF_FILENAME));
    QualifiedJobId qualifiedJobId = new QualifiedJobId("cluster1", "job_1329348432655_0001");
    Long submitTimeMillis = 1329348443227L;
    JobDesc jobDesc = JobDescFactory.createJobDesc(qualifiedJobId, submitTimeMillis, jobConf);

    List<Put> jobConfPuts = JobHistoryService.getHbasePuts(jobDesc, jobConf);
    assertNotNull(jobConfPuts);
    int expSize = 1;
    assertEquals(expSize, jobConfPuts.size());
    Long mbMillis = historyFileParser.getMegaByteMillis(jobConf);
    Long expValue = 2981062L;
    assertEquals(expValue, mbMillis);
  }

  @Test(expected=ProcessingException.class)
  public void testIncorrectGetXmxValue(){
    JobHistoryFileParserHadoop1 historyFileParser1 = new JobHistoryFileParserHadoop1();
    String xmxValue = "-XmxSOMETHINGWRONG!";
    @SuppressWarnings("unused")
    long val = historyFileParser1.getXmxValue(xmxValue);
  }

  @Test(expected=ProcessingException.class)
  public void testNullGetXmxValue(){
    JobHistoryFileParserHadoop1 historyFileParser1 = new JobHistoryFileParserHadoop1();
    String xmxValue = null;
    @SuppressWarnings("unused")
    long val = historyFileParser1.getXmxValue(xmxValue);
  }

  @Test
  public void testGetXmxValue(){
    // check for megabyte value itself
    String xmxValue = "-Xmx500m";
    long expValue = 500;
    JobHistoryFileParserHadoop1 historyFileParser1 = new JobHistoryFileParserHadoop1();
    long actualValue = historyFileParser1.getXmxValue(xmxValue);
    assertEquals(expValue, actualValue);
    long totalValue = historyFileParser1.getXmxTotal(actualValue);
    assertEquals(666L, totalValue);

    // check if megabytes is returned for kilobytes
    xmxValue = "-Xmx2048K";
    actualValue = historyFileParser1.getXmxValue(xmxValue);
    expValue = 2L;
    assertEquals(expValue, actualValue);
    totalValue = historyFileParser1.getXmxTotal(actualValue);
    long expTotalVal = 2L;
    assertEquals(expTotalVal, totalValue);

    // check if megabytes is returned for gigabytes
    xmxValue = "-Xmx2G";
    actualValue = historyFileParser1.getXmxValue(xmxValue);
    expValue = 2048;
    assertEquals(expValue, actualValue);
    totalValue = historyFileParser1.getXmxTotal(actualValue);
    expTotalVal = 2730L;
    assertEquals(expTotalVal, totalValue);

    // check if megabytes is returned for bytes
    xmxValue = "-Xmx2097152";
    actualValue = historyFileParser1.getXmxValue(xmxValue);
    expValue = 2L;
    assertEquals(expValue, actualValue);
    totalValue = historyFileParser1.getXmxTotal(actualValue);
    expTotalVal = 2L;
    assertEquals(expTotalVal, totalValue);

    xmxValue = " -Xmx1024m -verbose:gc -Xloggc:/tmp/@taskid@.gc" ;
    actualValue = historyFileParser1.getXmxValue(xmxValue);
    expValue = 1024L;
    assertEquals(expValue, actualValue);
    totalValue = historyFileParser1.getXmxTotal(actualValue);
    expTotalVal = 1365L;
    assertEquals(expTotalVal, totalValue);
  }

  @Test
  public void testExtractXmxValue() {
    String jc = " -Xmx1024m -verbose:gc -Xloggc:/tmp/@taskid@.gc" ;
    JobHistoryFileParserHadoop1 historyFileParser1 = new JobHistoryFileParserHadoop1();
    String valStr = historyFileParser1.extractXmxValueStr(jc);
    String expStr = "1024m";
    assertEquals(expStr, valStr);
  }

  @Test(expected=ProcessingException.class) 
  public void testExtractXmxValueIncorrectInput(){
    String jc = " -Xmx" ;
    JobHistoryFileParserHadoop1 historyFileParser1 = new JobHistoryFileParserHadoop1();
    String valStr = historyFileParser1.extractXmxValueStr(jc);
    String expStr = "";
    assertEquals(expStr, valStr);

  }
}