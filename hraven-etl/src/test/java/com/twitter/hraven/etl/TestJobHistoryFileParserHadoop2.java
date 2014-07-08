/*
 * Copyright 2013 Twitter, Inc. Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may obtain a copy of the License
 * at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
 * writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package com.twitter.hraven.etl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import com.google.common.io.Files;
import com.twitter.hraven.Constants;
import com.twitter.hraven.HadoopVersion;
import com.twitter.hraven.JobHistoryKeys;
import com.twitter.hraven.JobHistoryRecord;
import com.twitter.hraven.JobHistoryRecordCollection;
import com.twitter.hraven.JobHistoryTaskRecord;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.RecordCategory;
import com.twitter.hraven.RecordDataKey;
import com.twitter.hraven.datasource.JobKeyConverter;
import com.twitter.hraven.datasource.ProcessingException;
import com.twitter.hraven.datasource.TaskKeyConverter;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Test {@link JobHistoryFileParserHadoop2}
 */
public class TestJobHistoryFileParserHadoop2 {

  @Test
  public void testCreateJobHistoryFileParserCorrectCreation() throws IOException {

    final String JOB_HISTORY_FILE_NAME =
        "src/test/resources/job_1329348432655_0001-1329348443227-user-Sleep+job-1329348468601-10-1-SUCCEEDED-default.jhist";

    File jobHistoryfile = new File(JOB_HISTORY_FILE_NAME);
    byte[] contents = Files.toByteArray(jobHistoryfile);
    // now load the conf file and check
    final String JOB_CONF_FILE_NAME =
        "src/test/resources/job_1329348432655_0001_conf.xml";
    Configuration jobConf = new Configuration();
    jobConf.addResource(new Path(JOB_CONF_FILE_NAME));

    JobHistoryFileParser historyFileParser =
        JobHistoryFileParserFactory.createJobHistoryFileParser(contents, jobConf);
    assertNotNull(historyFileParser);

    // confirm that we get back an object that can parse hadoop 2.0 files
    assertTrue(historyFileParser instanceof JobHistoryFileParserHadoop2);

    JobKey jobKey = new JobKey("cluster1", "user", "Sleep", 1, "job_1329348432655_0001");
    historyFileParser.parse(contents, jobKey);

    JobHistoryRecordCollection jobRecords = (JobHistoryRecordCollection)historyFileParser.getJobRecords();
    assertEquals(156, jobRecords.size());

    assertEquals("cluster1!user!Sleep!1!job_1329348432655_0001",
      jobRecords.getKey().toString());

    // check hadoop version
    Object versionRecord =
        jobRecords.getValue(RecordCategory.HISTORY_META, new RecordDataKey(
            JobHistoryKeys.hadoopversion.toString().toLowerCase()));
    
    assertNotNull(versionRecord);
    assertEquals((String)versionRecord, HadoopVersion.TWO.toString());

    // check job status
    Object statusRecord =
        jobRecords.getValue(RecordCategory.HISTORY_META, new RecordDataKey(
            JobHistoryKeys.JOB_STATUS.toString().toLowerCase()));
    
    assertNotNull(statusRecord);
    assertEquals((String)statusRecord, JobHistoryFileParserHadoop2.JOB_STATUS_SUCCEEDED);

    List<JobHistoryTaskRecord> taskRecords = (List<JobHistoryTaskRecord>) historyFileParser.getTaskRecords();
    assertEquals(382, taskRecords.size());

    Set<String> putRowKeys =
        new HashSet<String>(Arrays.asList(
          "cluster1!user!Sleep!1!job_1329348432655_0001!AM_appattempt_1329348432655_0001_000001",
          "cluster1!user!Sleep!1!job_1329348432655_0001!r_000000",
          "cluster1!user!Sleep!1!job_1329348432655_0001!r_000000_0",
          "cluster1!user!Sleep!1!job_1329348432655_0001!m_000000",
          "cluster1!user!Sleep!1!job_1329348432655_0001!m_000000_0",
          "cluster1!user!Sleep!1!job_1329348432655_0001!m_000009",
          "cluster1!user!Sleep!1!job_1329348432655_0001!m_000009_0",
          "cluster1!user!Sleep!1!job_1329348432655_0001!m_000008",
          "cluster1!user!Sleep!1!job_1329348432655_0001!m_000008_0",
          "cluster1!user!Sleep!1!job_1329348432655_0001!m_000007",
          "cluster1!user!Sleep!1!job_1329348432655_0001!m_000007_0",
          "cluster1!user!Sleep!1!job_1329348432655_0001!m_000006",
          "cluster1!user!Sleep!1!job_1329348432655_0001!m_000006_0",
          "cluster1!user!Sleep!1!job_1329348432655_0001!m_000005",
          "cluster1!user!Sleep!1!job_1329348432655_0001!m_000005_0",
          "cluster1!user!Sleep!1!job_1329348432655_0001!m_000004",
          "cluster1!user!Sleep!1!job_1329348432655_0001!m_000004_0",
          "cluster1!user!Sleep!1!job_1329348432655_0001!m_000003",
          "cluster1!user!Sleep!1!job_1329348432655_0001!m_000003_0",
          "cluster1!user!Sleep!1!job_1329348432655_0001!m_000002",
          "cluster1!user!Sleep!1!job_1329348432655_0001!m_000002_0",
          "cluster1!user!Sleep!1!job_1329348432655_0001!m_000001",
          "cluster1!user!Sleep!1!job_1329348432655_0001!m_000001_0"));

    String tKey;
    for (JobHistoryTaskRecord p: taskRecords) {
      assertTrue(putRowKeys.contains(p.getKey().toString()));
    }

    // check post processing for megabytemillis
    // first with empty job conf
    Long mbMillis = historyFileParser.getMegaByteMillis();
    assertNotNull(mbMillis);
    Long expValue = 10390016L;
    assertEquals(expValue, mbMillis);
  }

  @Test(expected=ProcessingException.class)
  public void testCreateJobHistoryFileParserNullConf() throws IOException {

    final String JOB_HISTORY_FILE_NAME =
        "src/test/resources/job_1329348432655_0001-1329348443227-user-Sleep+job-1329348468601-10-1-SUCCEEDED-default.jhist";

    File jobHistoryfile = new File(JOB_HISTORY_FILE_NAME);
    byte[] contents = Files.toByteArray(jobHistoryfile);
    JobHistoryFileParser historyFileParser =
        JobHistoryFileParserFactory.createJobHistoryFileParser(contents, null);
    assertNotNull(historyFileParser);

    // confirm that we get back an object that can parse hadoop 2.0 files
    assertTrue(historyFileParser instanceof JobHistoryFileParserHadoop2);

    JobKey jobKey = new JobKey("cluster1", "user", "Sleep", 1, "job_1329348432655_0001");
    // pass in null as jobConf and confirm the exception thrown
    historyFileParser.parse(contents, jobKey);
  }

  @Test
  public void testMissingSlotMillis() throws IOException {
    final String JOB_HISTORY_FILE_NAME =
        "src/test/resources/job_1329348432999_0003-1329348443227-user-Sleep+job-1329348468601-10-1-SUCCEEDED-default.jhist";

    File jobHistoryfile = new File(JOB_HISTORY_FILE_NAME);
    byte[] contents = Files.toByteArray(jobHistoryfile);
    final String JOB_CONF_FILE_NAME =
        "src/test/resources/job_1329348432655_0001_conf.xml";
    Configuration jobConf = new Configuration();
    jobConf.addResource(new Path(JOB_CONF_FILE_NAME));

    JobHistoryFileParser historyFileParser =
        JobHistoryFileParserFactory.createJobHistoryFileParser(contents, jobConf);
    assertNotNull(historyFileParser);

    // confirm that we get back an object that can parse hadoop 2.0 files
    assertTrue(historyFileParser instanceof JobHistoryFileParserHadoop2);
    JobKey jobKey = new JobKey("cluster1", "user", "Sleep", 1, "job_1329348432655_0001");
    historyFileParser.parse(contents, jobKey);

    // this history file has only map slot millis no reduce millis
    Long mbMillis = historyFileParser.getMegaByteMillis();
    assertNotNull(mbMillis);
    Long expValue = 10402816L;
    assertEquals(expValue, mbMillis);
  }

  /**
   * To ensure we write these keys as Longs, not as ints
   */
  @Test
  public void testLongExpGetValuesIntBytes() {

    String[] keysToBeChecked = {"totalMaps", "totalReduces", "finishedMaps",
                                 "finishedReduces", "failedMaps", "failedReduces"};
    Long longValue = null;
    int intValue10 = 10;
    long longValue10 = 10L;

    JobHistoryFileParserHadoop2 jh = new JobHistoryFileParserHadoop2(null);

    for(String key: keysToBeChecked) {
      longValue = (Long) jh.getValue(JobHistoryKeys.HADOOP2_TO_HADOOP1_MAPPING.get(key), intValue10);
      assertEquals(longValue.longValue(), longValue10);
     }
  }

  /**
   * To ensure we write these keys as ints
   */
  @Test
  public void testIntExpGetValuesIntBytes() {

    String[] keysToBeChecked = {"httpPort"};
    Integer intValue = null;
    int intValue10 = 10;

    JobHistoryFileParserHadoop2 jh = new JobHistoryFileParserHadoop2(null);

    for(String key: keysToBeChecked) {
      intValue = (Integer) jh.getValue(JobHistoryKeys.HADOOP2_TO_HADOOP1_MAPPING.get(key), intValue10);
      assertEquals(intValue.intValue(), intValue10);
     }
  }
}
