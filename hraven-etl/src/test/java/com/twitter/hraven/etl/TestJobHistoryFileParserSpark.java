/*
 * Copyright 2014 Twitter, Inc. Licensed under the Apache License, Version 2.0 (the "License"); you
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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import com.google.common.io.Files;
import com.twitter.hraven.Constants;
import com.twitter.hraven.HistoryFileType;
import com.twitter.hraven.JobHistoryKeys;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.datasource.JobKeyConverter;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Test {@link JobHistoryFileParserHadoop2}
 */
public class TestJobHistoryFileParserSpark {

  @Test
  public void testCreateJobHistoryFileParserCorrectCreation() throws IOException {

    final String JOB_HISTORY_FILE_NAME =
        "src/test/resources/spark_1413515656084_3051855";

    File jobHistoryfile = new File(JOB_HISTORY_FILE_NAME);
    byte[] contents = Files.toByteArray(jobHistoryfile);
    // now load the conf file and check
    final String JOB_CONF_FILE_NAME =
        "src/test/resources/spark_1413515656084_3051855_conf.xml";
    Configuration jobConf = new Configuration();
    jobConf.addResource(new Path(JOB_CONF_FILE_NAME));

    JobHistoryFileParser historyFileParser =
        JobHistoryFileParserFactory.createJobHistoryFileParser(contents, jobConf,
          HistoryFileType.SPARK);
    assertNotNull(historyFileParser);

    // confirm that we get back an object that can parse spark files
    assertTrue(historyFileParser instanceof JobHistoryFileParserSpark);

    JobKey jobKey = new JobKey("cluster1",
      "user1",
      "com.example.spark_program.simple_example.Main$",
      1413515656084L,
      "spark_1413515656084_305185");
    historyFileParser.parse(contents, jobKey);

    List<Put> jobPuts = historyFileParser.getJobPuts();
    assertNotNull(jobPuts);
    assertEquals(2, jobPuts.size());

    JobKeyConverter jobKeyConv = new JobKeyConverter();
    System.out.println(" rowkey " +jobPuts.get(0).getRow().toString());
    assertEquals(jobKey.toString(), jobKeyConv.fromBytes(jobPuts.get(0).getRow()).toString());

    // check history file type
    boolean foundVersion2 = false;
    for (Put p : jobPuts) {
      List<Cell> kv2 = p.get(Constants.INFO_FAM_BYTES,
          Bytes.toBytes(JobHistoryKeys.hadoopversion.toString()));
      if (kv2.size() == 0) {
        // we are interested in hadoop version put only
        // hence continue
          continue;
      }
      assertEquals(1, kv2.size());
      Map<byte[], List<KeyValue>> d = p.getFamilyMap();
      for (List<KeyValue> lkv : d.values()) {
        for (KeyValue kv : lkv) {
        // ensure we have a hadoop2 version as the value
        assertEquals(Bytes.toString(CellUtil.cloneValue(kv)), 
            HistoryFileType.SPARK.toString()); 

          // ensure we don't see the same put twice
          assertFalse(foundVersion2);
        // now set this to true
        foundVersion2 = true;
        }
       }
    }
    // ensure that we got the hadoop2 version put
    assertTrue(foundVersion2);

    // check job status
    boolean foundJobStatus = false;
    for (Put p : jobPuts) {
      List<Cell> kv2 =
          p.get(Constants.INFO_FAM_BYTES,
            Bytes.toBytes(JobHistoryKeys.JOB_STATUS.toString().toLowerCase()));
      if (kv2.size() == 0) {
        // we are interested in JobStatus put only
        // hence continue
        continue;
      }
      assertEquals(1, kv2.size());

      for (Cell kv : kv2) {
        // ensure we have a job status value as the value
        assertEquals(Bytes.toString(CellUtil.cloneValue(kv)),
          JobHistoryFileParserHadoop2.JOB_STATUS_SUCCEEDED);

        // ensure we don't see the same put twice
        assertFalse(foundJobStatus);
        // now set this to true
        foundJobStatus = true;
      }
    }
    // ensure that we got the JobStatus put
    assertTrue(foundJobStatus);

    List<Put> taskPuts = historyFileParser.getTaskPuts();
    assertEquals(taskPuts.size(), 0);

    // check post processing for megabytemillis
    // first with empty job conf
    Long mbMillis = historyFileParser.getMegaByteMillis();
    assertNotNull(mbMillis);
    Long expValue = 1528224768L;
    assertEquals(expValue, mbMillis);
  }
}
