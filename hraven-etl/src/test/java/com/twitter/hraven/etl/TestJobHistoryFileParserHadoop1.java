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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Test;

import com.google.common.io.Files;
import com.twitter.hraven.Constants;
import com.twitter.hraven.HistoryFileType;
import com.twitter.hraven.JobKey;

public class TestJobHistoryFileParserHadoop1 {

  @Test
  public void testNullMegaByteMills() {
    JobHistoryFileParserHadoop1 historyFileParser1 = new JobHistoryFileParserHadoop1(null);
    assertNotNull(historyFileParser1);
    Long mbMillis = historyFileParser1.getMegaByteMillis();
    assertEquals(Constants.NOTFOUND_VALUE, mbMillis);
  }

  @Test
  public void testMegaByteMillis() throws IOException {

    final String JOB_HISTORY_FILE_NAME =
        "src/test/resources/job_201311192236_3583_1386370578196_user1_Sleep+job";

    File jobHistoryfile = new File(JOB_HISTORY_FILE_NAME);
    byte[] contents = Files.toByteArray(jobHistoryfile);
    final String JOB_CONF_FILENAME = "src/test/resources/job_1329348432655_0001_conf.xml";
    Configuration jobConf = new Configuration();
    jobConf.addResource(new Path(JOB_CONF_FILENAME));

    JobHistoryFileParser historyFileParser =
        JobHistoryFileParserFactory.createJobHistoryFileParser(contents, jobConf,
          HistoryFileType.ONE);
    assertNotNull(historyFileParser);

    // confirm that we get back an object that can parse hadoop 1.0 files
    assertTrue(historyFileParser instanceof JobHistoryFileParserHadoop1);

    JobKey jobKey = new JobKey("cluster1", "user1", "Sleep", 1, "job_201311192236_3583");
    historyFileParser.parse(contents, jobKey);

    List<Put> jobPuts = historyFileParser.getJobPuts();
    assertNotNull(jobPuts);

    Long mbMillis = historyFileParser.getMegaByteMillis();
    Long expValue = 2981062L;
    assertEquals(expValue, mbMillis);
  }
}