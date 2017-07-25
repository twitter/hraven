/*
Copyright 2016 Twitter, Inc.

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


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.twitter.hraven.datasource.JobHistoryByIdService;
import com.twitter.hraven.datasource.JobKeyConverter;


/**
 * Stores data in job_history table
 * also retrieves flow stats
 */
public class GenerateFlowTestData {
  @SuppressWarnings("unused")
  private static Log LOG = LogFactory.getLog(GenerateFlowTestData.class);
  /** Default dummy configuration properties */
  private static Map<String,String> DEFAULT_CONFIG = new HashMap<String,String>();
  static {
    DEFAULT_CONFIG.put("testproperty1", "value1");
    DEFAULT_CONFIG.put("testproperty2", "value2");
  }

  private int jobIdCnt;
  public static int SUBMIT_LAUCH_DIFF = 500 ;

  // TODO: change method signature to get rid of table and pass a connection instead
  public void loadFlow(String cluster, String user, String app, long runId,
      String version, int jobCount, long baseStats,
      JobHistoryByIdService idService, Table historyTable)
  throws Exception {
    loadFlow(cluster, user, app, runId, version, jobCount, baseStats, idService, historyTable,
        DEFAULT_CONFIG);
  }

  public void loadFlow(String cluster, String user, String app, long runId,
      String version, int jobCount, long baseStats,
      JobHistoryByIdService idService, Table historyTable, Map<String,String> config)
  throws Exception {
    List<Put> puts = new ArrayList<Put>(jobCount);
    JobKeyConverter keyConv = new JobKeyConverter();
    long curTime = 1355614887;
    long submitTime = curTime - GenerateFlowTestData.SUBMIT_LAUCH_DIFF;
    for (int i = 0; i < jobCount; i++) {
      String jobId = String.format("job_20120101000000_%04d", jobIdCnt++);
      JobKey key = new JobKey(cluster, user, app, runId, jobId);
      Put p = new Put(keyConv.toBytes(key));
      p.addColumn(Constants.INFO_FAM_BYTES, JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.JOBID),
          Bytes.toBytes(jobId));
      p.addColumn(Constants.INFO_FAM_BYTES, JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.JOB_STATUS),
          Bytes.toBytes("SUCCESS"));
      p.addColumn(Constants.INFO_FAM_BYTES, Constants.VERSION_COLUMN_BYTES, Bytes.toBytes(version));
      p.addColumn(Constants.INFO_FAM_BYTES, JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.hadoopversion),
        Bytes.toBytes(HistoryFileType.ONE.toString()));
      p.addColumn(Constants.INFO_FAM_BYTES, JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.TOTAL_MAPS),
          Bytes.toBytes(baseStats));
      p.addColumn(Constants.INFO_FAM_BYTES, JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.TOTAL_REDUCES),
          Bytes.toBytes(baseStats));
      p.addColumn(Constants.INFO_FAM_BYTES, Constants.MEGABYTEMILLIS_BYTES, Bytes.toBytes(baseStats));
      p.addColumn(Constants.INFO_FAM_BYTES, JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.SUBMIT_TIME),
        Bytes.toBytes(submitTime));
      p.addColumn(Constants.INFO_FAM_BYTES, JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.LAUNCH_TIME),
          Bytes.toBytes(curTime));
      p.addColumn(Constants.INFO_FAM_BYTES, JobHistoryKeys.KEYS_TO_BYTES.get(JobHistoryKeys.FINISH_TIME),
          Bytes.toBytes( 1000 + curTime));
      p.addColumn(Constants.INFO_FAM_BYTES, Bytes.toBytes("g!FileSystemCounters!HDFS_BYTES_WRITTEN"),
          Bytes.toBytes(baseStats));
      p.addColumn(Constants.INFO_FAM_BYTES, Bytes.toBytes("g!FileSystemCounters!HDFS_BYTES_READ"),
          Bytes.toBytes(baseStats) );
      p.addColumn(Constants.INFO_FAM_BYTES, Bytes.toBytes("gr!FileSystemCounters!FILE_BYTES_READ"),
          Bytes.toBytes(baseStats));
      p.addColumn(Constants.INFO_FAM_BYTES,
          Bytes.toBytes("gr!org.apache.hadoop.mapred.Task$Counter!REDUCE_SHUFFLE_BYTES"),
          Bytes.toBytes(baseStats));
      p.addColumn(Constants.INFO_FAM_BYTES, Bytes.toBytes("gm!FileSystemCounters!FILE_BYTES_READ"),
          Bytes.toBytes(baseStats));
      p.addColumn(Constants.INFO_FAM_BYTES, Bytes.toBytes("gm!FileSystemCounters!FILE_BYTES_WRITTEN"),
          Bytes.toBytes(baseStats));
      p.addColumn(Constants.INFO_FAM_BYTES,
          Bytes.toBytes("g!org.apache.hadoop.mapred.JobInProgress$Counter!SLOTS_MILLIS_MAPS"),
          Bytes.toBytes(baseStats));
      p.addColumn(Constants.INFO_FAM_BYTES,
          Bytes.toBytes("g!org.apache.hadoop.mapred.JobInProgress$Counter!SLOTS_MILLIS_REDUCES"),
          Bytes.toBytes(baseStats));

      // add some config properties
      if (config != null) {
        for (Map.Entry<String,String> entry : config.entrySet()) {
          p.addColumn(Constants.INFO_FAM_BYTES,
              Bytes.toBytes(Constants.JOB_CONF_COLUMN_PREFIX + Constants.SEP + entry.getKey()),
              Bytes.toBytes(entry.getValue()));
        }
      }

      puts.add(p);
      curTime += 1000 ;
      submitTime = curTime - GenerateFlowTestData.SUBMIT_LAUCH_DIFF;

      idService.writeIndexes(key);
    }
    historyTable.put(puts);
  }

}
