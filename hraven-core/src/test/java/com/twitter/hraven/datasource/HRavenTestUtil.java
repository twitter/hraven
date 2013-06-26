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
package com.twitter.hraven.datasource;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;

import com.twitter.hraven.Constants;

/**
 * Common utilities to support test cases.
 */
public class HRavenTestUtil {
  public static void createSchema(HBaseTestingUtility util) throws IOException {
    createHistoryTable(util);
    createTaskTable(util);
    createHistoryByJobIdTable(util);
    createRawTable(util);
    createProcessTable(util);
    createAppVersionTable(util);
    createFlowQueueTable(util);
    createFlowEventTable(util);
  }

  public static HTable createHistoryTable(HBaseTestingUtility util)
      throws IOException {
    return util.createTable(Constants.HISTORY_TABLE_BYTES,
        Constants.INFO_FAM_BYTES);
  }

  public static HTable createTaskTable(HBaseTestingUtility util)
      throws IOException {
    return util.createTable(Constants.HISTORY_TASK_TABLE_BYTES,
        Constants.INFO_FAM_BYTES);
  }

  public static HTable createHistoryByJobIdTable(HBaseTestingUtility util)
      throws IOException {
    return util.createTable(Constants.HISTORY_BY_JOBID_TABLE_BYTES,
        Constants.INFO_FAM_BYTES);
  }

  public static HTable createRawTable(HBaseTestingUtility util)
      throws IOException {
    return util.createTable(Constants.HISTORY_RAW_TABLE_BYTES,
        new byte[][]{Constants.INFO_FAM_BYTES, Constants.RAW_FAM_BYTES});
  }

  public static HTable createProcessTable(HBaseTestingUtility util)
      throws IOException {
    return util.createTable(Constants.JOB_FILE_PROCESS_TABLE_BYTES,
        Constants.INFO_FAM_BYTES);
  }

  public static HTable createAppVersionTable(HBaseTestingUtility util)
      throws IOException {
    return util.createTable(Constants.HISTORY_APP_VERSION_TABLE_BYTES,
        Constants.INFO_FAM_BYTES);
  }

  public static HTable createFlowQueueTable(HBaseTestingUtility util)
      throws IOException {
    return util.createTable(Constants.FLOW_QUEUE_TABLE_BYTES, Constants.INFO_FAM_BYTES);
  }

  public static HTable createFlowEventTable(HBaseTestingUtility util)
      throws IOException {
    return util.createTable(Constants.FLOW_EVENT_TABLE_BYTES, Constants.INFO_FAM_BYTES);
  }
}
