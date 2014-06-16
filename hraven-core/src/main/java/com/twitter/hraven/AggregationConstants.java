/*
Copyright 2014 Twitter, Inc.

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

import org.apache.hadoop.hbase.util.Bytes;

/**
 * defines the aggregation related constants
 *
 */
public class AggregationConstants {

  public static final String AGG_DAILY_TABLE = "hraven_agg_daily";
  public static final byte[] AGG_DAILY_TABLE_BYTES = Bytes.toBytes(AGG_DAILY_TABLE);

  public static final String AGG_WEEKLY_TABLE = "hraven_agg_weekly";
  public static final byte[] AGG_WEEKLY_TABLE_BYTES = Bytes.toBytes(AGG_WEEKLY_TABLE);

  public static final String INFO_FAM = "i";
  public static final byte[] INFO_FAM_BYTES = Bytes.toBytes(INFO_FAM);

  public static final String RAW_FAM = "r";
  public static final byte[] RAW_FAM_BYTES = Bytes.toBytes(RAW_FAM);

  /** parameter that specifies whether or not to aggregate */
  public static final String AGGREGATION_FLAG_NAME = "aggregate";

  /** column name for app id in aggregation table */
  public static final String APP_ID_COL = "app_id";
  public static final byte[] APP_ID_COL_BYTES = Bytes.toBytes(APP_ID_COL.toLowerCase());

  /**
   * the number of runs in an aggregation
   */
  public static final String NUMBER_RUNS = "number_runs";

  /** raw bytes representation of the number of runs parameter */
  public static final byte[] NUMBER_RUNS_BYTES = Bytes.toBytes(NUMBER_RUNS.toLowerCase());

  /**
   * the user who ran this app
   */
  public static final String USER = "user";
  public static final byte[] USER_BYTES = Bytes.toBytes(USER.toLowerCase());

  /**
   * the number of jobs in an aggregation
   */
  public static final String TOTAL_JOBS = "total_jobs";

  /** raw bytes representation of the number jobs parameter */
  public static final byte[] TOTAL_JOBS_BYTES = Bytes.toBytes(TOTAL_JOBS.toLowerCase());

  /**
   * use this config setting to define an hadoop-version-independent property for queuename
   */
  public static final String HRAVEN_QUEUE = "queue";

  /** raw bytes representation of the queue parameter */
  public static final byte[] HRAVEN_QUEUE_BYTES = Bytes.toBytes(HRAVEN_QUEUE.toLowerCase());

  /** total maps and reduces */
  public static final String TOTAL_MAPS = "total_maps";
  public static final byte[] TOTAL_MAPS_BYTES = Bytes.toBytes(TOTAL_MAPS.toLowerCase());
  public static final String TOTAL_REDUCES = "total_reduces";
  public static final byte[] TOTAL_REDUCES_BYTES = Bytes.toBytes(TOTAL_REDUCES.toLowerCase());

  /** slot millis for maps and reduces */
  public static final String SLOTS_MILLIS_MAPS = "slots_millis_maps";
  public static final byte[] SLOTS_MILLIS_MAPS_BYTES = Bytes.toBytes(SLOTS_MILLIS_MAPS
      .toLowerCase());
  public static final String SLOTS_MILLIS_REDUCES = "slots_millis_reduces";
  public static final byte[] SLOTS_MILLIS_REDUCES_BYTES = Bytes.toBytes(SLOTS_MILLIS_REDUCES
      .toLowerCase());

  /** used to indicate how expensive a job is in terms of memory and time taken */
  public static final String MEGABYTEMILLIS = "megabytemillis";
  public static final byte[] MEGABYTEMILLIS_BYTES = Bytes.toBytes(MEGABYTEMILLIS.toLowerCase());

  /** used to indicate the cost of a job is in terms of currency units */
  public static final String JOBCOST = "jobcost";
  public static final byte[] JOBCOST_BYTES = Bytes.toBytes(JOBCOST.toLowerCase());

}
