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
 * Defines various constants used in hdfs stats processing
 */
public class HdfsConstants {

  // HBase constants
  // separator character used between key components
  public static final char SEP_CHAR = '!';
  public static final String SEP = "" + SEP_CHAR;
  public static final byte[] SEP_BYTES = Bytes.toBytes(SEP);

  // common default values
  public static final byte[] EMPTY_BYTES = new byte[0];
  public static final byte[] ZERO_INT_BYTES = Bytes.toBytes(0);
  public static final byte[] ZERO_LONG_BYTES = Bytes.toBytes(0L);
  public static final byte[] ZERO_SINGLE_BYTE = new byte[] { 0 };
  public static final int NUM_SECONDS_IN_A_DAY = 86400;

  /** max retries in case of no data */
  public static final int MAX_RETRIES = 10;

  /**
   * a series of older dates like
   * 1 day ago, 1 week ago, 2 weeks ago, 1 month ago etc
   */
  public static final int ageMult[] = {1, 4, 10, 18, 30, 45, 60, 90, 120, 150};

  public static final String UNKNOWN = "";

  /** default number of records returned in json response */
  public static final int RECORDS_RETURNED_LIMIT = 500;

  /** Hdfs Stats Table names */
  public static final String HDFS_USAGE_TABLE = "chargeback_hdfs_usage";
  public static final byte[] HDFS_USAGE_TABLE_BYTES = Bytes.toBytes(HDFS_USAGE_TABLE);

  public static final String HDFS_USER_ACCESS_TABLE = "chargeback_user_access";
  public static final byte[] HDFS_USER_ACCESS_TABLE_BYTES = Bytes.toBytes(HDFS_USER_ACCESS_TABLE);

  public static final int NUM_HDFS_USAGE_ROWKEY_COMPONENTS = 3;

  public static final String DISK_INFO_FAM = "disk_info";
  public static final byte[] DISK_INFO_FAM_BYTES = Bytes.toBytes(DISK_INFO_FAM);

  public static final String ACCESS_INFO_FAM = "access_info";
  public static final byte[] ACCESS_INFO_FAM_BYTES = Bytes.toBytes(ACCESS_INFO_FAM);

  public static final String FILE_COUNT_COLUMN = "fileCount";
  public static final byte[] FILE_COUNT_COLUMN_BYTES = Bytes.toBytes(FILE_COUNT_COLUMN);

  public static final String DIR_COUNT_COLUMN = "directoryCount";
  public static final byte[] DIR_COUNT_COLUMN_BYTES = Bytes.toBytes(DIR_COUNT_COLUMN);

  public static final String OWNER_COLUMN = "owner";
  public static final byte[] OWNER_COLUMN_BYTES = Bytes.toBytes(OWNER_COLUMN);

  public static final String SPACE_CONSUMED_COLUMN = "spaceConsumed";
  public static final byte[] SPACE_CONSUMED_COLUMN_BYTES = Bytes.toBytes(SPACE_CONSUMED_COLUMN);

  public static final String ACCESS_COUNT_TOTAL_COLUMN = "accessCountTotal";
  public static final byte[] ACCESS_COUNT_TOTAL_COLUMN_BYTES = Bytes
      .toBytes(ACCESS_COUNT_TOTAL_COLUMN);

  public static final String QUOTA_COLUMN = "quota";
  public static final byte[] QUOTA_COLUMN_BYTES = Bytes.toBytes(QUOTA_COLUMN);

  public static final String SPACE_QUOTA_COLUMN = "spaceQuota";
  public static final byte[] SPACE_QUOTA_COLUMN_BYTES = Bytes.toBytes(SPACE_QUOTA_COLUMN);

  public static final String TRASH_FILE_COUNT_COLUMN = "trashFileCount";
  public static final byte[] TRASH_FILE_COUNT_COLUMN_BYTES = Bytes.toBytes(TRASH_FILE_COUNT_COLUMN);

  public static final String TRASH_SPACE_CONSUMED_COLUMN = "trashSpaceConsumed";
  public static final byte[] TRASH_SPACE_CONSUMED_COLUMN_BYTES = Bytes
      .toBytes(TRASH_SPACE_CONSUMED_COLUMN);

  public static final String TMP_FILE_COUNT_COLUMN = "tmpFileCount";
  public static final byte[] TMP_FILE_COUNT_COLUMN_BYTES = Bytes.toBytes(TMP_FILE_COUNT_COLUMN);

  public static final String TMP_SPACE_CONSUMED_COLUMN = "tmpSpaceConsumed";
  public static final byte[] TMP_SPACE_CONSUMED_COLUMN_BYTES = Bytes
      .toBytes(TMP_SPACE_CONSUMED_COLUMN);

  public static final String ACCESS_COST_COLUMN = "accessCost";
  public static final byte[] ACCESS_COST_COLUMN_BYTES = Bytes.toBytes(ACCESS_COST_COLUMN);

  public static final String STORAGE_COST_COLUMN = "accessCost";
  public static final byte[] STORAGE_COST_COLUMN_BYTES = Bytes.toBytes(STORAGE_COST_COLUMN);

}
