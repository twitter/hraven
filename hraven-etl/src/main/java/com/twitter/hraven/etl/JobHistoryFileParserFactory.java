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

import org.apache.commons.lang.StringUtils;

/**
 * Deal with {@link JobHistoryFileParser} implementations.
 * Creates an appropriate Job History File Parser Object based
 * on the type of job history file
 */
public class JobHistoryFileParserFactory {

  /**
   * NOTE that this version string is a replica of
   * {@link org.apache.hadoop.mapreduce.jobhistory.EventWriter} Since that class is not public, the
   * VERSION variable there becomes package-level visible and hence we need a replica
   */
  public static final String HADOOP2_VERSION_STRING = "Avro-Json";
  public static final String HADOOP1_VERSION_STRING = "Meta VERSION=\"1\" .";
  private static final int HADOOP2_VERSION_LENGTH = 9;
  private static final int HADOOP1_VERSION_LENGTH = 18;
  private static final int HISTORY_FILE_VERSION1 = 1;
  private static final int HISTORY_FILE_VERSION2 = 2;

  /**
   * determines the verison of hadoop that the history file belongs to
   *
   * @return 
   * returns 1 for hadoop 1 (pre MAPREDUCE-1016)
   * returns 2 for newer job history files
   *         (newer job history files have "AVRO-JSON" as the signature at the start of the file,
   *         REFERENCE: https://issues.apache.org/jira/browse/MAPREDUCE-1016? \
   *         focusedCommentId=12763160& \ page=com.atlassian.jira.plugin.system
   *         .issuetabpanels:comment-tabpanel#comment-12763160
   * 
   * @throws IllegalArgumentException if neither match
   */
  public static int getVersion(byte[] historyFileContents) {
    if(historyFileContents.length > HADOOP2_VERSION_LENGTH) {
      // the first 10 bytes in a hadoop2.0 history file contain Avro-Json
      String version2Part =  new String(historyFileContents, 0, HADOOP2_VERSION_LENGTH);
      if (StringUtils.equalsIgnoreCase(version2Part, HADOOP2_VERSION_STRING)) {
        return HISTORY_FILE_VERSION2;
      } else {
        if(historyFileContents.length > HADOOP1_VERSION_LENGTH) {
          // the first 18 bytes in a hadoop1.0 history file contain Meta VERSION="1" .
          String version1Part =  new String(historyFileContents, 0, HADOOP1_VERSION_LENGTH);
          if (StringUtils.equalsIgnoreCase(version1Part, HADOOP1_VERSION_STRING)) {
            return HISTORY_FILE_VERSION1;
          }
        }
      }
    }
    // throw an exception if we did not find any matching version
    throw new IllegalArgumentException(" Unknown format of job history file: " + historyFileContents);
  }

  /**
   * creates an instance of {@link JobHistoryParseHadoop1}
   * or
   * {@link JobHistoryParseHadoop2} that can parse post MAPREDUCE-1016 job history files
   *
   * @param historyFile: history file contents
   *
   * @return an object that can parse job history files
   * Or return null if either input is null
   */
  public static JobHistoryFileParser createJobHistoryFileParser(
      byte[] historyFileContents) throws IllegalArgumentException {

    if (historyFileContents == null) {
      throw new IllegalArgumentException(
          "Job history contents should not be null");
    }

    int version = getVersion(historyFileContents);

    switch (version) {
    case 1:
      return new JobHistoryFileParserHadoop1();

    case 2:
      return new JobHistoryFileParserHadoop2();

    default:
      throw new IllegalArgumentException(
          " Unknown format of job history file ");
    }
  }

  /**
   * @return HISTORY_FILE_VERSION1
   */
  public static int getHistoryFileVersion1() {
    return HISTORY_FILE_VERSION1;
  }

  /**
   * @return HISTORY_FILE_VERSION2
   */
  public static int getHistoryFileVersion2() {
    return HISTORY_FILE_VERSION2;
  }
}
