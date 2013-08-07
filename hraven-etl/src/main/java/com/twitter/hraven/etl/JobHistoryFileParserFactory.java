/*
 * Copyright 2013 Twitter, Inc. Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may obtain a copy of the License
 * at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
 * writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package com.twitter.hraven.etl;

import org.apache.commons.lang.StringUtils;

/**
 * Deal with {@link JobHistoryFileParser} implementations. Creates an appropriate Job History File
 * Parser Object based on the type of job history file
 */
public class JobHistoryFileParserFactory {

  /**
   * NOTE that this version string is a replica of
   * {@link org.apache.hadoop.mapreduce.jobhistory.EventWriter} Since that class is not public, the
   * VERSION variable there becomes package-level visible and hence we need a replica
   */
  public static final String HADOOP2_VERSION_STRING = "Avro-Json";
  private static final int HISTORY_FILE_VERSION1 = 1;
  private static final int HISTORY_FILE_VERSION2 = 2;

  /**
   * determines the verison of hadoop that the history file belongs to
   * @return returns 1 for hadoop 1 (pre MAPREDUCE-1016) returns 2 for newer job history files (newer
   *         job history files have "AVRO-JSON" as the signature at the start of the file,
   *         REFERENCE: https://issues.apache.org/jira/browse/MAPREDUCE-1016? \
   *         focusedCommentId=12763160& \ page=com.atlassian.jira.plugin.system
   *         .issuetabpanels:comment-tabpanel#comment-12763160
   */
  public static int getVersion(String historyFileContents) {
    String versionPart = getVersionStringFromFile(historyFileContents);
    if (StringUtils.equalsIgnoreCase(versionPart, HADOOP2_VERSION_STRING)) {
      return HISTORY_FILE_VERSION2;
    } else {
      return HISTORY_FILE_VERSION1;
    }
  }

  /**
   * method to return the version string that's inside the history file
   * @return versionString
   */
  private static String getVersionStringFromFile(String contents) {
    return contents.substring(0, 9);
  }

  /**
   * creates an instance of {@link JobHistoryParseHadoop1} to be later enhanced to return either
   * {@link JobHistoryParseHadoop1} or an object that can parse post MAPREDUCE-1016 job history
   * files
   * @param historyFile : input stream to the history file contents
   * @return an object of {@link JobHistoryParseHadoop1} that can parse Hadoop 1.0 (pre
   *         MAPREDUCE-1016) generated job history files Or return null if either input is null
   */
  public static JobHistoryFileParser createJobHistoryFileParser(String historyFileContents)
      throws IllegalArgumentException {

    if (historyFileContents == null) {
      throw new IllegalArgumentException("Job history file contents should not be null");
    }

    int version = getVersion(historyFileContents);

    switch (version) {
    case 1:
      return new JobHistoryFileParserHadoop1();

    case 2:
      return new JobHistoryFileParserHadoop2();

    default:
      throw new IllegalArgumentException(" Unknown format of job history file: "
          + getVersionStringFromFile(historyFileContents));
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
