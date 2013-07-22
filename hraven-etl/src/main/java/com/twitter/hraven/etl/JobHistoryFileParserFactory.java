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

import java.io.InputStream;

/**
 * Deal with {@link JobHistoryFileParser} implementations.
 */
public class JobHistoryFileParserFactory {

	/**
	 * determines the verison of hadoop that the history file belongs to
	 * 
	 * @return currently returns 1 for hadoop 1 (pre MAPREDUCE-1016) for newer
	 *         job history files, this method will look at the history file and
	 *         return values appropriately
	 * 
	 *         (newer job history files have "AVRO-JSON" as the signature at the
	 *         start of the file, REFERENCE:
	 *         https://issues.apache.org/jira/browse/MAPREDUCE-1016? \
	 *         focusedCommentId=12763160& \
	 *         page=com.atlassian.jira.plugin.system
	 *         .issuetabpanels:comment-tabpanel#comment-12763160
	 * 
	 */
	public static int getVersion(InputStream historyFile) {
		return 1;
	}

	/**
	 * creates an instance of {@link JobHistoryParseHadoop1}
	 * 
	 * to be later enhanced to return either {@link JobHistoryParseHadoop1} or
	 * an object that can parse post MAPREDUCE-1016 job history files
	 * 
	 * @param historyFile
	 *            : input stream to the history file contents
	 * 
	 * @return an object of {@link JobHistoryParseHadoop1} that can parse Hadoop
	 *         1.0 (pre MAPREDUCE-1016) generated job history files Or return
	 *         null if either input is null
	 * 
	 */
	public static JobHistoryFileParser createJobHistoryFileParser(
			InputStream historyFile) throws IllegalArgumentException {

		if (historyFile == null) {
			throw new IllegalArgumentException(
					"Job history input stream should not be null");
		}

		int version = getVersion(historyFile);

		switch (version) {
		case 1:
			return new JobHistoryFileParserHadoop1();

			/*
			 * right now, the default won't be in any code path but as we add
			 * support for post MAPREDUCE-1016 and Hadoop 2.0 this would be
			 * relevant
			 */
		default:
			throw new IllegalArgumentException(
					" Unknown format of job history file");
		}
	}
}
