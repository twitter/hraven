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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.mapred.JobHistoryCopy;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.datasource.ProcessingException;
import com.twitter.hraven.mapreduce.JobHistoryListener;

/**
 * Deal with JobHistory file parsing for job history files which are generated
 * pre MAPREDUCE-1016
 * 
 */
public class JobHistoryFileParserHadoop1 implements JobHistoryFileParser {

	private static final Log LOG = LogFactory
			.getLog(JobHistoryFileParserHadoop1.class);

	private JobHistoryListener jobHistoryListener = null;

	/**
	 * {@inheritDoc}
	 * 
	 */
	@Override
	public void parse(byte[] historyFile, JobKey jobKey)
			throws ProcessingException {

		try {
			jobHistoryListener = new JobHistoryListener(jobKey);
			JobHistoryCopy.parseHistoryFromIS(new ByteArrayInputStream(historyFile), jobHistoryListener);
		} catch (IOException ioe) {
			LOG.error(" Exception during parsing hadoop 1.0 file ", ioe);
			throw new ProcessingException(
					" Unable to parse history file in function parse, "
							+ "cannot process this record!" + jobKey
							+ " error: " , ioe);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<Put> getJobPuts() {
		if (jobHistoryListener != null) {
			return jobHistoryListener.getJobPuts();
		} else {
			return null;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<Put> getTaskPuts() {
		if (jobHistoryListener != null) {
			return jobHistoryListener.getTaskPuts();
		} else {
			return null;
		}
	}
}
