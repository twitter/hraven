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

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.mapred.JobHistoryCopy;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.mapreduce.JobHistoryListener;

/**
 * Deal with JobHistory file parsing for job history files
 * which are generated pre MAPREDUCE-1016
 * 
 */
public class JobHistoryFileDecipherHadoop1 extends JobHistoryFileDecipherBase {

	private static final Log LOG = LogFactory
			.getLog(JobHistoryFileDecipherHadoop1.class);
	
	private JobHistoryListener jobHistoryListener = null;

	/*
	 * default constructor
	 */
	public JobHistoryFileDecipherHadoop1() {
		super();
	}

	/*
	 * (non-Javadoc)
	 * @see com.twitter.hraven.etl.JobHistoryFileDecipherBase#decipher(java.io.InputStream, com.twitter.hraven.JobKey)
	 */
	@Override
	public boolean decipher(InputStream historyFile, JobKey jobKey) {

		try {
			jobHistoryListener = new JobHistoryListener(
					jobKey);
			JobHistoryCopy.parseHistoryFromIS(historyFile, jobHistoryListener);
		} catch (IOException ioe) {
			LOG.error(" Exception during parsing hadoop 1.0 file " +ioe.getMessage());
			LOG.error(ioe.getStackTrace());
			return false;
		}
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see com.twitter.hraven.etl.JobHistoryFileDecipherBase#getJobPuts()
	 */
	@Override
	public List<Put> getJobPuts() {
		if (jobHistoryListener != null)
			return jobHistoryListener.getJobPuts();
		else
			return null;
	}

	/*
	 * (non-Javadoc)
	 * @see com.twitter.hraven.etl.JobHistoryFileDecipherBase#getTaskPuts()
	 */
	@Override
	public List<Put> getTaskPuts() {
		if (jobHistoryListener != null)
			return jobHistoryListener.getTaskPuts();
		else
			return null;
	}
}