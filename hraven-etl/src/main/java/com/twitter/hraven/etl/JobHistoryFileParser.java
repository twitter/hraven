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
import java.util.List;
import org.apache.hadoop.hbase.client.Put;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.datasource.ProcessingException;

/**
 * Interface for job history file parsing Should be implemented for parsing
 * different formats of history files change by MAPREDUCE-1016 in hadoop1.0 as
 * well as hadoop2.0
 * 
 */

public interface JobHistoryFileParser {

	/**
	 * this method should parse the history file and populate the puts
	 * 
	 * @throws ProcessingException
	 */
	public void parse(byte[] historyFile, JobKey jobKey);

	/**
	 * Return the generated list of job puts assembled when history file is
	 * parsed
	 * 
	 * @return a list of jobPuts
	 */
	public List<Put> getJobPuts();

	/**
	 * Return the generated list of task puts assembled when history file is
	 * parsed
	 * 
	 * @return a list of taskPuts
	 */
	public List<Put> getTaskPuts();

}
