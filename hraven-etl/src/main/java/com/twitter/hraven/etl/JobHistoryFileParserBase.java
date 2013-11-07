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

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.twitter.hraven.Constants;
import com.twitter.hraven.JobHistoryKeys;

/**
 *  Abstract class for job history file parsing 
 *  
 *  Implements the interface for history file parsing
 *  Adds the implementation for a getHadoopVersionPut function
 *  Other methods to be implemented for parsing by sub classes
 * 
 */

public abstract class JobHistoryFileParserBase implements JobHistoryFileParser {

	/**
	 * generates a put that sets the hadoop version for a record
	 * 
	 * @param historyFileVersion
	 * @param jobKeyBytes
	 * 
	 * @return Put
	 */
	public Put getHadoopVersionPut(int historyFileVersion, byte[] jobKeyBytes) {
	  Put pVersion = new Put(jobKeyBytes);
	  byte[] valueBytes = null;
	  valueBytes = Bytes.toBytes(Integer.toString(historyFileVersion));
	  byte[] qualifier = Bytes.toBytes(JobHistoryKeys.hadoopversion.toString().toLowerCase());
	  pVersion.add(Constants.INFO_FAM_BYTES, qualifier, valueBytes);
	  return pVersion;
	}
}
