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

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.twitter.hraven.Constants;
import com.twitter.hraven.HadoopVersion;
import com.twitter.hraven.JobHistoryKeys;
import com.twitter.hraven.util.HBasePutUtil;

/**
 *  Abstract class for job history file parsing 
 *  
 *  Implements the interface for history file parsing
 *  Adds the implementation for a getHadoopVersionPut function
 *  Other methods to be implemented for parsing by sub classes
 * 
 */

public abstract class JobHistoryFileParserBase implements JobHistoryFileParser {

  private static final Log LOG = LogFactory.getLog(JobHistoryFileParserBase.class);
	/**
	 * generates a put that sets the hadoop version for a record
	 * 
	 * @param historyFileVersion
	 * @param jobKeyBytes
	 * 
	 * @return Put
	 */
	public Put getHadoopVersionPut(HadoopVersion historyFileVersion, byte[] jobKeyBytes) {
	  Put pVersion = new Put(jobKeyBytes);
	  byte[] valueBytes = null;
	  valueBytes = Bytes.toBytes(historyFileVersion.toString());
	  byte[] qualifier = Bytes.toBytes(JobHistoryKeys.hadoopversion.toString().toLowerCase());
	  pVersion.add(Constants.INFO_FAM_BYTES, qualifier, valueBytes);
	  return pVersion;
	}

  /**
   * parses the -Xmx value from the mapred.child.java.opts in the job conf
   * @return Xmx value
   */
  public Long getXmxValue(List<Put> jobConfPuts) {
    // usually appears as the following in the job conf:
    // "mapred.child.java.opts" : "-Xmx3072M"
    String qualifier = Constants.JOB_CONF_COLUMN_PREFIX + Constants.SEP +
        Constants.JAVA_CHILD_OPTS_CONF_KEY;
    String javaOpts = "";
    for (Put p : jobConfPuts) {
      javaOpts = HBasePutUtil.getStringValueFromPut(p, Constants.INFO_FAM_BYTES,
        Bytes.toBytes(qualifier));
    }
    if (StringUtils.isBlank(javaOpts)) {
      LOG.error("No such config parameter? " + javaOpts);
      return 0L;
    }
    String[] XmxStr = javaOpts.split(Constants.JAVA_XMX_PREFIX);
    Long retVal = 0L;
    if (XmxStr.length >= 1) {
      String[] valuesStr = XmxStr[1].split(" ");
      if (valuesStr.length >= 1) {
        // if the last char is an alphabet, remove it
        String valueStr = valuesStr[0];
        char lastChar = valueStr.charAt(valueStr.length() - 1);
        try {
          if (Character.isLetter(lastChar)) {
            String XmxValStr = valuesStr[0].substring(0, valuesStr[0].length() - 1);
            retVal = Long.parseLong(XmxValStr);
          } else {
            retVal = Long.parseLong(valueStr);
            // now convert to megabytes
            // since this was in bytes since the last char was absent
            retVal /= 1024;
          }
        } catch (NumberFormatException nfe) {
          LOG.error(" unable to get the Xmx value from " + javaOpts);
          nfe.printStackTrace();
        }
      }
    }
    return retVal;
  }

}
