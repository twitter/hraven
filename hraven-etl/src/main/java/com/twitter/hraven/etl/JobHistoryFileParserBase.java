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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.twitter.hraven.Constants;
import com.twitter.hraven.HadoopVersion;
import com.twitter.hraven.JobHistoryKeys;
import com.twitter.hraven.datasource.ProcessingException;

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
   * extract the string around Xmx in the java child opts " -Xmx1024m -verbose:gc"
   * @param javaChildOptsStr
   * @return
   */
  static String extractXmxValueStr(String javaChildOptsStr) {
    if (StringUtils.isBlank(javaChildOptsStr)) {
      LOG.info("Null/empty input argument to get xmxValue, returning "
          + Constants.DEFAULT_XMX_SETTING_STR);
      return Constants.DEFAULT_XMX_SETTING_STR;
     }
    // first split based on "-Xmx" in "-Xmx1024m -verbose:gc"
    final String JAVA_XMX_PREFIX = "-Xmx";
    String[] xmxStr = javaChildOptsStr.split(JAVA_XMX_PREFIX);
    if (xmxStr.length >= 2) {
      // xmxStr[0] is ''
      // and XmxStr[1] is "1024m -verbose:gc"
      String[] valuesStr = xmxStr[1].split(" ");
      // split on whitespace
      if (valuesStr.length >= 1) {
        // now valuesStr[0] is "1024m"
        return valuesStr[0];
      } else {
        LOG.info("Strange Xmx setting, returning default " + javaChildOptsStr);
        return Constants.DEFAULT_XMX_SETTING_STR;
      }
    } else {
      // Xmx is not present in java child opts
      LOG.info("Xmx setting absent, returning default " + javaChildOptsStr);
      return Constants.DEFAULT_XMX_SETTING_STR;
    }
  }

  /**
   * parses the -Xmx value from the mapred.child.java.opts
   * in the job conf usually appears as the
   * following in the job conf:
   * "mapred.child.java.opts" : "-Xmx3072M"
   * or
   * "mapred.child.java.opts" :" -Xmx1024m -verbose:gc -Xloggc:/tmp/@taskid@.gc
   * @return xmx value in MB
   */
  public static Long getXmxValue(String javaChildOptsStr) {
    Long retVal = 0L;
    String valueStr = extractXmxValueStr(javaChildOptsStr);
    char lastChar = valueStr.charAt(valueStr.length() - 1);
    try {
      if (Character.isLetter(lastChar)) {
        String xmxValStr = valueStr.substring(0, valueStr.length() - 1);
        retVal = Long.parseLong(xmxValStr);
        switch (lastChar) {
        case 'M':
        case 'm':
          // do nothing, since it's already in megabytes
          break;
        case 'K':
        case 'k':
          // convert kilobytes to megabytes
          retVal /= 1024;
          break;
        case 'G':
        case 'g':
          // convert gigabytes to megabtyes
          retVal *= 1024;
          break;
        default:
          throw new ProcessingException("Unable to get the Xmx value from " + javaChildOptsStr
              + " invalid value for Xmx " + xmxValStr);
       }
      } else {
        retVal = Long.parseLong(valueStr);
        // now convert to megabytes
        // since this was in bytes since the last char was absent
        retVal /= (1024 * 1024);
      }
    } catch (NumberFormatException nfe) {
      LOG.error("Unable to get the Xmx value from " + javaChildOptsStr);
      nfe.printStackTrace();
      throw new ProcessingException("Unable to get the Xmx value from " + javaChildOptsStr, nfe);
    }
    return retVal;
  }

  /**
   * considering the Xmx setting to be 75% of memory used 
   * return the total memory (xmx + native)
   */
  public static Long getXmxTotal(final long xmx75) {
    return (xmx75 * 100 / 75);
  }
}
