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
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobHistoryCopy;
import com.twitter.hraven.Constants;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.datasource.JobKeyConverter;
import com.twitter.hraven.datasource.ProcessingException;
import com.twitter.hraven.mapreduce.JobHistoryListener;
import com.twitter.hraven.util.HBasePutUtil;

/**
 * Deal with JobHistory file parsing for job history files which are generated
 * pre MAPREDUCE-1016
 * 
 */
public class JobHistoryFileParserHadoop1 extends JobHistoryFileParserBase {

	private static final Log LOG = LogFactory
			.getLog(JobHistoryFileParserHadoop1.class);

	private JobKey jobKey;
	private byte[] jobKeyBytes;
	private JobKeyConverter jobKeyConv = new JobKeyConverter();
	private JobHistoryListener jobHistoryListener = null;
	private List<Put> postProcessedPuts = new LinkedList<Put>();

	/**
	 * {@inheritDoc}
	 * 
	 */
	@Override
	public void parse(byte[] historyFile, JobKey jobKey)
			throws ProcessingException {

		try {
			jobHistoryListener = new JobHistoryListener(jobKey);
			this.jobKey = jobKey;
			this.jobKeyBytes = jobKeyConv.toBytes(jobKey);
			JobHistoryCopy.parseHistoryFromIS(new ByteArrayInputStream(historyFile), jobHistoryListener);
			// set the hadoop version for this record
			Put versionPut = getHadoopVersionPut(JobHistoryFileParserFactory.getHistoryFileVersion1(), jobHistoryListener.getJobKeyBytes());
			jobHistoryListener.includeHadoopVersionPut(versionPut);
		} catch (IOException ioe) {
			LOG.error(" Exception during parsing hadoop 1.0 file ", ioe);
			throw new ProcessingException(
					" Unable to parse history file in function parse, "
							+ "cannot process this record!" + this.jobKey
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


  /**
   * considering the Xmx setting to be 75% of memory used return the total memory (xmx + native)
   */
  Long getXmxTotal(final long Xmx75) {
    return (Xmx75 * 100 / 75);
  }

  @Override
  public List<Put> generatePostProcessedPuts(List<Put> jobConfPuts) {
    Long Xmx75 = getXmxValue(jobConfPuts);
    if (Xmx75 == 0L) {
      // don't want to throw an exception and
      // cause the processing to fail completely
      LOG.error("Xmx value is 0? check why");
      return null;
    }
    Long XmxTotal = getXmxTotal(Xmx75);
    long mapSlotMillis = 0L;
    long reduceSlotMillis = 0L;
    String prefixMillis = Constants.COUNTER_COLUMN_PREFIX + Constants.SEP;
    String mapMillisStr = prefixMillis + Constants.JOBINPROGRESS_COUNTER + Constants.SEP
        + Constants.SLOTS_MILLIS_MAPS;
    String reduceMillisStr = prefixMillis + Constants.JOBINPROGRESS_COUNTER + Constants.SEP
            + Constants.SLOTS_MILLIS_REDUCES;
    for (Put p : jobHistoryListener.getJobPuts()) {
      if (mapSlotMillis == 0L) {
        mapSlotMillis = HBasePutUtil.getLongValueFromPut(p, Constants.INFO_FAM_BYTES, Bytes.toBytes(mapMillisStr));
        if (mapSlotMillis == Constants.LONG_CONVERSION_ERROR_VALUE) {
          return null;
        }
      }
      if (reduceSlotMillis == 0L) {
        reduceSlotMillis = HBasePutUtil.getLongValueFromPut(p, Constants.INFO_FAM_BYTES,
            Bytes.toBytes(reduceMillisStr));
        if (reduceSlotMillis == Constants.LONG_CONVERSION_ERROR_VALUE) {
          return null;
        }
      }
    }

    LOG.trace("For " + jobKey.toString() +  " \n Xmx " + XmxTotal + " " + mapMillisStr
        + ": " + mapSlotMillis + " \n " + reduceMillisStr
        + ": " + reduceSlotMillis + " \n " );
    Long mbMillis = XmxTotal * mapSlotMillis + XmxTotal * reduceSlotMillis;
    byte[] qualifier = Constants.MEGABYTEMILLIS_BYTES;
    Put pMb = new Put(jobKeyBytes);
    pMb.add(Constants.INFO_FAM_BYTES, qualifier, Bytes.toBytes(mbMillis));
    postProcessedPuts.add(pMb);
    return postProcessedPuts;
  }
}
