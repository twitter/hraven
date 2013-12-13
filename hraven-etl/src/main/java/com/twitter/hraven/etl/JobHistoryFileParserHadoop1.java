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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.mapred.JobHistoryCopy;
import com.twitter.hraven.Constants;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.datasource.ProcessingException;
import com.twitter.hraven.mapreduce.JobHistoryListener;

/**
 * Deal with JobHistory file parsing for job history files which are generated
 * pre MAPREDUCE-1016
 * 
 */
public class JobHistoryFileParserHadoop1 extends JobHistoryFileParserBase {

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
			// set the hadoop version for this record
			Put versionPut = getHadoopVersionPut(JobHistoryFileParserFactory.getHistoryFileVersion1(), 
			  jobHistoryListener.getJobKeyBytes());
			jobHistoryListener.includeHadoopVersionPut(versionPut);
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

  /**
   * calculate mega byte millis as:
   * Total estimated memory (Xmx as 75% see below) * map slot millis +
   * Total estimated memory (Xmx as 75% see below) * reduce slot millis 
   *
   * for hadoop1 jobs, we can
   * consider the -Xmx value to be 75% of the memory used by that task.
   * For eg, if Xmx is set to 3G,
   * we can consider 4G to be the task's memory usage,
   * so that we account for native memory (25% presumption).
   * This way we don't depend on cluster specific memory 
   * and max map and reduce tasks on that cluster
   */
  @Override
  public Long getMegaByteMillis(Configuration jobConf) {

    if (jobHistoryListener == null ) {
      LOG.error("Cannot call getMegaByteMillis before parsing the history file!");
      return Constants.NOTFOUND_VALUE;
    }

    Long mapSlotMillis = jobHistoryListener.getMapSlotMillis();
    Long reduceSlotMillis = jobHistoryListener.getReduceSlotMillis();
    if (mapSlotMillis == Constants.NOTFOUND_VALUE
        || reduceSlotMillis == Constants.NOTFOUND_VALUE) {
      throw new ProcessingException("Cannot calculate megabytemillis "
          + " since mapSlotMillis " + mapSlotMillis 
          + " or reduceSlotMillis " + reduceSlotMillis + " not found!");
    }

    if (jobConf == null) {
      throw new ProcessingException("JobConf is null, cannot calculate megabytemillis");
    }
    Long xmx75 = getXmxValue(jobConf.get(Constants.JAVA_CHILD_OPTS_CONF_KEY));
    if (xmx75 == 0L) {
      /** maximum heap size as per
       * http://docs.oracle.com/javase/6/docs/technotes/guides/vm/gc-ergonomics.html
       * Is smaller of 1/4th of the physical memory or 1GB.
       * hence we assume default of 1GB
       */
      xmx75 = Constants.DEFAULT_XMX_SETTING;
      LOG.info("Xmx value is 0, now presuming default Xmx size "
          + Constants.DEFAULT_XMX_SETTING);
    }
    Long xmxTotal = getXmxTotal(xmx75);
    LOG.trace("\n Xmx " + xmxTotal + ": " + mapSlotMillis + " \n " + ": "
        + reduceSlotMillis + " \n ");
    Long mbMillis = xmxTotal * mapSlotMillis + xmxTotal * reduceSlotMillis;
    return mbMillis;
  }
}
