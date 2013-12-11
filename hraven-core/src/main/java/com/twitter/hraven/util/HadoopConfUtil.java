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

package com.twitter.hraven.util;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import com.twitter.hraven.Constants;
import com.twitter.hraven.datasource.ProcessingException;

/**
 * Utility class for accessing parameters from the Hadoop Conf
 * used in case of parameter name changes across hadoop versions
 */
public class HadoopConfUtil {

	private static Log LOG = LogFactory.getLog(HadoopConfUtil.class);

	/**
	 * Get the user name from the job conf check for hadoop2 config param, then
	 * hadoop1
	 * 
	 * @param jobConf
	 * @return userName
	 * 
	 * @throws IllegalArgumentException
	 */
	public static String getUserNameInConf(Configuration jobConf)
			throws IllegalArgumentException {
		String userName = jobConf.get(Constants.USER_CONF_KEY_HADOOP2);
		if (StringUtils.isBlank(userName)) {
			userName = jobConf.get(Constants.USER_CONF_KEY);
			if (StringUtils.isBlank(userName)) {
				// neither user.name nor hadoop.mapreduce.job.user.name found
				throw new IllegalArgumentException(" Found neither "
						+ Constants.USER_CONF_KEY + " nor "
						+ Constants.USER_CONF_KEY_HADOOP2);
			}
		}
		return userName;
	}

	/**
	 * checks if the jobConf contains a certain parameter
	 * 
	 * @param jobConf
	 * @param name
	 * @return true if the job conf contains that parameter
	 *         false if the job conf does not contain that parameter
	 */
	public static boolean contains(Configuration jobConf, String name) {
		if (StringUtils.isNotBlank(jobConf.get(name))) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * retrieves the queue name from a hadoop conf
	 * looks for hadoop2 and hadoop1 settings
	 * 
	 * @param jobConf
	 * @return queuename
	 */
	public static String getQueueName(Configuration jobConf) {
		// look for the hadoop2 queuename first
		String hRavenQueueName = jobConf.get(Constants.QUEUENAME_HADOOP2);
		if (StringUtils.isBlank(hRavenQueueName)) {
			// presumably a hadoop1 conf, check for fair scheduler pool name
			hRavenQueueName = jobConf
					.get(Constants.FAIR_SCHEDULER_POOLNAME_HADOOP1);
			if (StringUtils.isBlank(hRavenQueueName)) {
				// check for capacity scheduler queue name
				hRavenQueueName = jobConf
						.get(Constants.CAPACITY_SCHEDULER_QUEUENAME_HADOOP1);
				if (StringUtils.isBlank(hRavenQueueName)) {
					// neither pool (hadoop1) nor queuename (hadoop2) found
					// presumably FIFO scheduler, hence set to "DEFAULT_QUEUE"
					hRavenQueueName = Constants.DEFAULT_QUEUENAME;
					LOG.info(" Found neither "
							+ Constants.FAIR_SCHEDULER_POOLNAME_HADOOP1
							+ " nor " + Constants.QUEUENAME_HADOOP2 + " nor "
							+ Constants.CAPACITY_SCHEDULER_QUEUENAME_HADOOP1
							+ " hence presuming FIFO scheduler "
							+ " and setting the queuename to "
							+ Constants.DEFAULT_QUEUENAME);
				}
			}
		}
		return hRavenQueueName;
	}

  /**
   * fetches a value from the jobConf as a long
   * @param key
   * @param jobConf
   * @return value as long for that key in the jobConf defaultValue if key not found
   * @throws {@link} ProcessingException
   */
  public static Long getLongValue(String key, Configuration jobConf, Long defaultValue)
      throws ProcessingException {
    Long value = defaultValue;
    String valueStr = jobConf.get(key);
    if (StringUtils.isNotBlank(valueStr)) {
      try {
        value = Long.parseLong(valueStr);
      } catch (NumberFormatException nfe) {
        throw new ProcessingException("Could not convert " + valueStr + " for key " + key
            + " to long", nfe);
      }
    }
    return value;
  }
}
