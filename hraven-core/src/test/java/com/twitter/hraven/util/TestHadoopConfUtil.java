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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import com.twitter.hraven.Constants;

public class TestHadoopConfUtil {

	@Test
	public void testContains() throws FileNotFoundException {
		final String JOB_CONF_FILE_NAME = "src/test/resources/job_1329348432655_0001_conf.xml";
		Configuration jobConf = new Configuration();
		jobConf.addResource(new FileInputStream(JOB_CONF_FILE_NAME));
		assertTrue(HadoopConfUtil.contains(jobConf,
				Constants.USER_CONF_KEY_HADOOP2));
		assertFalse(HadoopConfUtil.contains(jobConf, Constants.USER_CONF_KEY));
	}

	@Test
	public void testGetUserNameInConf() throws FileNotFoundException {
		final String JOB_CONF_FILE_NAME = "src/test/resources/job_1329348432655_0001_conf.xml";
		Configuration jobConf = new Configuration();
		jobConf.addResource(new FileInputStream(JOB_CONF_FILE_NAME));
		String userName = HadoopConfUtil.getUserNameInConf(jobConf);
		assertEquals(userName, "user");
	}

	@Test
	public void testGetQueueName() throws FileNotFoundException {
		final String JOB_CONF_FILE_NAME = "src/test/resources/job_1329348432655_0001_conf.xml";
		Configuration jobConf = new Configuration();
		jobConf.addResource(new FileInputStream(JOB_CONF_FILE_NAME));
		String queueName = HadoopConfUtil.getQueueName(jobConf);
		assertEquals(queueName, "default");
	}

	@Test(expected=IllegalArgumentException.class)
	public void checkUserNameAlwaysSet() throws FileNotFoundException {
		  final String JOB_CONF_FILE_NAME =
			        "src/test/resources/job_1329348432655_0001_conf.xml";

		  Configuration jobConf = new Configuration();
		  jobConf.addResource(new FileInputStream(JOB_CONF_FILE_NAME));

		  // unset the user name to confirm exception thrown
		  jobConf.set(Constants.USER_CONF_KEY_HADOOP2, "");
		  jobConf.set(Constants.USER_CONF_KEY, "");
		  // test the hraven user name setting
		  String hRavenUserName = HadoopConfUtil.getUserNameInConf(jobConf);
		  assertNull(hRavenUserName);
	  }

}