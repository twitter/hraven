package com.twitter.hraven.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
		assertTrue(HadoopConfUtil.constains(jobConf,
				Constants.USER_CONF_KEY_HADOOP2));
		assertFalse(HadoopConfUtil.constains(jobConf, Constants.USER_CONF_KEY));
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
}