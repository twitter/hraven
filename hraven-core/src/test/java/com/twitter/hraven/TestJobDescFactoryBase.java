package com.twitter.hraven;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import static junit.framework.Assert.assertEquals;


public class TestJobDescFactoryBase extends JobDescFactoryBase {

  public static final String UNSAFE_NAME = "soMe long" + Constants.SEP + "name";
  public static final String SAFE_NAME = "soMe_long_name";
  
  /**
   * Not interesting for this particular test.
   * @param qualifiedJobId
   * @param submitTimeMillis
   * @param jobConf
   * @return
   */
  JobDesc create(QualifiedJobId qualifiedJobId, long submitTimeMillis,
      Configuration jobConf) {
    // Not interesting for this test.
    return null;
  }

  /**
   * @param jobName
   * @return
   */
  String getAppIdFromJobName(String jobName) {
    // Identity transform.
    return jobName;
  }
  
  /**
   * Test the method to get the app ID from the JobConf.
   */
  @Test
  public void testgetAppId() {
    Configuration conf = new Configuration();
    conf.set(Constants.APP_NAME_CONF_KEY, UNSAFE_NAME);
    assertEquals(SAFE_NAME, getAppId(conf)); 
  }

  /**
   * Test the method to get the app ID
   * from a hadoop2 JobConf
   */
  @Test
  public void testgetAppIdHadoop2() {
    Configuration conf = new Configuration();
    // ensure hadoop1 config key is blank
    conf.set(Constants.JOB_NAME_CONF_KEY, "");
    // ensure batch.desc is blank
    conf.set(Constants.APP_NAME_CONF_KEY, "");
    // set the hadoop2 config key
    conf.set(Constants.JOB_NAME_HADOOP2_CONF_KEY, "abc.def.xyz");
    assertEquals("abc.def.xyz", getAppId(conf));
  }

}
