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
package com.twitter.hraven;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class TestMRJobDescFactory {
  
  @Test
  public void testCreate() {
    MRJobDescFactory mrFac = new MRJobDescFactory();
    Configuration conf = new Configuration();
    QualifiedJobId qid = new QualifiedJobId("clusterId", "job_211212010355_45240");
    
    JobDesc jd = null;
    
    // batch.desc and mapred.job.name are not set
    jd = mrFac.create(qid, 1354772953639L, conf);
    Assert.assertEquals(jd.getAppId(), Constants.UNKNOWN);
    
    // batch.desc is not set, but mapred.job.name is set
    String name = "Crazy Job name! : test 1 2 3!";
    String processedName = "Crazy_Job_name__:_test_1_2_3_";
    conf.set("mapred.job.name", name);
    jd = mrFac.create(qid, 1354772953639L, conf);
    Assert.assertEquals(jd.getAppId(), processedName);
    
    // batch.desc is set and mapred.job.name is set
    name = "Other Crazy Job name! : test 1 2 3!";
    processedName = "Other_Crazy_Job_name__:_test_1_2_3_";
    conf.set("batch.desc", name);
    jd = mrFac.create(qid, 1354772953639L, conf);
    Assert.assertEquals(jd.getAppId(), processedName);
    
    // batch.desc is set set, and mapred.job.name is not set
    conf = new Configuration();
    name = "Third Crazy Job name! : test 1 2 3!";
    processedName = "Third_Crazy_Job_name__:_test_1_2_3_";
    conf.set("batch.desc", name);
    jd = mrFac.create(qid, 1354772953639L, conf);
    Assert.assertEquals(jd.getAppId(), processedName);
  }
}
