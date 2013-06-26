/*
Copyright 2012 Twitter, Inc.

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
import org.junit.Test;

import com.twitter.hraven.JobDescFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestJobDescFactory {
  @Test
  public void testCluster() {
    Configuration c = new Configuration();
    c.set(JobDescFactory.JOBTRACKER_KEY, "cluster1.identifier1.example.com:8021");
    String result = JobDescFactory.getCluster(c);
    assertNotNull(result);
    assertEquals("cluster1@identifier1", result);

    c = new Configuration();
    c.set(JobDescFactory.JOBTRACKER_KEY, "hbase-cluster2.identifier2.example.com:8021");
    result = JobDescFactory.getCluster(c);
    assertNotNull(result);
    assertEquals("hbase-cluster2@identifier2", result);
 
    }
}
