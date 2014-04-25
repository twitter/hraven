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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TestJobDescFactory {
  @Test
  public void testCluster() {

    // load the test properties file
    Cluster.loadHadoopClustersProps("testhRavenClusters.properties");

    Configuration c = new Configuration(false);
    c.set(JobDescFactory.JOBTRACKER_KEY, "cluster1.identifier1.example.com:8021");
    String result = JobDescFactory.getCluster(c);
    assertEquals("cluster1@identifier1", result);

    c = new Configuration(false);
    c.set(JobDescFactory.JOBTRACKER_KEY, "hbase-cluster2.identifier2.example.com:8021");
    result = JobDescFactory.getCluster(c);
    assertEquals("hbase-cluster2@identifier2", result);
    
    c = new Configuration(false);
    c.set(JobDescFactory.RESOURCE_MANAGER_KEY, "cluster2.identifier2.example.com:10020");
    result = JobDescFactory.getCluster(c);
    assertEquals("cluster2@identifier2", result);
    
    c = new Configuration(false);
    c.set(JobDescFactory.JOBTRACKER_KEY, "");
    result = JobDescFactory.getCluster(c);
    assertNull(result);
 
    c = new Configuration(false);
    result = JobDescFactory.getCluster(c);
    assertNull(result);  
  }
}
