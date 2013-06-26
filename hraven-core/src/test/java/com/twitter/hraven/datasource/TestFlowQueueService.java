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
package com.twitter.hraven.datasource;

import com.twitter.hraven.Flow;
import com.twitter.hraven.FlowQueueKey;
import com.twitter.hraven.datasource.FlowQueueService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 */
public class TestFlowQueueService {
  private static Log LOG = LogFactory.getLog(TestFlowQueueService.class);
  private static HBaseTestingUtility UTIL;
  private static final String TEST_CLUSTER = "test@test";
  private static final String TEST_USER = "testuser";

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    UTIL = new HBaseTestingUtility();
    UTIL.startMiniCluster();
    HRavenTestUtil.createFlowQueueTable(UTIL);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testFlowQueueReadWrite() throws Exception {
    FlowQueueService service = new FlowQueueService(UTIL.getConfiguration());

    // add a couple of test flows
    FlowQueueKey key1 = new FlowQueueKey(TEST_CLUSTER, Flow.Status.RUNNING,
        System.currentTimeMillis(), "flow1");
    Flow flow1 = new Flow(null);
    flow1.setJobGraphJSON("{}");
    flow1.setFlowName("flow1");
    flow1.setUserName(TEST_USER);
    flow1.setProgress(10);
    service.updateFlow(key1, flow1);

    FlowQueueKey key2 = new FlowQueueKey(TEST_CLUSTER, Flow.Status.RUNNING,
        System.currentTimeMillis(), "flow2");
    Flow flow2 = new Flow(null);
    flow2.setJobGraphJSON("{}");
    flow2.setFlowName("flow2");
    flow2.setUserName(TEST_USER);
    flow2.setProgress(20);
    service.updateFlow(key2, flow2);

    // read back one flow
    Flow flow1Retrieved = service.getFlowFromQueue(key1.getCluster(), key1.getTimestamp(),
        key1.getFlowId());
    assertNotNull(flow1Retrieved);
    assertFlowEquals(key1, flow1, flow1Retrieved);

    // try reading both flows back
    List<Flow> running = service.getFlowsForStatus(TEST_CLUSTER, Flow.Status.RUNNING, 10);
    assertNotNull(running);
    assertEquals(2, running.size());

    // results should be in reverse order by timestamp
    Flow result1 = running.get(1);
    assertFlowEquals(key1, flow1, result1);
    Flow result2 = running.get(0);
    assertFlowEquals(key2, flow2, result2);

    // move both flows to successful status
    FlowQueueKey newKey1 = new FlowQueueKey(key1.getCluster(), Flow.Status.SUCCEEDED,
        key1.getTimestamp(), key1.getFlowId());
    service.moveFlow(key1, newKey1);
    FlowQueueKey newKey2 = new FlowQueueKey(key2.getCluster(), Flow.Status.SUCCEEDED,
        key2.getTimestamp(), key2.getFlowId());
    service.moveFlow(key2, newKey2);

    List<Flow> succeeded = service.getFlowsForStatus(TEST_CLUSTER, Flow.Status.SUCCEEDED, 10);
    assertNotNull(succeeded);
    assertEquals(2, succeeded.size());
    // results should still be in reverse order by timestamp
    result1 = succeeded.get(1);
    assertFlowEquals(newKey1, flow1, result1);
    result2 = succeeded.get(0);
    assertFlowEquals(newKey2, flow2, result2);
  }

  protected void assertFlowEquals(FlowQueueKey expectedKey, Flow expectedFlow, Flow resultFlow) {
    assertNotNull(resultFlow.getQueueKey());
    LOG.info("Expected queue key is " + expectedKey);
    LOG.info("Result queue key is "+resultFlow.getQueueKey());
    assertTrue(expectedKey.equals(resultFlow.getQueueKey()));
    assertEquals(expectedFlow.getJobGraphJSON(), resultFlow.getJobGraphJSON());
    assertEquals(expectedFlow.getFlowName(), resultFlow.getFlowName());
    assertEquals(expectedFlow.getUserName(), resultFlow.getUserName());
    assertEquals(expectedFlow.getProgress(), resultFlow.getProgress());
  }
}
