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

import com.twitter.hraven.rest.PaginatedResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 */
public class TestFlowQueueService {
  private static Log LOG = LogFactory.getLog(TestFlowQueueService.class);
  private static HBaseTestingUtility UTIL;
  private static final String TEST_CLUSTER = "test@test";
  private static final String TEST_USER = "testuser";
  private static final String TEST_USER2 = "testuser2";

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
    Flow flow1 = createFlow(service, TEST_USER, 1);
    FlowQueueKey key1 = flow1.getQueueKey();
    Flow flow2 = createFlow(service, TEST_USER, 2);
    FlowQueueKey key2 = flow2.getQueueKey();

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

    // add flows from a second user
    Flow flow3 = createFlow(service, TEST_USER2, 3);
    FlowQueueKey key3 = flow3.getQueueKey();

    // 3rd should be the only one running
    running = service.getFlowsForStatus(TEST_CLUSTER, Flow.Status.RUNNING, 10);
    assertNotNull(running);
    assertEquals(1, running.size());
    assertFlowEquals(key3, flow3, running.get(0));

    // move flow3 to succeeded
    FlowQueueKey newKey3 = new FlowQueueKey(key3.getCluster(), Flow.Status.SUCCEEDED,
        key3.getTimestamp(), key3.getFlowId());
    service.moveFlow(key3, newKey3);

    succeeded = service.getFlowsForStatus(TEST_CLUSTER, Flow.Status.SUCCEEDED, 10);
    assertNotNull(succeeded);
    assertEquals(3, succeeded.size());
    Flow result3 = succeeded.get(0);
    assertFlowEquals(newKey3, flow3, result3);

    // test filtering by user name
    succeeded = service.getFlowsForStatus(TEST_CLUSTER, Flow.Status.SUCCEEDED, 10,
        TEST_USER2, null);
    assertNotNull(succeeded);
    assertEquals(1, succeeded.size());
    assertFlowEquals(newKey3, flow3, succeeded.get(0));

    // test pagination
    PaginatedResult<Flow> page1 = service.getPaginatedFlowsForStatus(
        TEST_CLUSTER, Flow.Status.SUCCEEDED, 1, null, null);
    List<Flow> pageValues = page1.getValues();
    assertNotNull(pageValues);
    assertNotNull(page1.getNextStartRow());
    assertEquals(1, pageValues.size());
    assertFlowEquals(newKey3, flow3, pageValues.get(0));
    // page 2
    PaginatedResult<Flow> page2 = service.getPaginatedFlowsForStatus(
        TEST_CLUSTER, Flow.Status.SUCCEEDED, 1, null, page1.getNextStartRow());
    pageValues = page2.getValues();
    assertNotNull(pageValues);
    assertNotNull(page2.getNextStartRow());
    assertEquals(1, pageValues.size());
    assertFlowEquals(newKey2, flow2, pageValues.get(0));
    // page 3
    PaginatedResult<Flow> page3 = service.getPaginatedFlowsForStatus(
        TEST_CLUSTER, Flow.Status.SUCCEEDED, 1, null, page2.getNextStartRow());
    pageValues = page3.getValues();
    assertNotNull(pageValues);
    assertNull(page3.getNextStartRow());
    assertEquals(1, pageValues.size());
    assertFlowEquals(newKey1, flow1, pageValues.get(0));
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

  protected Flow createFlow(FlowQueueService service, String user, int cnt) throws Exception {
    String flowName = "flow"+Integer.toString(cnt);
    FlowQueueKey key = new FlowQueueKey(TEST_CLUSTER, Flow.Status.RUNNING,
        System.currentTimeMillis(), flowName);
    Flow flow = new Flow(null);
    flow.setQueueKey(key);
    flow.setJobGraphJSON("{}");
    flow.setFlowName(flowName);
    flow.setUserName(user);
    flow.setProgress(10 * cnt);
    service.updateFlow(key, flow);
    return flow;
  }
}
