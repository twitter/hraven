/*
Copyright 2016 Twitter, Inc.

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.twitter.hraven.FlowEvent;
import com.twitter.hraven.FlowEventKey;
import com.twitter.hraven.FlowKey;
import com.twitter.hraven.Framework;

/**
 */
public class TestFlowEventService {
  private static final String TEST_CLUSTER = "test@test";
  private static final String TEST_USER = "testuser";
  private static final String TEST_APP = "TestFlowEventService";

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static Connection hbaseConnection = null;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    UTIL.startMiniCluster();
    HRavenTestUtil.createFlowEventTable(UTIL);
    hbaseConnection =
        ConnectionFactory.createConnection(UTIL.getConfiguration());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    try {
      if (hbaseConnection != null) {
        hbaseConnection.close();
      }
    } finally {
      UTIL.shutdownMiniCluster();
    }
  }

  @Test
  public void testFlowEventReadWrite() throws Exception {
    FlowEventService service = new FlowEventService(hbaseConnection);

    // setup some test data for a couple flows
    long flow1Run = System.currentTimeMillis();
    FlowKey flow1Key = new FlowKey(TEST_CLUSTER, TEST_USER, TEST_APP, flow1Run);
    List<FlowEvent> flow1Events = generateEvents(flow1Key, 5);
    service.addEvents(flow1Events);

    long flow2Run = flow1Run + 10;
    FlowKey flow2Key = new FlowKey(TEST_CLUSTER, TEST_USER, TEST_APP, flow2Run);
    List<FlowEvent> flow2Events = generateEvents(flow2Key, 10);
    service.addEvents(flow2Events);

    // verify we get the right events back for each
    List<FlowEvent> flow1Results = service.getFlowEvents(flow1Key);
    assertEvents(flow1Events, flow1Results);
    List<FlowEvent> flow2Results = service.getFlowEvents(flow2Key);
    assertEvents(flow2Events, flow2Results);
    // check partial results
    FlowEventKey flow2Last = flow2Events.get(4).getFlowEventKey();
    List<FlowEvent> flow2PartialResults = service.getFlowEventsSince(flow2Last);
    assertEvents(flow2Events.subList(5, flow2Events.size()),
        flow2PartialResults);

  }

  private List<FlowEvent> generateEvents(FlowKey flowKey, int count) {
    List<FlowEvent> events = new ArrayList<FlowEvent>(count);
    long now = System.currentTimeMillis();
    for (int i = 1; i <= count; i++) {
      FlowEvent event = new FlowEvent(new FlowEventKey(flowKey, i));
      event.setTimestamp(now + i);
      event.setFramework(Framework.PIG);
      event.setType("test");
      event.setEventDataJSON("event" + i);
      events.add(event);
    }
    return events;
  }

  private void assertEvents(List<FlowEvent> expected, List<FlowEvent> actual) {
    assertNotNull(actual);
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      FlowEvent expectedEvent = expected.get(i);
      FlowEvent actualEvent = actual.get(i);
      assertNotNull(actualEvent);
      assertEquals(expectedEvent.getFlowEventKey(),
          actualEvent.getFlowEventKey());
      assertEquals(expectedEvent.getTimestamp(), actualEvent.getTimestamp());
      assertEquals(expectedEvent.getType(), actualEvent.getType());
      assertEquals(expectedEvent.getFramework(), actualEvent.getFramework());
      assertEquals(expectedEvent.getEventDataJSON(),
          actualEvent.getEventDataJSON());
    }
  }
}
