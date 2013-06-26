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
import com.twitter.hraven.datasource.FlowQueueKeyConverter;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 */
public class TestFlowQueueKeyConverter {
  @Test
  public void testFlowQueueKey() throws Exception {
    FlowQueueKeyConverter conv = new FlowQueueKeyConverter();

    long now = System.currentTimeMillis();
    FlowQueueKey key1 = new FlowQueueKey("test@test", Flow.Status.RUNNING, now, "flow1");

    byte[] key1Bytes = conv.toBytes(key1);
    FlowQueueKey key2 = conv.fromBytes(key1Bytes);
    assertNotNull(key2);
    assertEquals(key1.getCluster(), key2.getCluster());
    assertEquals(key1.getStatus(), key2.getStatus());
    assertEquals(key1.getTimestamp(), key2.getTimestamp());
    assertEquals(key1.getFlowId(), key2.getFlowId());
  }
}
