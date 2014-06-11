package com.twitter.hraven;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class TestFlowKey {
  @Test
  public void testToString() {
    FlowKey key = new FlowKey("c1@local", "auser", "app", 1345L);
    String expected = "c1@local" + Constants.SEP + "auser"
        + Constants.SEP + "app" + Constants.SEP + 1345L;
    assertEquals(expected, key.toString());
  }
}
