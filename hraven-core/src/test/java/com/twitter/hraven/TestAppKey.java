package com.twitter.hraven;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class TestAppKey {
  @Test
  public void testToString() {
    AppKey key = new AppKey("c1@local", "auser", "app");
    String expected = "c1@local" + Constants.SEP + "auser" + Constants.SEP + "app";
    assertEquals(expected, key.toString());
  }
}
