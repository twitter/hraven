package com.twitter.hraven;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

/**
 */
public class TestCounterMap {
  @Test
  public void testIterator() {
    Counter g1k1 = new Counter("group1", "key1", 10);
    Counter g1k2 = new Counter("group1", "key2", 20);
    Counter g2k1 = new Counter("group2", "key1", 100);
    Counter g3k1 = new Counter("group3", "key1", 200);

    CounterMap base = new CounterMap();
    base.add(g1k1);
    base.add(g1k2);
    base.add(g2k1);
    base.add(g3k1);

    CounterMap copy = new CounterMap();
    copy.addAll(base);

    Counter c = copy.getCounter("group1", "key1");
    assertNotNull(c);
    assertEquals(g1k1, c);
    c = copy.getCounter("group1", "key2");
    assertNotNull(c);
    assertEquals(g1k2, c);
    c = copy.getCounter("group2", "key1");
    assertNotNull(c);
    assertEquals(g2k1, c);
    c = copy.getCounter("group3", "key1");
    assertNotNull(c);
    assertEquals(g3k1, c);
  }
}
