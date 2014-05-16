package com.twitter.hraven;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 */
public class TestFlow {
  private static final String CLUSTER = "cluster@dc";
  private static final String USER = "testuser";
  private static final String APP_ID = "testapp";
  private static final String QUEUE1 = "queue1";

  @Test
  public void testJobAggregation() {
    long runId = System.currentTimeMillis();
    JobDetails job1 = new JobDetails(new JobKey(CLUSTER, USER, APP_ID, runId, "job_20120101000000_0001"));
    job1.setTotalMaps(100);
    job1.setTotalReduces(10);
    job1.setSubmitTime(runId);
    job1.setQueue(QUEUE1);
    CounterMap counters1 = new CounterMap();
    counters1.add(new Counter("group1", "key1", 100));
    counters1.add(new Counter("group2", "key1", 1000));
    job1.setCounters(counters1);

    JobDetails job2 = new JobDetails(new JobKey(CLUSTER, USER, APP_ID, runId, "job_20120101000000_0002"));
    job2.setTotalMaps(10);
    job2.setTotalReduces(1);
    job2.setQueue(QUEUE1 + "2");
    job2.setSubmitTime(runId + 3600000L);
    CounterMap counters2 = new CounterMap();
    counters2.add(new Counter("group2", "key2", 1));
    job2.setCounters(counters2);

    JobDetails job3 = new JobDetails(new JobKey(CLUSTER, USER, APP_ID, runId, "job_20120101000000_0003"));
    job3.setTotalMaps(1000);
    job3.setTotalReduces(10);
    job3.setSubmitTime(runId + 4800000L);
    job3.setQueue(QUEUE1+ "3");
    CounterMap counters3 = new CounterMap();
    counters3.add(new Counter("group1", "key1", 50));
    counters3.add(new Counter("group2", "key1", 100));
    job3.setCounters(counters3);
    job3.setMapCounters(counters3);
    job3.setReduceCounters(counters3);

    Flow flow = new Flow(new FlowKey(CLUSTER, USER, APP_ID, runId));
    flow.addJob(job1);
    flow.addJob(job2);
    flow.addJob(job3);

    assertEquals(3, flow.getJobCount());
    // totalMaps = 100 + 10 + 1000
    assertEquals(1110, flow.getTotalMaps());
    // totalReduces = 10 + 1 + 10
    assertEquals(21, flow.getTotalReduces());
    // ensure the queue for the first job in the flow is set as queue for the flow
    assertTrue(QUEUE1.equals(flow.getQueue()));
    // total counters: group1, key1 = 100 + 50
    assertEquals(150, flow.getCounters().getCounter("group1", "key1").getValue());
    // total counters: group2, key1 = 1000 + 100
    assertEquals(1100, flow.getCounters().getCounter("group2", "key1").getValue());
    // total counters: group2, key2 = 1
    assertEquals(1, flow.getCounters().getCounter("group2", "key2").getValue());
    // map counters: group1, key1 = 50
    assertEquals(50, flow.getMapCounters().getCounter("group1", "key1").getValue());
    // map counters: group2, key1 = 100
    assertEquals(100, flow.getMapCounters().getCounter("group2", "key1").getValue());
    // reduce counters: group1, key1 = 50
    assertEquals(50, flow.getReduceCounters().getCounter("group1", "key1").getValue());
    // reduce counters: group2, key1 = 100
    assertEquals(100, flow.getReduceCounters().getCounter("group2", "key1").getValue());
  }
}
