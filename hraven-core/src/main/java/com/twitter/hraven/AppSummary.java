/*
Copyright 2014 Twitter, Inc.

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

import java.util.HashSet;
import java.util.Set;

import org.codehaus.jackson.map.annotate.JsonSerialize;

import com.twitter.hraven.datasource.ProcessingException;

/**
 * Represents summary information about a hadoop application
 *
 * cluster, user, application name identify this app via {@linkplain AppKey}
 *
 * Used to represent collective statistics of an app
 * either over a period of time or any other summary reporting
 *
 * An actual instance of an app run is represented by {@linkplain Flow}
 */
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class AppSummary {

  /** the key that uniquely identifies this hadoop application */
  private AppKey appKey;

  /** number of runs in this summary */
  private long numberRuns;

  /** run id of the first time this app ran in this summary */
  private long firstRunId;

  /** run id of the last time this app ran in this summary */
  private long lastRunId;

  /** how many hadoop jobs in this summary */
  private long jobCount;

  /** Number of map tasks in this summary */
  private long totalMaps;

  /** Number of reduce tasks in this summary */
  private long totalReduces;

  /** total cost of this app in this summary */
  private double cost;

  /** map slot millis it has taken up */
  private long mapSlotMillis;

  /** reduce slot millis it has taken up */
  private long reduceSlotMillis;

  /** mega byte millis it has taken up */
  private long mbMillis;

  /** the queue(s) this app ran in, in the context of this summary*/
  private Set<String> queues;

  public AppSummary(AppKey key) {
    this.appKey = key;
    this.queues = new HashSet<String>();
  }

  public AppKey getKey() {
    return appKey;
  }

  public void setKey(AppKey key) {
    this.appKey = key;
  }

  public double getCost() {
    return cost;
  }

  public void setCost(double cost) {
    this.cost = cost;
  }

  public long getMbMillis() {
    return mbMillis;
  }

  public void setMbMillis(long mbMillis) {
    this.mbMillis = mbMillis;
  }

  public Set<String> getQueue() {
    return queues;
  }

  public void setQueue(Set<String> queue) {
    this.queues = queue;
  }

  public void addQueue(String aQueue) {
    if (this.queues != null) {
      this.queues.add(aQueue);
    } else {
      throw new ProcessingException("Could not add pool to list of queue for this app "
          + this.appKey);
    }
  }

  public long getNumberRuns() {
    return numberRuns;
  }

  public void setNumberRuns(long numberRuns) {
    this.numberRuns = numberRuns;
  }

  public long getJobCount() {
    return jobCount;
  }

  public void setJobCount(long jobCount) {
    this.jobCount = jobCount;
  }

  public long getMapSlotMillis() {
    return mapSlotMillis;
  }

  public void setMapSlotMillis(long mapSlotMillis) {
    this.mapSlotMillis = mapSlotMillis;
  }

  public long getReduceSlotMillis() {
    return reduceSlotMillis;
  }

  public void setReduceSlotMillis(long reduceSlotMillis) {
    this.reduceSlotMillis = reduceSlotMillis;
  }

  public long getTotalMaps() {
    return totalMaps;
  }

  public void setTotalMaps(long totalMaps) {
    this.totalMaps = totalMaps;
  }

  public long getTotalReduces() {
    return totalReduces;
  }

  public void setTotalReduces(long totalReduces) {
    this.totalReduces = totalReduces;
  }

  public long getFirstRunId() {
    return firstRunId;
  }

  public void setFirstRunId(long firstRunId) {
    this.firstRunId = firstRunId;
  }

  public long getLastRunId() {
    return lastRunId;
  }

  public void setLastRunId(long lastRunId) {
    this.lastRunId = lastRunId;
  }

  /**
   * adds a flow (a run of the app) of the app summary
   * @param flow
   */
  public void addFlow(Flow flow) {

    // add the flow stats to this app summary
    this.numberRuns++;
    this.jobCount += flow.getJobCount();
    this.mapSlotMillis += flow.getMapSlotMillis();
    this.reduceSlotMillis += flow.getReduceSlotMillis();
    this.totalMaps += flow.getTotalMaps();
    this.totalReduces += flow.getTotalReduces();
    this.mbMillis += flow.getMegabyteMillis();

    // add the queue of this flow to the set of queues in this app summary
    this.queues.add(flow.getQueue());

    /**  TODO add jobcost once job cost has been added to job details and flow */

    // store the latest timestamp seen for this app summary
    // since these are epoch timestamps, a bigger number means more recent
    if ((this.lastRunId == 0L) || (this.lastRunId < flow.getRunId())) {
      this.lastRunId = flow.getRunId();
    }

    // store the oldest seen timestamp seen for this app summary
    // since these are epoch timestamps, a smaller number means older run
    if ((this.firstRunId == 0L) || (this.firstRunId > flow.getRunId())) {
      this.firstRunId = flow.getRunId();
    }

  }

}
