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
import java.util.List;
import java.util.Set;

import org.codehaus.jackson.map.annotate.JsonSerialize;

import com.twitter.hraven.datasource.ProcessingException;

/**
 * Represents a hadoop application that runs on a hadoop cluster a cluster, user, application name
 * identify this app via {@linkplain AppKey} Can be used to represent collective statistics of an
 * app over a period of time An actual instance of an app run is represented by {@linkplain Flow}
 */
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class App {

  /** the key that uniquely identifies this hadoop application */
  private AppKey appKey;

  /** how many times this app ran in the given time */
  private long numberRuns;

  /**
   * run id of the first time this app ran in the given time range
   */
  private long firstRunId;

  /**
   * run id of the last time this app ran in the given time range
   */
  private long lastRunId;

  /** how many hadoop jobs ran */
  private long jobCount;

  /** Number of map tasks in this flow */
  private long totalMaps;

  /** Number of reduce tasks in this flow */
  private long totalReduces;

  /** cost of this app */
  private double cost;

  /** map slot millis it has taken up */
  private long mapSlotMillis;

  /** reduce slot millis it has taken up */
  private long reduceSlotMillis;

  /** mega byte millis it has taken up */
  private long mbMillis;

  /** the queue(s) this app ran in */
  private Set<String> queue;

  public App(AppKey key) {
    this.appKey = key;
    this.queue = new HashSet<String>();
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
    return queue;
  }

  public void setQueue(Set<String> queue) {
    this.queue = queue;
  }

  public void addQueue(String aQueue) {
    if (this.queue != null) {
      this.queue.add(aQueue);
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
   * populates details of this app given these runs
   * @param flows
   */
  public void populateDetails(List<Flow> flows) {

    for (Flow f : flows) {
      this.numberRuns++;
      this.jobCount += f.getJobCount();
      this.mapSlotMillis += f.getMapSlotMillis();
      this.reduceSlotMillis += f.getReduceSlotMillis();
      this.totalMaps += f.getTotalMaps();
      this.totalReduces += f.getTotalReduces();
      this.mbMillis += f.getMegabyteMillis();
      this.queue.add(f.getQueue());
      /**
       * TODO add jobcost once job cost has been added to job details and flow
       */
    }

    /**
     * assumes flows are sorted as per their run ids
     * which should be the case since hbase stores row
     * keys in lexicographically sorted order
     * the most recent flows are stored first
     */
    if (flows.size() > 0) {
      this.lastRunId = flows.get(0).getRunId();
      this.firstRunId = flows.get(flows.size() - 1).getRunId();
    }

  }

}
