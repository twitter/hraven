/*
Copyright 2013 Twitter, Inc.

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

import com.twitter.hraven.Constants;
import com.twitter.hraven.Flow;
import com.twitter.hraven.FlowKey;
import com.twitter.hraven.FlowQueueKey;
import com.twitter.hraven.rest.PaginatedResult;
import com.twitter.hraven.util.ByteUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 */
public class FlowQueueService {
  /* Constants for column names */
  public static final String JOB_GRAPH_COL = "dag";
  public static final byte[] JOB_GRAPH_COL_BYTES = Bytes.toBytes(JOB_GRAPH_COL);
  public static final String FLOW_NAME_COL = "flowname";
  public static final byte[] FLOW_NAME_COL_BYTES = Bytes.toBytes(FLOW_NAME_COL);
  public static final String USER_NAME_COL = "username";
  public static final byte[] USER_NAME_COL_BYTES = Bytes.toBytes(USER_NAME_COL);
  public static final String PROGRESS_COL = "progress";
  public static final byte[] PROGRESS_COL_BYTES = Bytes.toBytes(PROGRESS_COL);

  private FlowQueueKeyConverter queueKeyConverter = new FlowQueueKeyConverter();
  private FlowKeyConverter flowKeyConverter = new FlowKeyConverter();

  private HTable flowQueueTable;

  public FlowQueueService(Configuration conf) throws IOException {
    this.flowQueueTable = new HTable(conf, Constants.FLOW_QUEUE_TABLE_BYTES);
  }

  public void updateFlow(FlowQueueKey key, Flow flow) throws IOException {
    Put p = createPutForFlow(key, flow);
    flowQueueTable.put(p);
  }

  /**
   * Moves a flow_queue record from one row key to another.  All KeyValues in the existing row
   * will be written to the new row.  This would primarily be used for transitioning a flow's
   * data from one status to another.
   *
   * @param oldKey the existing row key to move
   * @param newKey the new row key to move to
   * @throws IOException
   */
  public void moveFlow(FlowQueueKey oldKey, FlowQueueKey newKey)
      throws DataException, IOException {
    byte[] oldRowKey = queueKeyConverter.toBytes(oldKey);
    Get get = new Get(oldRowKey);
    Result result = flowQueueTable.get(get);
    if (result == null || result.isEmpty()) {
      // no existing row
      throw new DataException("No row for key "+ Bytes.toStringBinary(oldRowKey));
    }
    // copy the existing row to the new key
    Put p = new Put(queueKeyConverter.toBytes(newKey));
    for (KeyValue kv : result.raw()) {
      p.add(kv.getFamily(), kv.getQualifier(), kv.getValue());
    }
    flowQueueTable.put(p);
    // delete the old row
    Delete d = new Delete(oldRowKey);
    flowQueueTable.delete(d);
  }

  protected Put createPutForFlow(FlowQueueKey key, Flow flow) {
    Put p = new Put(queueKeyConverter.toBytes(key));
    if (flow.getFlowKey() != null) {
      p.add(Constants.INFO_FAM_BYTES, Constants.ROWKEY_COL_BYTES,
          flowKeyConverter.toBytes(flow.getFlowKey()));
    }
    if (flow.getJobGraphJSON() != null) {
      p.add(Constants.INFO_FAM_BYTES, JOB_GRAPH_COL_BYTES, Bytes.toBytes(flow.getJobGraphJSON()));
    }
    if (flow.getFlowName() != null) {
      p.add(Constants.INFO_FAM_BYTES, FLOW_NAME_COL_BYTES, Bytes.toBytes(flow.getFlowName()));
    }
    if (flow.getUserName() != null) {
      p.add(Constants.INFO_FAM_BYTES, USER_NAME_COL_BYTES, Bytes.toBytes(flow.getUserName()));
    }
    p.add(Constants.INFO_FAM_BYTES, PROGRESS_COL_BYTES, Bytes.toBytes(flow.getProgress()));
    return p;
  }

  public Flow getFlowFromQueue(String cluster, long timestamp, String flowId) throws IOException {
    // since flow_queue rows can transition status, we check all at once
    List<Get> gets = new ArrayList<Get>();
    for (Flow.Status status : Flow.Status.values()) {
      FlowQueueKey key = new FlowQueueKey(cluster, status, timestamp, flowId);
      gets.add(new Get(queueKeyConverter.toBytes(key)));
    }
    Result[] results = flowQueueTable.get(gets);
    Flow flow = null;
    for (Result r : results) {
      flow = createFlowFromResult(r);
      if (flow != null) {
        break;
      }
    }
    return flow;
  }

  /**
   * Returns the flows currently listed in the given {@link Flow.Status}
   * @param cluster The cluster where flows have run
   * @param status The flows' status
   * @param limit Return up to this many Flow instances
   * @return a list of up to {@code limit} Flows
   * @throws IOException in the case of an error retrieving the data
   */
  public List<Flow> getFlowsForStatus(String cluster, Flow.Status status, int limit)
    throws IOException {
    return getFlowsForStatus(cluster, status, limit, null, null);
  }

  /**
   * Returns the flows currently listed in the given {@link Flow.Status}
   * @param cluster The cluster where flows have run
   * @param status The flows' status
   * @param limit Return up to this many Flow instances
   * @param user Filter flows returned to only this user (if present)
   * @param startRow Start results at this key.  Use this in combination with {@code limit} to
   *                 support pagination through the results.
   * @return a list of up to {@code limit} Flows
   * @throws IOException in the case of an error retrieving the data
   */
  public List<Flow> getFlowsForStatus(String cluster, Flow.Status status, int limit,
                                      String user, byte[] startRow)
      throws IOException {
    byte[] rowPrefix = ByteUtil.join(Constants.SEP_BYTES,
        Bytes.toBytes(cluster), status.code(), Constants.EMPTY_BYTES);
    if (startRow == null) {
      startRow = rowPrefix;
    }
    Scan scan = new Scan(startRow);
    FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    // early out when prefix ends
    filters.addFilter(new WhileMatchFilter(new PrefixFilter(rowPrefix)));
    if (user != null) {
      SingleColumnValueFilter userFilter = new SingleColumnValueFilter(
          Constants.INFO_FAM_BYTES, USER_NAME_COL_BYTES,
          CompareFilter.CompareOp.EQUAL, Bytes.toBytes(user)
      );
      userFilter.setFilterIfMissing(true);
      filters.addFilter(userFilter);
    }
    scan.setFilter(filters);
    // TODO: need to constrain this by timerange as well to prevent unlimited scans

    // get back the results in a single response
    scan.setCaching(limit);
    List<Flow> results = new ArrayList<Flow>(limit);
    ResultScanner scanner = null;
    try {
      scanner = flowQueueTable.getScanner(scan);
      int cnt = 0;
      for (Result r : scanner) {
        Flow flow = createFlowFromResult(r);
        if (flow != null) {
          cnt++;
          results.add(flow);
        }
        if (cnt >= limit) {
          break;
        }
      }
    } finally {
      if (scanner != null) {
        scanner.close();
      }
    }
    return results;
  }

  /**
   * Returns a page of flows for the given cluster and status
   * @param cluster The cluster for the flows' execution
   * @param status The flows' status
   * @param limit Maximum number of flows to retrieve
   * @param user Filter results to this user, if present
   * @param startRow Start pagination with this row (inclusive), if present
   * @return A page of Flow instances
   * @throws IOException In the case of an error retrieving results
   */
  public PaginatedResult<Flow> getPaginatedFlowsForStatus(String cluster, Flow.Status status,
                                                          int limit, String user, byte[] startRow)
      throws IOException {
    // retrieve one more flow than requested for pagination support
    List<Flow> flows = getFlowsForStatus(cluster, status, limit+1, user, startRow);
    PaginatedResult<Flow> result = new PaginatedResult<Flow>(limit);
    if (flows.size() > limit) {
      result.setValues(flows.subList(0, limit));
      Flow lastFlow = flows.get(limit);
      result.setNextStartRow(queueKeyConverter.toBytes(lastFlow.getQueueKey()));
    } else {
      result.setValues(flows);
    }
    return result;
  }


  protected Flow createFlowFromResult(Result result) {
    if (result == null || result.isEmpty()) {
      return null;
    }
    FlowQueueKey queueKey = queueKeyConverter.fromBytes(result.getRow());
    FlowKey flowKey = null;
    // when flow is first being launched FlowKey may not yet be present
    if (result.containsColumn(Constants.INFO_FAM_BYTES, Constants.ROWKEY_COL_BYTES)) {
      flowKey = flowKeyConverter.fromBytes(
          result.getValue(Constants.INFO_FAM_BYTES, Constants.ROWKEY_COL_BYTES));
    }
    Flow flow = new Flow(flowKey);
    flow.setQueueKey(queueKey);
    if (result.containsColumn(Constants.INFO_FAM_BYTES, JOB_GRAPH_COL_BYTES)) {
      flow.setJobGraphJSON(
          Bytes.toString(result.getValue(Constants.INFO_FAM_BYTES, JOB_GRAPH_COL_BYTES)));
    }
    if (result.containsColumn(Constants.INFO_FAM_BYTES, FLOW_NAME_COL_BYTES)) {
      flow.setFlowName(
          Bytes.toString(result.getValue(Constants.INFO_FAM_BYTES, FLOW_NAME_COL_BYTES)));
    }
    if (result.containsColumn(Constants.INFO_FAM_BYTES, USER_NAME_COL_BYTES)) {
      flow.setUserName(
          Bytes.toString(result.getValue(Constants.INFO_FAM_BYTES, USER_NAME_COL_BYTES)));
    }
    if (result.containsColumn(Constants.INFO_FAM_BYTES, PROGRESS_COL_BYTES)) {
      flow.setProgress(Bytes.toInt(result.getValue(Constants.INFO_FAM_BYTES, PROGRESS_COL_BYTES)));
    }
    return flow;
  }

  public void close() throws IOException {
    if (this.flowQueueTable != null) {
      this.flowQueueTable.close();
    }
  }
}
