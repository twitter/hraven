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
import com.twitter.hraven.FlowEvent;
import com.twitter.hraven.FlowEventKey;
import com.twitter.hraven.FlowKey;
import com.twitter.hraven.Framework;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Service for reading and writing rows in the {@link Constants#FLOW_EVENT_TABLE} table
 */
public class FlowEventService {
  private static Log LOG = LogFactory.getLog(FlowEventService.class);

  public static final String TIMESTAMP_COL = "ts";
  public static final byte[] TIMESTAMP_COL_BYTES = Bytes.toBytes(TIMESTAMP_COL);

  public static final String TYPE_COL = "type";
  public static final byte[] TYPE_COL_BYTES = Bytes.toBytes(TYPE_COL);

  public static final String DATA_COL = "data";
  public static final byte[] DATA_COL_BYTES = Bytes.toBytes(DATA_COL);

  private final Configuration conf;
  private final Connection conn;
  private Table eventTable;
  private FlowKeyConverter flowKeyConverter = new FlowKeyConverter();
  private FlowEventKeyConverter keyConverter = new FlowEventKeyConverter();

  /**
   * Opens a new connection to HBase server and opens connections to the tables.
   *
   * User is responsible for calling {@link #close()} when finished using this service.
   *
   * @param hbaseConf
   *          configuration of the processing job, not the conf of the files we
   *          are processing. Used to connect to HBase.
   * @throws IOException
   */
  public FlowEventService(Configuration hbaseConf) throws IOException {
    if (hbaseConf == null) {
      conf = new Configuration();
    } else {
      conf = hbaseConf;
    }

    conn = ConnectionFactory.createConnection(conf);

    eventTable = conn.getTable(TableName.valueOf(Constants.FLOW_EVENT_TABLE_BYTES));
  }

  /**
   * close open connections to tables and the hbase cluster.
   * @throws IOException
   */
  public void close() throws IOException {
    IOException ret = null;

    try {
      if (eventTable != null) {
        eventTable.close();
      }
    } catch (IOException ioe) {
      LOG.error(ioe);
      ret = ioe;
    }

    try {
      if (conn != null) {
        conn.close();
      }
    } catch (IOException ioe) {
      LOG.error(ioe);
      ret = ioe;
    }

    if (ret != null) {
      throw ret;
    }
  }

  /**
   * Stores a single flow event row
   * @param event
   * @throws IOException
   */
  public void addEvent(FlowEvent event) throws IOException {
    Put p = createPutForEvent(event);
    eventTable.put(p);
  }

  /**
   * Stores a batch of events
   * @param events
   * @throws IOException
   */
  public void addEvents(List<FlowEvent> events) throws IOException {
    List<Put> puts = new ArrayList<Put>(events.size());
    for (FlowEvent e : events) {
      puts.add(createPutForEvent(e));
    }
    eventTable.put(puts);
  }

  /**
   * Retrieves all the event rows matching a single {@link com.twitter.hraven.Flow}.
   * @param flowKey
   * @return
   */
  public List<FlowEvent> getFlowEvents(FlowKey flowKey) throws IOException {
    byte[] startKey = Bytes.add(flowKeyConverter.toBytes(flowKey), Constants.SEP_BYTES);
    Scan scan = new Scan(startKey);
    scan.setFilter(new WhileMatchFilter(new PrefixFilter(startKey)));

    List<FlowEvent> results = new ArrayList<FlowEvent>();
    ResultScanner scanner = null;
    try {
      scanner = eventTable.getScanner(scan);
      for (Result r : scanner) {
        FlowEvent event = createEventFromResult(r);
        if (event != null) {
          results.add(event);
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
   * Retrieves all events added after the given event key (with sequence numbers greater than the
   * given key).  If no new events are found returns an empty list.
   * @param lastSeen
   * @return
   */
  public List<FlowEvent> getFlowEventsSince(FlowEventKey lastSeen) throws IOException {
    // rows must match the FlowKey portion + SEP
    byte[] keyPrefix = Bytes.add(flowKeyConverter.toBytes(lastSeen), Constants.SEP_BYTES);
    // start at the next following sequence number
    FlowEventKey nextEvent = new FlowEventKey(lastSeen.getCluster(), lastSeen.getUserName(),
        lastSeen.getAppId(), lastSeen.getRunId(), lastSeen.getSequence()+1);
    byte[] startKey = keyConverter.toBytes(nextEvent);
    Scan scan = new Scan(startKey);
    scan.setFilter(new WhileMatchFilter(new PrefixFilter(keyPrefix)));

    List<FlowEvent> results = new ArrayList<FlowEvent>();
    ResultScanner scanner = null;
    try {
      scanner = eventTable.getScanner(scan);
      for (Result r : scanner) {
        FlowEvent event = createEventFromResult(r);
        if (event != null) {
          results.add(event);
        }
      }
    } finally {
      if (scanner != null) {
        scanner.close();
      }
    }
    return results;
  }

  protected Put createPutForEvent(FlowEvent event) {
    Put p = new Put(keyConverter.toBytes(event.getFlowEventKey()));
    p.add(Constants.INFO_FAM_BYTES, TIMESTAMP_COL_BYTES, Bytes.toBytes(event.getTimestamp()));
    if (event.getType() != null) {
      p.add(Constants.INFO_FAM_BYTES, TYPE_COL_BYTES, Bytes.toBytes(event.getType()));
    }
    if (event.getFramework() != null) {
      p.add(Constants.INFO_FAM_BYTES, Constants.FRAMEWORK_COLUMN_BYTES,
          Bytes.toBytes(event.getFramework().getCode()));
    }
    if (event.getEventDataJSON() != null) {
      p.add(Constants.INFO_FAM_BYTES, DATA_COL_BYTES, Bytes.toBytes(event.getEventDataJSON()));
    }
    return p;
  }

  protected FlowEvent createEventFromResult(Result result) {
    if (result == null || result.isEmpty()) {
      return null;
    }
    FlowEventKey key = keyConverter.fromBytes(result.getRow());
    FlowEvent event = new FlowEvent(key);
    if (result.containsColumn(Constants.INFO_FAM_BYTES, TIMESTAMP_COL_BYTES)) {
      event.setTimestamp(Bytes.toLong(
          result.getValue(Constants.INFO_FAM_BYTES, TIMESTAMP_COL_BYTES)));
    }
    if (result.containsColumn(Constants.INFO_FAM_BYTES, TYPE_COL_BYTES)) {
      event.setType(Bytes.toString(result.getValue(Constants.INFO_FAM_BYTES, TYPE_COL_BYTES)));
    }
    if (result.containsColumn(Constants.INFO_FAM_BYTES, Constants.FRAMEWORK_COLUMN_BYTES)) {
      String code = Bytes.toString(result.getValue(
          Constants.INFO_FAM_BYTES, Constants.FRAMEWORK_COLUMN_BYTES));
      event.setFramework(Framework.get(code));
    }
    if (result.containsColumn(Constants.INFO_FAM_BYTES, DATA_COL_BYTES)) {
      event.setEventDataJSON(Bytes.toString(
          result.getValue(Constants.INFO_FAM_BYTES, DATA_COL_BYTES)));
    }
    return event;
  }
}
