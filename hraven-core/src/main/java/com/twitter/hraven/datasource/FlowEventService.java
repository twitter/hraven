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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.twitter.hraven.Constants;
import com.twitter.hraven.FlowEvent;
import com.twitter.hraven.FlowEventKey;
import com.twitter.hraven.FlowKey;
import com.twitter.hraven.Framework;

/**
 * Service for reading and writing rows in the
 * {@link Constants#FLOW_EVENT_TABLE} table
 */
public class FlowEventService {
  private static Log LOG = LogFactory.getLog(FlowEventService.class);

  public static final String TIMESTAMP_COL = "ts";
  public static final byte[] TIMESTAMP_COL_BYTES = Bytes.toBytes(TIMESTAMP_COL);

  public static final String TYPE_COL = "type";
  public static final byte[] TYPE_COL_BYTES = Bytes.toBytes(TYPE_COL);

  public static final String DATA_COL = "data";
  public static final byte[] DATA_COL_BYTES = Bytes.toBytes(DATA_COL);

  private final Connection hbaseConnection;
  private FlowKeyConverter flowKeyConverter = new FlowKeyConverter();
  private FlowEventKeyConverter keyConverter = new FlowEventKeyConverter();

  /**
   * Used to add events to the flow event table.
   *
   * @param hbaseConnection Used to connect to get references to HBase tables.
   * @throws IOException
   */
  public FlowEventService(Connection hbaseConnection) throws IOException {
    this.hbaseConnection = hbaseConnection;
  }

  /**
   * close open connections to tables and the hbase cluster.
   * @throws IOException
   */
  public void close() throws IOException {
  }

  /**
   * Stores a single flow event row
   * @param event
   * @throws IOException
   */
  public void addEvent(FlowEvent event) throws IOException {
    Put p = createPutForEvent(event);
    Table eventTable = null;
    try {
      eventTable = hbaseConnection
          .getTable(TableName.valueOf(Constants.FLOW_EVENT_TABLE));
      eventTable.put(p);
    } finally {
      if (eventTable != null) {
        eventTable.close();
      }
    }
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
    Table eventTable = null;
    try {
      eventTable = hbaseConnection
          .getTable(TableName.valueOf(Constants.FLOW_EVENT_TABLE));
      eventTable.put(puts);
    } finally {
      if (eventTable != null) {
        eventTable.close();
      }
    }
  }

  /**
   * Retrieves all the event rows matching a single
   * {@link com.twitter.hraven.Flow}.
   * @param flowKey
   * @return
   */
  public List<FlowEvent> getFlowEvents(FlowKey flowKey) throws IOException {
    byte[] startKey =
        Bytes.add(flowKeyConverter.toBytes(flowKey), Constants.SEP_BYTES);
    Scan scan = new Scan(startKey);
    scan.setFilter(new WhileMatchFilter(new PrefixFilter(startKey)));

    List<FlowEvent> results = new ArrayList<FlowEvent>();
    ResultScanner scanner = null;
    Table eventTable = null;
    try {
      eventTable = hbaseConnection
          .getTable(TableName.valueOf(Constants.FLOW_EVENT_TABLE));
      scanner = eventTable.getScanner(scan);
      for (Result r : scanner) {
        FlowEvent event = createEventFromResult(r);
        if (event != null) {
          results.add(event);
        }
      }
    } finally {
      try {
        if (scanner != null) {
          scanner.close();
        }
      } finally {
        if (eventTable != null) {
          eventTable.close();
        }
      }
    }
    return results;
  }

  /**
   * Retrieves all events added after the given event key (with sequence numbers
   * greater than the given key). If no new events are found returns an empty
   * list.
   * @param lastSeen
   * @return
   */
  public List<FlowEvent> getFlowEventsSince(FlowEventKey lastSeen)
      throws IOException {
    // rows must match the FlowKey portion + SEP
    byte[] keyPrefix =
        Bytes.add(flowKeyConverter.toBytes(lastSeen), Constants.SEP_BYTES);
    // start at the next following sequence number
    FlowEventKey nextEvent = new FlowEventKey(lastSeen.getCluster(),
        lastSeen.getUserName(), lastSeen.getAppId(), lastSeen.getRunId(),
        lastSeen.getSequence() + 1);
    byte[] startKey = keyConverter.toBytes(nextEvent);
    Scan scan = new Scan(startKey);
    scan.setFilter(new WhileMatchFilter(new PrefixFilter(keyPrefix)));

    List<FlowEvent> results = new ArrayList<FlowEvent>();
    ResultScanner scanner = null;
    Table eventTable = null;
    try {
      eventTable = hbaseConnection
          .getTable(TableName.valueOf(Constants.FLOW_EVENT_TABLE));
      scanner = eventTable.getScanner(scan);
      for (Result r : scanner) {
        FlowEvent event = createEventFromResult(r);
        if (event != null) {
          results.add(event);
        }
      }
    } finally {
      try {
        if (scanner != null) {
          scanner.close();
        }
      } finally {
        if (eventTable != null) {
          eventTable.close();
        }
      }
    }
    return results;
  }

  protected Put createPutForEvent(FlowEvent event) {
    Put p = new Put(keyConverter.toBytes(event.getFlowEventKey()));
    p.addColumn(Constants.INFO_FAM_BYTES, TIMESTAMP_COL_BYTES,
        Bytes.toBytes(event.getTimestamp()));
    if (event.getType() != null) {
      p.addColumn(Constants.INFO_FAM_BYTES, TYPE_COL_BYTES,
          Bytes.toBytes(event.getType()));
    }
    if (event.getFramework() != null) {
      p.addColumn(Constants.INFO_FAM_BYTES, Constants.FRAMEWORK_COLUMN_BYTES,
          Bytes.toBytes(event.getFramework().getCode()));
    }
    if (event.getEventDataJSON() != null) {
      p.addColumn(Constants.INFO_FAM_BYTES, DATA_COL_BYTES,
          Bytes.toBytes(event.getEventDataJSON()));
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
      event.setType(Bytes
          .toString(result.getValue(Constants.INFO_FAM_BYTES, TYPE_COL_BYTES)));
    }
    if (result.containsColumn(Constants.INFO_FAM_BYTES,
        Constants.FRAMEWORK_COLUMN_BYTES)) {
      String code = Bytes.toString(result.getValue(Constants.INFO_FAM_BYTES,
          Constants.FRAMEWORK_COLUMN_BYTES));
      event.setFramework(Framework.get(code));
    }
    if (result.containsColumn(Constants.INFO_FAM_BYTES, DATA_COL_BYTES)) {
      event.setEventDataJSON(Bytes
          .toString(result.getValue(Constants.INFO_FAM_BYTES, DATA_COL_BYTES)));
    }
    return event;
  }
}
