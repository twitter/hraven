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
package com.twitter.hraven.etl;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * Updates a processRecord to the given status when called.
 */
public class ProcessRecordUpdater implements Callable<Boolean> {

  /**
   * Which is to be updated.
   */
  private final ProcessRecord processRecord;

  /**
   * The new state to set the record to using the service.
   */
  private final ProcessState newState;

  /**
   * Used to connect to HBase.
   */
  private final Configuration hbaseConf;

  /**
   * @param hBaseconf used to connect to HBase
   * @throws IOException
   */
  public ProcessRecordUpdater(Configuration hBaseconf,
      ProcessRecord processRecord, ProcessState newState) throws IOException {
    this.hbaseConf = hBaseconf;
    this.processRecord = processRecord;
    this.newState = newState;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.util.concurrent.Callable#call()
   */
  @Override
  public Boolean call() throws Exception {

    ProcessRecord updatedRecord = null;
    Connection hbaseConnection = null;
    try {
      hbaseConnection = ConnectionFactory.createConnection(hbaseConf);
      // Connect only when needed.
      ProcessRecordService processRecordService =
          new ProcessRecordService(hbaseConf, hbaseConnection);

      updatedRecord =
          processRecordService.setProcessState(processRecord, newState);
    } finally {
      if (hbaseConnection != null) {
        hbaseConnection.close();
      }
    }

    if ((updatedRecord != null)
        && (updatedRecord.getProcessState() == newState)) {
      return true;
    }
    return false;
  }

}
