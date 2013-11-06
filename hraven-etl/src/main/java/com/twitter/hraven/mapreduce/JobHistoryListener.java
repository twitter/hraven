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
package com.twitter.hraven.mapreduce;

import java.io.IOException;
import java.text.ParseException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobHistoryCopy;
import org.apache.hadoop.mapred.JobHistoryCopy.Listener;
import org.apache.hadoop.mapred.JobHistoryCopy.RecordTypes;

import com.twitter.hraven.Constants;
import com.twitter.hraven.JobHistoryKeys;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.TaskKey;
import com.twitter.hraven.datasource.JobKeyConverter;
import com.twitter.hraven.datasource.TaskKeyConverter;
import com.twitter.hraven.etl.ImportException;
import com.twitter.hraven.etl.JobHistoryFileParserFactory;


public class JobHistoryListener implements Listener {

  private static Log LOG = LogFactory.getLog(JobHistoryListener.class);

  private JobKey jobKey;
  private String jobId;
  /** Job ID, minus the leading "job_" */
  private String jobNumber = "";
  private final byte[] jobKeyBytes;
  private List<Put> jobPuts = new LinkedList<Put>();
  private List<Put> taskPuts = new LinkedList<Put>();
  private JobKeyConverter jobKeyConv = new JobKeyConverter();
  private TaskKeyConverter taskKeyConv = new TaskKeyConverter();

  /**
   * Constructor for listener to be used to read in a Job History File. While
   * reading a list of HBase puts is assembled.
   * 
   * @param jobKey jobKey of the job to be persisted
   */
  public JobHistoryListener(JobKey jobKey) {
    if (null == jobKey) {
      String msg = "JobKey cannot be null";
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }
    this.jobKey = jobKey;
    this.jobKeyBytes = jobKeyConv.toBytes(jobKey);
    setJobId(jobKey.getJobId().getJobIdString());
  }

  @Override
  public void handle(RecordTypes recType, Map<JobHistoryKeys, String> values)
      throws IOException {
    switch (recType) {
    case Job:
      handleJob(values);
      break;
    case Task:
      handleTask(values);
      break;
    case MapAttempt:
      handleMapAttempt(values);
      break;
    case ReduceAttempt:
      handleReduceAttempt(values);
      break;
    default:
      // skip other types
      ;
    }
    //System.out.println("Reading: " + recType.toString());
  }

  private void handleJob(Map<JobHistoryKeys, String> values) {
    String id = values.get(JobHistoryKeys.JOBID);

    if (jobId == null) {
      setJobId(id);
    } else if (!jobId.equals(id)) {
      String msg = "Current job ID '" + id
          + "' does not match previously stored value '" + jobId + "'";
      LOG.error(msg);
      throw new ImportException(msg);
    }
    // add job ID to values to put
    Put p = new Put(this.jobKeyBytes);
    for (Map.Entry<JobHistoryKeys, String> e : values.entrySet()) {
      addKeyValues(p, Constants.INFO_FAM_BYTES, e.getKey(), e.getValue());
    }
    this.jobPuts.add(p);

    // set the hadoop version for this record
    Put versionPut = JobHistoryFileParserFactory.getHadoopVersionPut(
    		JobHistoryFileParserFactory.getHistoryFileVersion1(), this.jobKeyBytes);
    this.jobPuts.add(versionPut);

  }

  private void handleTask(Map<JobHistoryKeys, String> values) {
    byte[] taskIdKeyBytes = getTaskKey("task_", this.jobNumber, values.get(JobHistoryKeys.TASKID));
    Put p = new Put(taskIdKeyBytes);

    p.add(Constants.INFO_FAM_BYTES, Constants.RECORD_TYPE_COL_BYTES,
        Bytes.toBytes(RecordTypes.Task.toString()));
    for (Map.Entry<JobHistoryKeys,String> e : values.entrySet()) {
      addKeyValues(p, Constants.INFO_FAM_BYTES, e.getKey(), e.getValue());
    }
    this.taskPuts.add(p);
  }

  private void handleMapAttempt(Map<JobHistoryKeys, String> values) {
    byte[] taskIdKeyBytes = getTaskKey("attempt_", this.jobNumber, values.get(JobHistoryKeys.TASK_ATTEMPT_ID));
    Put p = new Put(taskIdKeyBytes);

    p.add(Constants.INFO_FAM_BYTES, Constants.RECORD_TYPE_COL_BYTES,
        Bytes.toBytes(RecordTypes.MapAttempt.toString()));
    for (Map.Entry<JobHistoryKeys,String> e : values.entrySet()) {
      addKeyValues(p, Constants.INFO_FAM_BYTES, e.getKey(), e.getValue());
    }

    this.taskPuts.add(p);
  }

  private void handleReduceAttempt(Map<JobHistoryKeys, String> values) {
    byte[] taskIdKeyBytes = getTaskKey("attempt_", this.jobNumber, values.get(JobHistoryKeys.TASK_ATTEMPT_ID));
    Put p = new Put(taskIdKeyBytes);

    p.add(Constants.INFO_FAM_BYTES, Constants.RECORD_TYPE_COL_BYTES,
        Bytes.toBytes(RecordTypes.ReduceAttempt.toString()));
    for (Map.Entry<JobHistoryKeys,String> e : values.entrySet()) {
      addKeyValues(p, Constants.INFO_FAM_BYTES, e.getKey(), e.getValue());
    }

    this.taskPuts.add(p);
  }

  private void addKeyValues(Put p, byte[] family, JobHistoryKeys key, String value) {
    if (key == JobHistoryKeys.COUNTERS || key == JobHistoryKeys.MAP_COUNTERS
        || key == JobHistoryKeys.REDUCE_COUNTERS) {
      try {
        Counters counters = Counters.fromEscapedCompactString(value);
        /*
         * Name counter columns as:
         *     g!groupname!countername
         */
        byte[] counterPrefix = null;
        if (key == JobHistoryKeys.COUNTERS) {
            counterPrefix = Bytes.add(Constants.COUNTER_COLUMN_PREFIX_BYTES,
                Constants.SEP_BYTES);
        } else if (key == JobHistoryKeys.MAP_COUNTERS) {
          counterPrefix = Bytes.add(Constants.MAP_COUNTER_COLUMN_PREFIX_BYTES,
              Constants.SEP_BYTES);
        } else if (key == JobHistoryKeys.REDUCE_COUNTERS) {
          counterPrefix = Bytes.add(Constants.REDUCE_COUNTER_COLUMN_PREFIX_BYTES,
              Constants.SEP_BYTES);
        } else {
          throw new IllegalArgumentException("Unknown counter type "+key.toString());
        }

        for (Counters.Group group : counters) {
          byte[] groupPrefix = Bytes.add(
              counterPrefix, Bytes.toBytes(group.getName()), Constants.SEP_BYTES);
          for (Counters.Counter counter : group) {
            byte[] qualifier = Bytes.add(groupPrefix, Bytes.toBytes(counter.getName()));
            p.add(family, qualifier, Bytes.toBytes(counter.getValue()));
          }
        }
      } catch (ParseException pe) {
        LOG.error("Counters could not be parsed from string'"+value+"'", pe);
      }
    } else {
      @SuppressWarnings("rawtypes")
      Class clazz = JobHistoryKeys.KEY_TYPES.get(key);
      byte[] valueBytes = null;
      if (Integer.class.equals(clazz)) {
        try {
          valueBytes = (value != null && value.trim().length() > 0) ?
              Bytes.toBytes(Integer.parseInt(value)) : Constants.ZERO_INT_BYTES;
        } catch (NumberFormatException nfe) {
          // us a default value
          valueBytes = Constants.ZERO_INT_BYTES;
        }
      } else if (Long.class.equals(clazz)) {
        try {
          valueBytes = (value != null && value.trim().length() > 0) ?
              Bytes.toBytes(Long.parseLong(value)) : Constants.ZERO_LONG_BYTES;
        } catch (NumberFormatException nfe) {
          // us a default value
          valueBytes = Constants.ZERO_LONG_BYTES;
        }
      } else {
        // keep the string representation by default
        valueBytes = Bytes.toBytes(value);
      }
      byte[] qualifier = Bytes.toBytes(key.toString().toLowerCase());
      p.add(family, qualifier, valueBytes);
    }
  }

  /**
   * Sets the job ID and strips out the job number (job ID minus the "job_" prefix).
   * @param id
   */
  private void setJobId(String id) {
    this.jobId = id;
    if (id != null && id.startsWith("job_") && id.length() > 4) {
      this.jobNumber = id.substring(4);
    }
  }

  /**
   * Returns the Task ID or Task Attempt ID, stripped of the leading job ID,
   * appended to the job row key.
   */
  public byte[] getTaskKey(String prefix, String jobNumber, String fullId) {
    String taskComponent = fullId;
    if (fullId == null) {
      taskComponent = "";
    } else {
      String expectedPrefix = prefix + jobNumber + "_";
      if (fullId.startsWith(expectedPrefix)
          && fullId.length() > expectedPrefix.length()) {
        taskComponent = fullId.substring(expectedPrefix.length());
      }
    }

    return taskKeyConv.toBytes(new TaskKey(this.jobKey, taskComponent));
  }

  /**
   * Return the generated list of put assembled when
   *         {@link JobHistoryCopy#parseHistoryFromFS(String, Listener, org.apache.hadoop.fs.FileSystem)}
   *         is called with this listener.
   * @return a non-null (possibly empty) list of jobPuts
   */
  public List<Put> getJobPuts() {
    return this.jobPuts;
  }

  public List<Put> getTaskPuts() {
    return this.taskPuts;
  }
}
