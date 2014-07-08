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
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobHistoryCopy;
import org.apache.hadoop.mapred.JobHistoryCopy.Listener;
import org.apache.hadoop.mapred.JobHistoryCopy.RecordTypes;

import com.google.common.base.Function;
import com.twitter.hraven.Constants;
import com.twitter.hraven.HravenRecord;
import com.twitter.hraven.JobHistoryRecordCollection;
import com.twitter.hraven.JobHistoryKeys;
import com.twitter.hraven.JobHistoryRawRecord;
import com.twitter.hraven.JobHistoryRecord;
import com.twitter.hraven.JobHistoryTaskRecord;
import com.twitter.hraven.RecordDataKey;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.RecordCategory;
import com.twitter.hraven.TaskKey;
import com.twitter.hraven.datasource.JobKeyConverter;
import com.twitter.hraven.datasource.TaskKeyConverter;
import com.twitter.hraven.etl.ImportException;


public class JobHistoryListener implements Listener {

  private static Log LOG = LogFactory.getLog(JobHistoryListener.class);

  private JobKey jobKey;
  private String jobId;
  /** Job ID, minus the leading "job_" */
  private String jobNumber = "";

  /** explicitly initializing map millis and
   * reduce millis in case it's not found
   */
  private long mapSlotMillis = 0L;
  private long reduceSlotMillis = 0L;
  private Collection jobRecords;
  private Collection taskRecords;

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
    this.jobRecords = new JobHistoryRecordCollection(jobKey);
    this.taskRecords = new ArrayList<JobHistoryTaskRecord>();
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
  }

  private interface RecordGenerator {
	  public HravenRecord getRecord(RecordDataKey key, Object value, boolean isNumeric);
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
    
    for (Map.Entry<JobHistoryKeys, String> e : values.entrySet()) {
      addRecords(jobRecords, e.getKey(), e.getValue(), new RecordGenerator() {
        @Override
        public JobHistoryRecord getRecord(RecordDataKey key, Object value, boolean isNumeric) {
          return new JobHistoryRecord(isNumeric ? RecordCategory.HISTORY_COUNTER : RecordCategory.HISTORY_META, jobKey, key, value);
        }
      });
    }
  }

  /**
   * sets the hadoop version put in the list of job puts
   * @param pVersion
   * @throws IllegalArgumentException if put is null
   */
  public void includeHadoopVersionRecord(JobHistoryRecord pVersion) {
	  // set the hadoop version for this record
	  if (pVersion != null) {
		  this.jobRecords.add(pVersion);
	  } else {
		  String msg = "Hadoop Version put cannot be null";
		  LOG.error(msg);
		  throw new IllegalArgumentException(msg);
	  }
  }

  private void handleTask(Map<JobHistoryKeys, String> values) {
    final TaskKey taskKey = getTaskKey("task_", this.jobNumber, values.get(JobHistoryKeys.TASKID));

    this.taskRecords.add(new JobHistoryTaskRecord(RecordCategory.HISTORY_TASK_META, taskKey,
        new RecordDataKey(Constants.RECORD_TYPE_COL), RecordTypes.Task.toString()));

    for (Map.Entry<JobHistoryKeys, String> e : values.entrySet()) {
      addRecords(taskRecords, e.getKey(), e.getValue(), new RecordGenerator() {
        @Override
        public JobHistoryTaskRecord getRecord(RecordDataKey key, Object value, boolean isNumeric) {
          return new JobHistoryTaskRecord(isNumeric ? RecordCategory.HISTORY_TASK_COUNTER
              : RecordCategory.HISTORY_TASK_META, taskKey, key, value);
        }
      });
    }
  }

  private void handleMapAttempt(Map<JobHistoryKeys, String> values) {
    final TaskKey taskKey =
        getTaskKey("attempt_", this.jobNumber, values.get(JobHistoryKeys.TASK_ATTEMPT_ID));

    this.taskRecords.add(new JobHistoryTaskRecord(RecordCategory.HISTORY_TASK_META, taskKey,
        new RecordDataKey(Constants.RECORD_TYPE_COL), RecordTypes.MapAttempt.toString()));

    for (Map.Entry<JobHistoryKeys, String> e : values.entrySet()) {
      addRecords(taskRecords, e.getKey(), e.getValue(), new RecordGenerator() {
        @Override
        public JobHistoryTaskRecord getRecord(RecordDataKey key, Object value, boolean isNumeric) {
          return new JobHistoryTaskRecord(isNumeric ? RecordCategory.HISTORY_TASK_COUNTER
              : RecordCategory.HISTORY_TASK_META, taskKey, key, value);
        }
      });
    }
  }

  private void handleReduceAttempt(Map<JobHistoryKeys, String> values) {
    final TaskKey taskKey =
        getTaskKey("attempt_", this.jobNumber, values.get(JobHistoryKeys.TASK_ATTEMPT_ID));

    this.taskRecords.add(new JobHistoryTaskRecord(RecordCategory.HISTORY_TASK_META, taskKey,
        new RecordDataKey(Constants.RECORD_TYPE_COL), RecordTypes.ReduceAttempt.toString()));

    for (Map.Entry<JobHistoryKeys, String> e : values.entrySet()) {
      addRecords(taskRecords, e.getKey(), e.getValue(), new RecordGenerator() {
        @Override
        public JobHistoryTaskRecord getRecord(RecordDataKey key, Object value, boolean isNumeric) {
          return new JobHistoryTaskRecord(isNumeric ? RecordCategory.HISTORY_TASK_COUNTER
              : RecordCategory.HISTORY_TASK_META, taskKey, key, value);
        }
      });
    }
  }

  private void addRecords(Collection recordCollection, JobHistoryKeys key, String value,
      RecordGenerator generator) {
    if (key == JobHistoryKeys.COUNTERS || key == JobHistoryKeys.MAP_COUNTERS
        || key == JobHistoryKeys.REDUCE_COUNTERS) {
      try {
        Counters counters = Counters.fromEscapedCompactString(value);

        for (Counters.Group group : counters) {
          for (Counters.Counter counter : group) {
            RecordDataKey dataKey = new RecordDataKey(key.toString());
            dataKey.add(group.getName());
            String counterName = counter.getName();
            long counterValue = counter.getValue();
            dataKey.add(counterName);
            recordCollection.add(generator.getRecord(dataKey, counterValue, true));

            // get the map and reduce slot millis for megabytemillis
            // calculations
            if (Constants.SLOTS_MILLIS_MAPS.equals(counterName)) {
              this.mapSlotMillis = counterValue;
            }
            if (Constants.SLOTS_MILLIS_REDUCES.equals(counterName)) {
              this.reduceSlotMillis = counterValue;
            }
          }
        }
      } catch (ParseException pe) {
        LOG.error("Counters could not be parsed from string'" + value + "'", pe);
      }
    } else {
      @SuppressWarnings("rawtypes")
      Class clazz = JobHistoryKeys.KEY_TYPES.get(key);
      Object valueObj = value;
      boolean isNumeric = false;
      if (Integer.class.equals(clazz)) {
        isNumeric = true;
        try {
          valueObj = (value != null && value.trim().length() > 0) ? Integer.parseInt(value) : 0;
        } catch (NumberFormatException nfe) {
          valueObj = 0;
        }
      } else if (Long.class.equals(clazz)) {
        isNumeric = true;
        try {
          valueObj = (value != null && value.trim().length() > 0) ? Long.parseLong(value) : 0;
        } catch (NumberFormatException nfe) {
          valueObj = 0;
        }
      }
      
      recordCollection.add(generator.getRecord(new RecordDataKey(key.toString()),
        valueObj, isNumeric));
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
  public TaskKey getTaskKey(String prefix, String jobNumber, String fullId) {
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

    return new TaskKey(this.jobKey, taskComponent);
  }

  /**
   * getter for jobKeyBytes
   * @return the byte array of jobKeyBytes
   */
  public JobKey getJobKey() {
	return this.jobKey;
  }

  /**
   * Return the generated list of put assembled when
   *         {@link JobHistoryCopy#parseHistoryFromFS(String, Listener, org.apache.hadoop.fs.FileSystem)}
   *         is called with this listener.
   * @return a non-null (possibly empty) list of jobPuts
   */
  public Collection getJobRecords() {
    return this.jobRecords;
  }

  public Collection getTaskRecords() {
    return this.taskRecords;
  }

  public Long getMapSlotMillis() {
    return this.mapSlotMillis;
  }

  public Long getReduceSlotMillis() {
    return this.reduceSlotMillis;
  }
  
}
