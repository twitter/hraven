/*
 * Copyright 2013 Twitter, Inc. Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may obtain a copy of the License
 * at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
 * writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package com.twitter.hraven.etl;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobHistoryCopy.RecordTypes;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.twitter.hraven.Constants;
import com.twitter.hraven.JobHistoryKeys;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.TaskKey;
import com.twitter.hraven.util.ByteArrayWrapper;
import com.twitter.hraven.datasource.JobKeyConverter;
import com.twitter.hraven.datasource.ProcessingException;
import com.twitter.hraven.datasource.TaskKeyConverter;

/**
 * Deal with JobHistory file parsing for job history files which are generated after MAPREDUCE-1016
 * (hadoop 1.x (0.21 and later) and hadoop 2.x)
 */
public class JobHistoryFileParserHadoop2 implements JobHistoryFileParser {

  private JobKey jobKey;
  /** Job ID, minus the leading "job_" */
  private String jobNumber = "";
  private byte[] jobKeyBytes;
  private List<Put> jobPuts = new LinkedList<Put>();
  private List<Put> taskPuts = new LinkedList<Put>();
  private JobKeyConverter jobKeyConv = new JobKeyConverter();
  private TaskKeyConverter taskKeyConv = new TaskKeyConverter();

  private static final String AM_ATTEMPT_PREFIX = "AM_";
  private static final String TASK_PREFIX = "task_";
  private static final String TASK_ATTEMPT_PREFIX = "taskAttempt_";

  private static final Log LOG = LogFactory.getLog(JobHistoryFileParserHadoop2.class);

  private Schema schema;
  private Decoder decoder;
  private DatumReader<GenericRecord> reader;

  private static final String TYPE = "type";
  private static final String EVENT = "event";
  private static final String NAME = "name";
  private static final String FIELDS = "fields";
  private static final String COUNTS = "counts";
  private static final String GROUPS = "groups";
  private static final String VALUE = "value";
  private static final String TASKID = "taskid";
  private static final String APPLICATION_ATTEMPTID = "applicationAttemptId";
  private static final String ATTEMPTID = "attemptId";

  private static final String TYPE_INT = "int";
  private static final String TYPE_BOOLEAN = "boolean";
  private static final String TYPE_LONG = "long";
  private static final String TYPE_STRING = "String";
  /** only acls in the job history file seem to be of this type: map of strings */
  private static final String TYPE_MAP_STRINGS = "{\"type\":\"map\",\"values\":\"string\"}";
  /**
   * vMemKbytes, clockSplit, physMemKbytes, cpuUsages are arrays of ints See MAPREDUCE-5432
   */
  private static final String TYPE_ARRAY_INTS = "{\"type\":\"array\",\"items\":\"int\"}";
  /** this is part of {@link org.apache.hadoop.mapreduce.jobhistory.TaskFailedEvent.java} */
  private static final String NULL_STRING = "[\"null\",\"string\"]";

  public static enum Hadoop2RecordType {
    JobFinished, JobInfoChange, JobInited, AMStarted, JobPriorityChange, JobStatusChanged,
    JobSubmitted, JobUnsuccessfulCompletion, MapAttemptFinished, ReduceAttemptFinished,
    TaskAttemptFinished, TaskAttemptStarted, TaskAttemptUnsuccessfulCompletion, TaskFailed,
    TaskFinished, TaskStarted, TaskUpdated
  }

  public static enum CounterTypes {
    counters, mapCounters, reduceCounters, totalCounters
  }

  private static final Set<String> counterNames = new HashSet<String>();
  private static final Map<String, Hadoop2RecordType> eventRecordNames =
      new HashMap<String, Hadoop2RecordType>();
  private Map<Hadoop2RecordType, Map<String, String>> fieldTypes =
      new HashMap<Hadoop2RecordType, Map<String, String>>();
  private static Map<String, String> oldToNewMapping = new HashMap<String, String>();

  /**
   * constructor populates the counterNames hash set and the fieldTypes map
   */
  JobHistoryFileParserHadoop2() {
    /**
     * populating this map since the symbols and key to get the types of fields the symbol denotes
     * the record in the file (like JOB_SUBMITTED) and it's value in the map (like JobSubmitted)
     * helps us get the types of fields that that record contains (this type information is present
     * in the schema)
     */
    eventRecordNames.put("JOB_SUBMITTED", Hadoop2RecordType.JobSubmitted);
    eventRecordNames.put("JOB_INITED", Hadoop2RecordType.JobInited);
    eventRecordNames.put("AM_STARTED", Hadoop2RecordType.AMStarted);
    eventRecordNames.put("CLEANUP_ATTEMPT_KILLED",
      Hadoop2RecordType.TaskAttemptUnsuccessfulCompletion);
    eventRecordNames.put("CLEANUP_ATTEMPT_FAILED",
      Hadoop2RecordType.TaskAttemptUnsuccessfulCompletion);
    eventRecordNames.put("CLEANUP_ATTEMPT_FINISHED", Hadoop2RecordType.TaskAttemptFinished);
    eventRecordNames.put("CLEANUP_ATTEMPT_STARTED", Hadoop2RecordType.TaskAttemptStarted);
    eventRecordNames.put("SETUP_ATTEMPT_KILLED",
      Hadoop2RecordType.TaskAttemptUnsuccessfulCompletion);
    eventRecordNames.put("SETUP_ATTEMPT_FAILED",
      Hadoop2RecordType.TaskAttemptUnsuccessfulCompletion);
    eventRecordNames.put("SETUP_ATTEMPT_STARTED", Hadoop2RecordType.TaskAttemptStarted);
    eventRecordNames.put("REDUCE_ATTEMPT_KILLED",
      Hadoop2RecordType.TaskAttemptUnsuccessfulCompletion);
    eventRecordNames.put("REDUCE_ATTEMPT_FAILED",
      Hadoop2RecordType.TaskAttemptUnsuccessfulCompletion);
    eventRecordNames.put("REDUCE_ATTEMPT_FINISHED", Hadoop2RecordType.ReduceAttemptFinished);
    eventRecordNames.put("REDUCE_ATTEMPT_STARTED", Hadoop2RecordType.TaskAttemptStarted);
    eventRecordNames.put("MAP_ATTEMPT_KILLED", Hadoop2RecordType.TaskAttemptUnsuccessfulCompletion);
    eventRecordNames.put("MAP_ATTEMPT_FAILED", Hadoop2RecordType.TaskAttemptUnsuccessfulCompletion);
    eventRecordNames.put("MAP_ATTEMPT_FINISHED", Hadoop2RecordType.MapAttemptFinished);
    eventRecordNames.put("MAP_ATTEMPT_STARTED", Hadoop2RecordType.TaskAttemptStarted);
    eventRecordNames.put("TASK_UPDATED", Hadoop2RecordType.TaskUpdated);
    eventRecordNames.put("TASK_FAILED", Hadoop2RecordType.TaskFailed);
    eventRecordNames.put("TASK_FINISHED", Hadoop2RecordType.TaskFinished);
    eventRecordNames.put("TASK_STARTED", Hadoop2RecordType.TaskStarted);
    eventRecordNames.put("JOB_INFO_CHANGED", Hadoop2RecordType.JobInfoChange);
    eventRecordNames.put("JOB_KILLED", Hadoop2RecordType.JobUnsuccessfulCompletion);
    eventRecordNames.put("JOB_STATUS_CHANGED", Hadoop2RecordType.JobStatusChanged);
    eventRecordNames.put("JOB_FAILED", Hadoop2RecordType.JobUnsuccessfulCompletion);
    eventRecordNames.put("JOB_PRIORITY_CHANGED", Hadoop2RecordType.JobPriorityChange);
    eventRecordNames.put("JOB_FINISHED", Hadoop2RecordType.JobFinished);

    /**
     * populate the hash set for counter names
     */
    for (CounterTypes ct : CounterTypes.values()) {
      counterNames.add(ct.toString());
    }

    oldToNewMapping.put("startTime", JobHistoryKeys.START_TIME.toString());
    oldToNewMapping.put("finishTime", JobHistoryKeys.FINISH_TIME.toString());
    oldToNewMapping.put("submitTime", JobHistoryKeys.SUBMIT_TIME.toString());
    oldToNewMapping.put("launchTime", JobHistoryKeys.LAUNCH_TIME.toString());
    oldToNewMapping.put("totalMaps", JobHistoryKeys.TOTAL_MAPS.toString());
    oldToNewMapping.put("totalReduces", JobHistoryKeys.TOTAL_REDUCES.toString());
    oldToNewMapping.put("failedMaps", JobHistoryKeys.FAILED_MAPS.toString());
    oldToNewMapping.put("failedReduces", JobHistoryKeys.FAILED_REDUCES.toString());
    oldToNewMapping.put("finishedMaps", JobHistoryKeys.FINISHED_MAPS.toString());
    oldToNewMapping.put("finishedReduces", JobHistoryKeys.FINISHED_REDUCES.toString());
    oldToNewMapping.put("jobStatus", JobHistoryKeys.JOB_STATUS.toString());
    oldToNewMapping.put("taskType", JobHistoryKeys.TASK_TYPE.toString());
    oldToNewMapping.put("jobConfPath", JobHistoryKeys.JOBCONF.toString());
    oldToNewMapping.put("taskStatus", JobHistoryKeys.TASK_STATUS.toString());
    oldToNewMapping.put("shuffleFinishTime", JobHistoryKeys.SHUFFLE_FINISHED.toString());
    oldToNewMapping.put("sortFinishTime", JobHistoryKeys.SORT_FINISHED.toString());
    oldToNewMapping.put("splitLocations", JobHistoryKeys.SPLITS.toString());
    oldToNewMapping.put("httpPort", JobHistoryKeys.HTTP_PORT.toString());
    oldToNewMapping.put("trackerName", JobHistoryKeys.TRACKER_NAME.toString());
    oldToNewMapping.put("state", JobHistoryKeys.STATE_STRING.toString());
    oldToNewMapping.put("jobQueueName", JobHistoryKeys.JOB_QUEUE.toString());

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void parse(String historyFileContents, JobKey jobKey) throws ProcessingException {

    this.jobKey = jobKey;
    this.jobKeyBytes = jobKeyConv.toBytes(jobKey);
    setJobId(jobKey.getJobId().getJobIdString());

    try {
      FSDataInputStream in =
          new FSDataInputStream(new ByteArrayWrapper(historyFileContents.getBytes()));

      /** first line is the version, ignore it */
      String versionIgnore = in.readLine();

      /** second line in file is the schema */
      this.schema = schema.parse(in.readLine());

      /** now figure out the schema */
      understandSchema(schema.toString());

      /** now read the rest of the file */
      this.reader = new GenericDatumReader<GenericRecord>(schema);
      this.decoder = DecoderFactory.get().jsonDecoder(schema, in);

      GenericRecord record = null;
      Hadoop2RecordType recType = null;
      try {
        while ((record = reader.read(null, decoder)) != null) {
          if (record.get(TYPE) != null) {
            recType = eventRecordNames.get(record.get(TYPE).toString());
          } else {
            throw new ProcessingException("expected one of "
                + Arrays.asList(Hadoop2RecordType.values())
                + " \n but not found, cannot process this record! " + jobKey);
          }
          if (recType == null) {
            throw new ProcessingException("new record type has surfaced: "
                + record.get(TYPE).toString() + " cannot process this record! " + jobKey);
          }
          // GenericRecord's get returns an Object
          Object eDetails = record.get(EVENT);

          // confirm that we got an "event" object
          if (eDetails != null) {
            JSONObject eventDetails = new JSONObject(eDetails.toString());
            processRecords(recType, eventDetails);
          } else {
            throw new ProcessingException("expected event details but not found "
                + record.get(TYPE).toString() + " cannot process this record! " + jobKey);
          }
        }
      } catch (EOFException eof) {
        // not an error, simply end of file
        LOG.info("Done parsing file, reached eof for " + jobKey);
      }
    } catch (IOException ioe) {
      throw new ProcessingException(" Unable to parse history file in function parse, "
          + "cannot process this record!" + jobKey + " error: ", ioe);
    } catch (JSONException jse) {
      throw new ProcessingException(" Unable to parse history file in function parse, "
          + "cannot process this record! " + jobKey + " error: ", jse);
    }

    LOG.info("For " + this.jobKey + " #jobPuts " + jobPuts.size() + " #taskPuts: "
        + taskPuts.size());
    printAllPuts();
  }

  /**
   * understand the schema so that we can parse the rest of the file
   * @throws JSONException
   */
  private void understandSchema(String schema) throws JSONException {

    JSONObject j1 = new JSONObject(schema);
    JSONArray fields = j1.getJSONArray(FIELDS);

    for (int k = 0; k < fields.length(); k++) {
      JSONObject allEvents = new JSONObject(fields.get(k).toString());
      Object name = allEvents.get(NAME);
      if (name != null) {
        if (name.toString().equalsIgnoreCase(EVENT)) {
          JSONArray allTypeDetails = allEvents.getJSONArray(TYPE);
          for (int i = 0; i < allTypeDetails.length(); i++) {
            JSONObject actual = (JSONObject) allTypeDetails.get(i);
            JSONArray types = actual.getJSONArray(FIELDS);
            Map<String, String> typeDetails = new HashMap<String, String>();
            for (int j = 0; j < types.length(); j++) {
              typeDetails.put(types.getJSONObject(j).getString(NAME), types.getJSONObject(j)
                  .getString(TYPE));
            }
            fieldTypes.put(Hadoop2RecordType.valueOf(actual.get(NAME).toString()), typeDetails);
          }
        }
      }
    }
  }

  /**
   * process the counter details example line in .jhist file for counters: { "name":"MAP_COUNTERS",
   * "groups":[ { "name":"org.apache.hadoop.mapreduce.FileSystemCounter",
   * "displayName":"File System Counters", "counts":[ { "name":"HDFS_BYTES_READ",
   * "displayName":"HDFS: Number of bytes read", "value":480 }, { "name":"HDFS_BYTES_WRITTEN",
   * "displayName":"HDFS: Number of bytes written", "value":0 } ] }, {
   * "name":"org.apache.hadoop.mapreduce.TaskCounter", "displayName":"Map-Reduce Framework",
   * "counts":[ { "name":"MAP_INPUT_RECORDS", "displayName":"Map input records", "value":10 }, {
   * "name":"MAP_OUTPUT_RECORDS", "displayName":"Map output records", "value":10 } ] } ] }
   */
  private void processCounters(Put p, JSONObject eventDetails, String key) {

    try {
      JSONObject jsonCounters = eventDetails.getJSONObject(key);
      String counterMetaGroupName = jsonCounters.getString(NAME);
      JSONArray groups = jsonCounters.getJSONArray(GROUPS);
      for (int i = 0; i < groups.length(); i++) {
        JSONObject aCounter = groups.getJSONObject(i);
        JSONArray counts = aCounter.getJSONArray(COUNTS);
        for (int j = 0; j < counts.length(); j++) {
          JSONObject countDetails = counts.getJSONObject(j);
          populatePut(p, Constants.INFO_FAM_BYTES, counterMetaGroupName, aCounter.get(NAME)
              .toString(), countDetails.get(NAME).toString(), countDetails.getLong(VALUE));
        }
      }
    } catch (JSONException e) {
      throw new ProcessingException(" Caught json exception while processing counters ", e);
    }

  }

  /**
   * process the event details as per their data type from schema definition
   * @throws JSONException
   */
  private void
      processAllTypes(Put p, Hadoop2RecordType recType, JSONObject eventDetails, String key)
          throws JSONException {

    if (counterNames.contains(key)) {
      processCounters(p, eventDetails, key);
    } else {
      String type = fieldTypes.get(recType).get(key);
      if (type.equalsIgnoreCase(TYPE_STRING)) {
        String value = eventDetails.getString(key);
        populatePut(p, Constants.INFO_FAM_BYTES, key, value);
      } else if (type.equalsIgnoreCase(TYPE_LONG)) {
        long value = eventDetails.getLong(key);
        populatePut(p, Constants.INFO_FAM_BYTES, key, value);
      } else if (type.equalsIgnoreCase(TYPE_INT)) {
        int value = eventDetails.getInt(key);
        populatePut(p, Constants.INFO_FAM_BYTES, key, value);
      } else if (type.equalsIgnoreCase(TYPE_BOOLEAN)) {
        boolean value = eventDetails.getBoolean(key);
        populatePut(p, Constants.INFO_FAM_BYTES, key, Boolean.toString(value));
      } else if (type.equalsIgnoreCase(TYPE_ARRAY_INTS)) {
        String value = eventDetails.getString(key);
        populatePut(p, Constants.INFO_FAM_BYTES, key, value);
      } else if (type.equalsIgnoreCase(NULL_STRING)) {
        // usually seen in FAILED tasks
        String value = eventDetails.getString(key);
        populatePut(p, Constants.INFO_FAM_BYTES, key, value);
      } else if (type.equalsIgnoreCase(TYPE_MAP_STRINGS)) {
        JSONObject ms = new JSONObject(eventDetails.get(key).toString());
        populatePut(p, Constants.INFO_FAM_BYTES, key, ms.toString());
      } else {
        throw new ProcessingException("Encountered a new type " + type
            + " unable to complete processing " + this.jobKey);
      }
    }
  }

  /**
   * iterate over the event details and prepare puts
   * @throws JSONException
   */
  private void iterateAndPreparePuts(JSONObject eventDetails, Put p, Hadoop2RecordType recType)
      throws JSONException {
    Iterator<?> keys = eventDetails.keys();
    while (keys.hasNext()) {
      String key = (String) keys.next();
      processAllTypes(p, recType, eventDetails, key);
    }
  }

  /**
   * process individual records
   * @throws JSONException
   */
  private void processRecords(Hadoop2RecordType recType, JSONObject eventDetails)
      throws JSONException {

    switch (recType) {
    case JobFinished:
    case JobInfoChange:
    case JobInited:
    case JobPriorityChange:
    case JobStatusChanged:
    case JobSubmitted:
    case JobUnsuccessfulCompletion:
      Put pJob = new Put(this.jobKeyBytes);
      iterateAndPreparePuts(eventDetails, pJob, recType);
      this.jobPuts.add(pJob);
      break;

    case AMStarted:
      byte[] amAttemptIdKeyBytes =
          getAMKey(AM_ATTEMPT_PREFIX, eventDetails.getString(APPLICATION_ATTEMPTID));
      // generate a new put per AM Attempt
      Put pAM = new Put(amAttemptIdKeyBytes);
      pAM.add(Constants.INFO_FAM_BYTES, Constants.RECORD_TYPE_COL_BYTES,
        Bytes.toBytes(RecordTypes.Task.toString()));
      iterateAndPreparePuts(eventDetails, pAM, recType);
      taskPuts.add(pAM);
      break;

    case MapAttemptFinished:
      byte[] taskMAttemptIdKeyBytes =
          getTaskKey(TASK_ATTEMPT_PREFIX, this.jobNumber, eventDetails.getString(ATTEMPTID));
      Put pMTaskAttempt = new Put(taskMAttemptIdKeyBytes);
      pMTaskAttempt.add(Constants.INFO_FAM_BYTES, Constants.RECORD_TYPE_COL_BYTES,
        Bytes.toBytes(RecordTypes.MapAttempt.toString()));
      iterateAndPreparePuts(eventDetails, pMTaskAttempt, recType);
      this.taskPuts.add(pMTaskAttempt);
      break;

    case ReduceAttemptFinished:
      byte[] taskRAttemptIdKeyBytes =
          getTaskKey(TASK_ATTEMPT_PREFIX, this.jobNumber, eventDetails.getString(ATTEMPTID));
      Put pRTaskAttempt = new Put(taskRAttemptIdKeyBytes);
      pRTaskAttempt.add(Constants.INFO_FAM_BYTES, Constants.RECORD_TYPE_COL_BYTES,
        Bytes.toBytes(RecordTypes.ReduceAttempt.toString()));
      iterateAndPreparePuts(eventDetails, pRTaskAttempt, recType);
      this.taskPuts.add(pRTaskAttempt);
      break;

    case TaskAttemptFinished:
    case TaskAttemptStarted:
    case TaskAttemptUnsuccessfulCompletion:
      byte[] taskAttemptIdKeyBytes =
          getTaskKey(TASK_ATTEMPT_PREFIX, this.jobNumber, eventDetails.getString(ATTEMPTID));
      Put pTaskAttempt = new Put(taskAttemptIdKeyBytes);
      pTaskAttempt.add(Constants.INFO_FAM_BYTES, Constants.RECORD_TYPE_COL_BYTES,
        Bytes.toBytes(RecordTypes.Task.toString()));
      iterateAndPreparePuts(eventDetails, pTaskAttempt, recType);
      taskPuts.add(pTaskAttempt);
      break;

    case TaskFailed:
    case TaskStarted:
    case TaskUpdated:
    case TaskFinished:
      byte[] taskIdKeyBytes =
          getTaskKey(TASK_PREFIX, this.jobNumber, eventDetails.getString(TASKID));
      Put pTask = new Put(taskIdKeyBytes);
      pTask.add(Constants.INFO_FAM_BYTES, Constants.RECORD_TYPE_COL_BYTES,
        Bytes.toBytes(RecordTypes.Task.toString()));
      iterateAndPreparePuts(eventDetails, pTask, recType);
      taskPuts.add(pTask);
      break;
    default:
      LOG.error("Check if recType was modified and has new members?");
      throw new ProcessingException("Check if recType was modified and has new members? " + recType);
    }
  }

  /**
   * Sets the job ID and strips out the job number (job ID minus the "job_" prefix).
   * @param id
   */
  private void setJobId(String id) {
    if (id != null && id.startsWith("job_") && id.length() > 4) {
      this.jobNumber = id.substring(4);
    }
  }

  /**
   * maintains compatibility between hadoop 1.0 keys and hadoop 2.0 keys
   */
  private String getKey(String key) {
    return (oldToNewMapping.containsKey(key) ? oldToNewMapping.get(key).toLowerCase() : key
        .toLowerCase());
  }

  /**
   * populates a put for long values
   * @param {@link Put} p
   * @param {@link Constants} family
   * @param String key
   * @param long value
   */
  private void populatePut(Put p, byte[] family, String key, long value) {

    byte[] valueBytes = null;
    valueBytes = (value != 0L) ? Bytes.toBytes(value) : Constants.ZERO_LONG_BYTES;
    byte[] qualifier = Bytes.toBytes(getKey(key));
    p.add(family, qualifier, valueBytes);
  }

  /**
   * populates a put for int values
   * @param {@link Put} p
   * @param {@link Constants} family
   * @param String key
   * @param int value
   */
  private void populatePut(Put p, byte[] family, String key, int value) {

    byte[] valueBytes = null;
    valueBytes = (value != 0) ? Bytes.toBytes(value) : Constants.ZERO_INT_BYTES;
    byte[] qualifier = Bytes.toBytes(getKey(key));
    p.add(family, qualifier, valueBytes);
  }

  /**
   * populates a put for string values
   * @param {@link Put} p
   * @param {@link Constants} family
   * @param {@link String} key
   * @param String value
   */
  private void populatePut(Put p, byte[] family, String key, String value) {
    byte[] valueBytes = null;
    valueBytes = Bytes.toBytes(value);
    byte[] qualifier = Bytes.toBytes(getKey(key));
    p.add(family, qualifier, valueBytes);
  }

  /**
   * populates a put for {@link Counters}
   * @param {@link Put} p
   * @param {@link Constants} family
   * @param String key
   * @param String groupName
   * @param String counterName
   * @param long counterValue
   */
  private void populatePut(Put p, byte[] family, String key, String groupName, String counterName,
      Long counterValue) {
    byte[] counterPrefix = null;

    try {
      switch (JobHistoryKeys.valueOf(JobHistoryKeys.class, key)) {
      case COUNTERS:
        counterPrefix = Bytes.add(Constants.COUNTER_COLUMN_PREFIX_BYTES, Constants.SEP_BYTES);
        break;
      case MAP_COUNTERS:
        counterPrefix = Bytes.add(Constants.MAP_COUNTER_COLUMN_PREFIX_BYTES, Constants.SEP_BYTES);
        break;
      case REDUCE_COUNTERS:
        counterPrefix =
            Bytes.add(Constants.REDUCE_COUNTER_COLUMN_PREFIX_BYTES, Constants.SEP_BYTES);
        break;
      case TOTAL_COUNTERS:
        counterPrefix = Bytes.add(Constants.TOTAL_COUNTER_COLUMN_PREFIX_BYTES, Constants.SEP_BYTES);
        break;
      case TASK_COUNTERS:
        counterPrefix = Bytes.add(Constants.TASK_COUNTER_COLUMN_PREFIX_BYTES, Constants.SEP_BYTES);
        break;
      case TASK_ATTEMPT_COUNTERS:
        counterPrefix =
            Bytes.add(Constants.TASK_ATTEMPT_COUNTER_COLUMN_PREFIX_BYTES, Constants.SEP_BYTES);
        break;
      default:
        throw new IllegalArgumentException("Unknown counter type " + key.toString());
      }
    } catch (IllegalArgumentException iae) {
      throw new ProcessingException("Unknown counter type " + key, iae);
    } catch (NullPointerException npe) {
      throw new ProcessingException("Null counter type " + key, npe);
    }

    byte[] groupPrefix = Bytes.add(counterPrefix, Bytes.toBytes(groupName), Constants.SEP_BYTES);
    byte[] qualifier = Bytes.add(groupPrefix, Bytes.toBytes(counterName));
    p.add(family, qualifier, Bytes.toBytes(counterValue));
  }

  /**
   * Returns the Task ID or Task Attempt ID, stripped of the leading job ID, appended to the job row
   * key.
   */
  public byte[] getTaskKey(String prefix, String jobNumber, String fullId) {
    String taskComponent = fullId;
    if (fullId == null) {
      taskComponent = "";
    } else {
      String expectedPrefix = prefix + jobNumber + "_";
      if ((fullId.startsWith(expectedPrefix)) && (fullId.length() > expectedPrefix.length())) {
        taskComponent = fullId.substring(expectedPrefix.length());
      }
    }
    return taskKeyConv.toBytes(new TaskKey(this.jobKey, taskComponent));
  }

  /**
   * Returns the AM Attempt id stripped of the leading job ID, appended to the job row key.
   */
  public byte[] getAMKey(String prefix, String fullId) {

    String taskComponent = prefix + fullId;
    return taskKeyConv.toBytes(new TaskKey(this.jobKey, taskComponent));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<Put> getJobPuts() {
    return jobPuts;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<Put> getTaskPuts() {
    return taskPuts;
  }

  /**
   * utitlity function for printing all puts
   */
  public void printAllPuts() {
    printAllJobPuts();
    printAllTaskPuts();
  }

  /**
   * utitlity function for printing all Job puts
   */
  public void printAllJobPuts() {
    for (Put p1 : jobPuts) {
      Map<byte[], List<KeyValue>> d = p1.getFamilyMap();
      for (byte[] k : d.keySet()) {
        System.out.println(" k " + Bytes.toString(k));
      }
      for (List<KeyValue> lkv : d.values()) {
        for (KeyValue kv : lkv) {
          System.out.println("\n row: " + taskKeyConv.fromBytes(kv.getRow())
          // + "\n family " + Bytes.toString(kv.getFamily())
              + "\n " + Bytes.toString(kv.getQualifier()) + ": " + Bytes.toString(kv.getValue()));
        }
      }
    }
  }

  /**
   * utitlity function for printing all Task puts
   */
  public void printAllTaskPuts() {
    for (Put p1 : taskPuts) {
      Map<byte[], List<KeyValue>> d = p1.getFamilyMap();
      for (byte[] k : d.keySet()) {
        System.out.println(" k " + Bytes.toString(k));
      }
      for (List<KeyValue> lkv : d.values()) {
        for (KeyValue kv : lkv) {
          System.out.println("\n row: " + taskKeyConv.fromBytes(kv.getRow())
          // + "\n family " + Bytes.toString(kv.getFamily())
              + "\n " + Bytes.toString(kv.getQualifier()) + ": " + Bytes.toString(kv.getValue()));
        }
      }
    }
  }

}
