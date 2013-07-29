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
package com.twitter.hraven.etl;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobHistoryCopy.RecordTypes;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.AMInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.JobInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;

import com.twitter.hraven.Constants;
import com.twitter.hraven.JobHistoryKeys;
import com.twitter.hraven.JobKey;
import com.twitter.hraven.TaskKey;
import com.twitter.hraven.util.ByteArrayWrapper;
import com.twitter.hraven.datasource.JobKeyConverter;
import com.twitter.hraven.datasource.ProcessingException;
import com.twitter.hraven.datasource.TaskKeyConverter;

/**
 * Deal with JobHistory file parsing for job history files which are generated
 * after MAPREDUCE-1016 (hadoop 1.x (0.21 and later) and hadoop 2.x)
 */
public class JobHistoryFileParserHadoop2 implements JobHistoryFileParser {

	private JobKey jobKey;
	private String jobId;
	/** Job ID, minus the leading "job_" */
	private String jobNumber = "";
	private byte[] jobKeyBytes;
	private List<Put> jobPuts = new LinkedList<Put>();
	private List<Put> taskPuts = new LinkedList<Put>();
	private JobKeyConverter jobKeyConv = new JobKeyConverter();
	private TaskKeyConverter taskKeyConv = new TaskKeyConverter();

	private static final String AM_ATTEMPT_PREFIX = "AM_";
	private static final String TASK_PREFIX= "task_";
	private static final String TASK_ATTEMPT_PREFIX = "taskAttempt_";

	private static final Log LOG = LogFactory.getLog(JobHistoryFileParserHadoop2.class);

	/**
	 * {@inheritDoc}
	 * 
	 */
	@Override
	public void parse(String historyFileContents, JobKey jobKey)
			throws ProcessingException {

		try {
			JobHistoryParser parser = new JobHistoryParser(
					new FSDataInputStream(new ByteArrayWrapper(historyFileContents.getBytes())));
			JobInfo jobInfo = parser.parse();
			this.jobKey = jobKey;
			this.jobKeyBytes = jobKeyConv.toBytes(jobKey);
			setJobId(jobKey.getJobId().getJobIdString());

			LOG.info("Processing job history for jobKey:" + jobKey.toString()
					+ " with jobName: " + jobInfo.getJobname());
			setJobPuts(jobInfo);
			setTaskPuts(jobInfo);
		} catch (IOException ioe) {
			LOG.error(" Exception during parsing hadoop 2.0 file ", ioe);
			throw new ProcessingException(" Unable to parse history file in function parse, "
						+ "cannot process this record!" + jobKey
						+ " error: ", ioe);
		}
	}

	/**
	 * Sets the job ID and strips out the job number (job ID minus the "job_"
	 * prefix).
	 * 
	 * @param id
	 */
	private void setJobId(String id) {
		this.jobId = id;
		if (id != null && id.startsWith("job_") && id.length() > 4) {
			this.jobNumber = id.substring(4);
			LOG.debug("Jobnumber for jobId " + id + " is " + this.jobNumber);
		}
	}

	/**
	 * populates a put for long values
	 * 
	 * @param {@link Put} p
	 * @param {@link Constants} family
	 * @param {@link JobHistoryKeys} key
	 * @param long value
	 */
	private void populatePut(Put p, byte[] family, JobHistoryKeys key,
			long value) {

		byte[] valueBytes = null;
		try {
			valueBytes = (value != 0L) ? Bytes.toBytes(value) : Constants.ZERO_LONG_BYTES;
		} catch (NumberFormatException nfe) {
			// use a default value
			valueBytes = Constants.ZERO_LONG_BYTES;
		}
		byte[] qualifier = Bytes.toBytes(key.toString().toLowerCase());
		p.add(family, qualifier, valueBytes);
	}

	/**
	 * populates a put for int values
	 * 
	 * @param {@link Put} p
	 * @param {@link Constants} family
	 * @param {@link JobHistoryKeys} key
	 * @param int value
	 */
	private void populatePut(Put p, byte[] family, JobHistoryKeys key, int value) {

		byte[] valueBytes = null;
		try {
			valueBytes = (value != 0) ? Bytes.toBytes(value) : Constants.ZERO_INT_BYTES;
		} catch (NumberFormatException nfe) {
			// use a default value
			valueBytes = Constants.ZERO_INT_BYTES;
		}
		byte[] qualifier = Bytes.toBytes(key.toString().toLowerCase());
		p.add(family, qualifier, valueBytes);
	}

	/**
	 * populates a put for string values
	 * 
	 * @param {@link Put} p
	 * @param {@link Constants} family
	 * @param {@link JobHistoryKeys} key
	 * @param String value
	 */
	private void populatePut(Put p, byte[] family, JobHistoryKeys key, String value) {
		byte[] valueBytes = null;
		valueBytes = Bytes.toBytes(value);
		byte[] qualifier = Bytes.toBytes(key.toString().toLowerCase());
		p.add(family, qualifier, valueBytes);
	}

	/**
	 * populates a put for {@link AMInfo} values
	 * 
	 * @param {@link Constants} family
	 * @param {@link JobHistoryKeys} key
	 * @param {@link AMInfo} amInfo
	 */
	private void populatePut(byte[] family, AMInfo amInfo) {
	
		byte[] amAttemptIdKeyBytes = getAMKey(AM_ATTEMPT_PREFIX, amInfo.getAppAttemptId().toString());	
		// generate a new put per AM Attempt
		Put p = new Put(amAttemptIdKeyBytes);	
		populatePut(p, family, JobHistoryKeys.APPLICATION_ATTEMPT_ID, amInfo.getAppAttemptId().toString());
		populatePut(p, family, JobHistoryKeys.AM_START_TIME, amInfo.getStartTime());
		populatePut(p, family, JobHistoryKeys.CONTAINER_ID, amInfo.getContainerId().toString());
		populatePut(p, family, JobHistoryKeys.NODE_MANAGER_HOST, amInfo.getNodeManagerHost());
		populatePut(p, family, JobHistoryKeys.NODE_MANAGER_PORT, amInfo.getNodeManagerPort());
		populatePut(p, family, JobHistoryKeys.NODE_MANAGER_HTTP_PORT, amInfo.getNodeManagerHttpPort());
		this.taskPuts.add(p);
	}

	/**
	 * populates a put for {@link Counters}
	 * 
	 * @param {@link Put} p
	 * @param {@link Constants} family
	 * @param {@link JobHistoryKeys} key
	 * @param {@link Counters} c
	 */
	private void populatePut(Put p, byte[] family, JobHistoryKeys key, Counters c) {
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
		} else if (key == JobHistoryKeys.TOTAL_COUNTERS) {
			counterPrefix = Bytes.add(Constants.TOTAL_COUNTER_COLUMN_PREFIX_BYTES,
					Constants.SEP_BYTES);
		} else if (key == JobHistoryKeys.TASK_COUNTERS) {
			counterPrefix = Bytes.add(Constants.TASK_COUNTER_COLUMN_PREFIX_BYTES,
					Constants.SEP_BYTES);
		} else if (key == JobHistoryKeys.TASK_ATTEMPT_COUNTERS) {
			counterPrefix = Bytes.add(Constants.TASK_ATTEMPT_COUNTER_COLUMN_PREFIX_BYTES,
					Constants.SEP_BYTES);
		} else {
			throw new IllegalArgumentException("Unknown counter type " + key.toString());
		}

		for (CounterGroup group : c) {
			byte[] groupPrefix = Bytes.add(counterPrefix, Bytes.toBytes(group.getName()), 
					Constants.SEP_BYTES);
			for (Counter counter : group) {
				byte[] qualifier = Bytes.add(groupPrefix, Bytes.toBytes(counter.getName()));
				p.add(family, qualifier, Bytes.toBytes(counter.getValue()));
			}
		}
	}

	/**
	 * Populate the job puts from
	 * {@link org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.JobInfo}
	 */
	private void setJobPuts(JobInfo jobInfo) {

		// create a new put per job key
		Put p = new Put(this.jobKeyBytes);
		populatePut(p, Constants.INFO_FAM_BYTES, JobHistoryKeys.JOBNAME, jobInfo.getJobname());
		populatePut(p, Constants.INFO_FAM_BYTES, JobHistoryKeys.USER, jobInfo.getUsername());
		populatePut(p, Constants.INFO_FAM_BYTES, JobHistoryKeys.JOB_QUEUE, jobInfo.getJobQueueName());
		populatePut(p, Constants.INFO_FAM_BYTES, JobHistoryKeys.SUBMIT_TIME, jobInfo.getSubmitTime());
		populatePut(p, Constants.INFO_FAM_BYTES, JobHistoryKeys.LAUNCH_TIME, jobInfo.getLaunchTime());
		populatePut(p, Constants.INFO_FAM_BYTES, JobHistoryKeys.JOB_STATUS, jobInfo.getJobStatus());
		populatePut(p, Constants.INFO_FAM_BYTES, JobHistoryKeys.JOB_PRIORITY, jobInfo.getPriority());
		populatePut(p, Constants.INFO_FAM_BYTES, JobHistoryKeys.TOTAL_MAPS,	jobInfo.getTotalMaps());
		populatePut(p, Constants.INFO_FAM_BYTES, JobHistoryKeys.TOTAL_REDUCES, jobInfo.getTotalReduces());
		populatePut(p, Constants.INFO_FAM_BYTES, JobHistoryKeys.UBERIZED, Boolean.toString(jobInfo.getUberized()));

		Counters mc = jobInfo.getMapCounters();
		if (mc != null) {
			populatePut(p, Constants.INFO_FAM_BYTES, JobHistoryKeys.MAP_COUNTERS, mc);
		}

		Counters rc = jobInfo.getReduceCounters();
		if (rc != null) {
			populatePut(p, Constants.INFO_FAM_BYTES, JobHistoryKeys.REDUCE_COUNTERS, rc);
		}

		Counters tc = jobInfo.getTotalCounters();
		if (rc != null) {
			populatePut(p, Constants.INFO_FAM_BYTES, JobHistoryKeys.TOTAL_COUNTERS, tc);
		}
		
		// store the puts created so far
		this.jobPuts.add(p);		
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
			if ((fullId.startsWith(expectedPrefix)) && 
					(fullId.length() > expectedPrefix.length())) {
				taskComponent = fullId.substring(expectedPrefix.length());
			}
		}
		return taskKeyConv.toBytes(new TaskKey(this.jobKey, taskComponent));
	}

	/**
	 * Returns the AM Attempt id stripped of the leading job ID,
	 * appended to the job row key.
	 */
	public byte[] getAMKey(String prefix, String fullId) {
		
		String taskComponent = prefix + fullId;
		return taskKeyConv.toBytes(new TaskKey(this.jobKey, taskComponent));
	}

	/**
	 * Populate the task puts from
	 * {@link org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.JobInfo}
	 */
	private void setTaskPuts(JobInfo jobInfo) {
		
		// Application Master related puts 
		// this is 2.0 specific 
		List<AMInfo> amInfos = jobInfo.getAMInfos();
		if ((amInfos != null) && amInfos.size() > 0) {
			for (AMInfo amInfo : jobInfo.getAMInfos()) {
				// will create one put per AM Attempt
				populatePut(Constants.INFO_FAM_BYTES, amInfo);
			}
		}

		for (TaskInfo ti : jobInfo.getAllTasks().values()) {
			byte[] taskIdKeyBytes = getTaskKey(TASK_PREFIX, this.jobNumber, ti.getTaskId().toString());
			Put p = new Put(taskIdKeyBytes);
			p.add(Constants.INFO_FAM_BYTES, Constants.RECORD_TYPE_COL_BYTES, 
					Bytes.toBytes(RecordTypes.Task.toString()));
			populatePut(p, Constants.INFO_FAM_BYTES, JobHistoryKeys.TASKID, ti.getTaskId().toString());
			populatePut(p, Constants.INFO_FAM_BYTES, JobHistoryKeys.TASK_START_TIME, ti.getStartTime());
			populatePut(p, Constants.INFO_FAM_BYTES, JobHistoryKeys.TASK_FINISH_TIME, ti.getFinishTime());
			populatePut(p, Constants.INFO_FAM_BYTES, JobHistoryKeys.TASK_TYPE, ti.getTaskType().toString());
			if (ti.getCounters() != null) {
				populatePut(p, Constants.INFO_FAM_BYTES, JobHistoryKeys.TASK_COUNTERS, ti.getCounters());
			}
			this.taskPuts.add(p);
			
			for (TaskAttemptInfo tinfo : ti.getAllTaskAttempts().values()) {
				byte[] taskAttemptIdKeyBytes = getTaskKey(TASK_ATTEMPT_PREFIX, this.jobNumber, tinfo.getAttemptId().toString());
				Put pTaskAttempt = new Put(taskAttemptIdKeyBytes);

				populatePut(pTaskAttempt, Constants.INFO_FAM_BYTES, JobHistoryKeys.TASK_ATTEMPT_ID,
						tinfo.getAttemptId().toString());
				populatePut(pTaskAttempt, Constants.INFO_FAM_BYTES,	JobHistoryKeys.START_TIME,
						tinfo.getStartTime());
				populatePut(pTaskAttempt, Constants.INFO_FAM_BYTES, JobHistoryKeys.FINISH_TIME,
						tinfo.getFinishTime());
				populatePut(pTaskAttempt, Constants.INFO_FAM_BYTES, JobHistoryKeys.ERROR,
						tinfo.getError());
				populatePut(pTaskAttempt, Constants.INFO_FAM_BYTES, JobHistoryKeys.TASK_STATUS,
						tinfo.getTaskStatus());
				populatePut(pTaskAttempt, Constants.INFO_FAM_BYTES,	JobHistoryKeys.TASK_STATE,
						tinfo.getState());
				populatePut(pTaskAttempt, Constants.INFO_FAM_BYTES, JobHistoryKeys.TASK_TYPE,
						tinfo.getTaskType().toString());
				populatePut(pTaskAttempt, Constants.INFO_FAM_BYTES, JobHistoryKeys.TRACKER_NAME,
						tinfo.getTrackerName());
				populatePut(pTaskAttempt, Constants.INFO_FAM_BYTES,	JobHistoryKeys.HTTP_PORT,
						tinfo.getHttpPort());
				populatePut(pTaskAttempt, Constants.INFO_FAM_BYTES,	JobHistoryKeys.SHUFFLE_PORT,
						tinfo.getShufflePort());
				populatePut(pTaskAttempt, Constants.INFO_FAM_BYTES, JobHistoryKeys.CONTAINER_ID, 
						tinfo.getContainerId().toString());
				if (tinfo.getCounters() != null) {
					populatePut(pTaskAttempt, Constants.INFO_FAM_BYTES, 
							JobHistoryKeys.TASK_ATTEMPT_COUNTERS, tinfo.getCounters());
				}
				this.taskPuts.add(pTaskAttempt);
			}
		}
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
}
