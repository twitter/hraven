hRaven [![Build Status](https://travis-ci.org/twitter/hraven.png)](https://travis-ci.org/twitter/hraven) [![Coverage Status](https://coveralls.io/repos/twitter/hraven/branch.png?branch=master)](https://coveralls.io/r/twitter/hraven?branch=master)
==========

hRaven collects run time data and statistics from map reduce jobs running on
Hadoop clusters and stores the collected job history in an easily queryable
format. For the jobs that are run through frameworks (Pig or
Scalding/Cascading) that decompose a script or application into a DAG of map
reduce jobs for actual execution, hRaven groups job history data together by
an application construct.  This allows for easier visualization of all of the
component jobs' execution for an application and more comprehensive trending
and analysis over time.

Requirements
--------------------

* Apache HBase (0.94+) - a running HBase cluster is required for the hRaven
  data storage
* Apache Hadoop - hRaven current supports collection of job data on specific
  versions of Hadoop:
  * CDH upto CDH3u5, Hadoop 1.x upto MAPREDUCE-1016
  * Hadoop 1.x post MAPREDUCE-1016 and Hadoop 2.0 are supported in versions 0.9.4 onwards

Quick start
--------------------

Clone the github repo or download the latest release:

    git clone git://github.com/twitter/hraven.git

If you cloned the repository, build the full tarball:

    mvn clean package assembly:single

Extract the assembly tarball on a machine with HBase client access.

Create the initial schema

    hbase [--config /path/to/hbase/conf] shell bin/create_schema.rb


Schema
--------------------

hRaven requires the following HBase tables in order to store data for map
reduce jobs:

* `job_history` - job-level statistics, one row per job
* `job_history_task` - task-level statistics, one row per task attempt
* `job_history-by_jobId` - index table pointing to `job_history` row by job ID
* `job_history_app_version` - distinct versions associated with an
  application, one row per application
* `job_history_raw` - stores the raw job configuration and job history files,
  as byte[] blobs
* `job_history_process` - meta table storing progress information for the data
  loading process
* `flow_queue` - time based index of flows for Ambrose integration
* `flow_event` - stores flow progress events for Ambrose integration

The initial table schema can be created by running the `create_schema.rb`
script:

    hbase [--config /path/to/hbase/conf] shell bin/create_schema.rb


Data Loading
--------------------

Currently, hRaven loads data for _completed_ map reduce jobs by reading and parsing the job history and job configuration files from HDFS.  As a pre-requisite, the Hadoop Job Tracker must be configured to archive job history files in HDFS, by adding the following setting to your `mapred-site.xml` file:

    <property>
      <name>mapred.job.tracker.history.completed.location</name>
       <value>hdfs://<namenode>:8020/hadoop/mapred/history/done</value>
      <description>Store history and conf files for completed jobs in HDFS.
      </description>
    </property>

Once your Job Tracker is running with this setting in place, you can load data into hRaven with a series of map reduce jobs:

1. **JobFilePreprocessor** - scans the HDFS job history archive location for newly completed jobs; writes the new filenames to a sequence file for processing in the next stage; records the sequence file name in a new row in the `job_history_process` table
2. **JobFileRawLoader** - scans the processing table for new records from JobFileProcessor; reads the associated sequence files; writes the associated job history files for each sequence file entry into the HBase `job_history_raw` table
3. **JobFileProcessor** - reads new records from the raw table; parses the stored job history contents into individual puts for the `job_history`, `job_history_task`, and related index tables

Each job has an associated shell script under the `bin/` directory.  See these scripts for more details on the job parameters.

REST API
--------------------

Once data has been loaded into hRaven tables, a REST API provides access to job data for common query patterns.  hRaven ships with a simple REST server, which can be started or stopped with the command:

    ./bin/hraven-daemon.sh (start|stop) rest

The following endpoints are currently supported:

### Get Job

Path: `/job/<cluster>[/jobId]`  
Returns: single job  
Optional QS Params: n/a

### Get Flow By JobId

Path: `/jobFlow/<cluster>[/jobId]`  
Returns: the flow for the jobId  
Optional QS Params - v1:  

* `limit` (default=1)

### Get Flows

Path: `/flow/<cluster>/<user>/<appId>[/version]`  
Returns: list of flows  
Optional QS Params - v1:

* `limit` (default=1) - max number of flows to return
* `includeConf` - filter configuration property keys to return only the given
  names
* `includeConfRegex` - filter configuration property keys to return only those
  matching the given regex patterns

### Get Flow Timeseries

Path: `/flowStats/<cluster>/<user>/<app>`  
Returns: list of flows with only minimal stats  
Optional QS params:

* `version` (optional filter)
* `startRow` (base64 encoded row key)
* `startTime` (ms since epoch) - restrict results to given time window
* `endTime` (ms since epoch) - restrict results to given time window
* `limit` (default=100) - max flows to return
* `includeJobs` (boolean flag) - include per-job details

*Note:* This endpoint duplicates functionality from the "/flow/" endpoint and
 maybe be combined back in to it in the future.


### Get App Versions

Path: `/appVersion/<cluster>/<user>/<app>`  
Returns: list of distinct app versions  
Optional QS params:

* `limit` - max results to return


Project Resources
--------------------

### Bug tracker
Have a bug? Please create an issue here on GitHub
https://github.com/twitter/hraven/issues

### Mailing list
Have a question? Ask on our mailing list!

*hRaven Users:*

[hraven-user@googlegroups.com](http://groups.google.com/group/hraven-user)

*hRaven Developers:*

[hraven-dev@googlegroups.com](http://groups.google.com/group/hraven-dev)

### Contributing to hRaven
For more details on how to contribute to hRaven, see CONTRIBUTING.md.


Known Issues
--------------------

1. While hRaven stores the full data available from job history logs, the rolled-up statistics in the `Flow` class only represent data from sucessful task attempts.  We plan to extend this so that the `Flow` class also reflects resources used by failed and killed task attempts.

Copyright and License
---------------------
Copyright 2013 Twitter, Inc. and other contributors

Licensed under the Apache License Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
