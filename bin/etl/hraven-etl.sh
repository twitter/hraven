#!/bin/bash
#
# Copyright 2013 Twitter, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Used pre-process unprocessed files in the /hadoop/mapred/history/done directory
# For each batch jobFiles, write a sequence file in /hadoop/mapred/history/processing/
# listing the jobFiles to be loaded and create corresponding process Record.
#
# Load all the jobFiles listed in the process file from the process record into HBase
#
# Script Use:
# 1. Set below parameters to correct values for execution environment
# 2. Run using "./hraven-etl.sh"
#

# Parameters
########## FILL IN APPROPRIATE VALUES BELOW ##########
cluster="mycluster"
mapredmaxsplitsize="204800"
batchsize="100"
schedulerpoolname="mypool"
threads="20"
hadoopconfdir=${HADOOP_CONF_DIR:-$HADOOP_HOME/conf}
hbaseconfdir=${HBASE_CONF_DIR:-$HBASE_HOME/conf}
# HDFS directories for processing and loading job history data
historyRawDir=/hadoop/mapred/history/done
historyProcessingDir=/hadoop/mapred/history/processing/
#######################################################

home=$(dirname $0)

# set the hraven-core jar as part of libjars and hadoop classpath
# set this here because it only pertains to the etl logic
export LIBJARS=$home/../../lib/hraven-core.jar
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$LIBJARS
hravenEtlJar=$home/../../lib/hraven-etl.jar

source $home/../../conf/hraven-env.sh
source $home/pidfiles.sh

# Each job has 2 files: history and config
batchsizejobs=$(($batchsize / 2))

myscriptname=$(basename "$0" .sh)
stopfile=$HRAVEN_PID_DIR/$myscriptname.stop
if [ -f $stopfile ]; then
  echo "Error: not allowed to run. Remove $stopfile continue." 1>&2
  exit 1
fi

create_pidfile $HRAVEN_PID_DIR
trap 'cleanup_pidfile_and_exit $HRAVEN_PID_DIR' INT TERM EXIT

# Pre-process
$home/jobFilePreprocessor.sh $hadoopconfdir $historyRawDir $historyProcessingDir $cluster $batchsize

# Load
$home/jobFileLoader.sh $hadoopconfdir $mapredmaxsplitsize $schedulerpoolname $cluster $historyProcessingDir

# Process
$home/jobFileProcessor.sh $hbaseconfdir $schedulerpoolname $historyProcessingDir $cluster $threads $batchsize