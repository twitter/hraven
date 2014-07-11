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
cluster="mycluster" #Name of your cluster (arbitrary)
mapredmaxsplitsize="204800"
batchsize="100" #default is 1, which is bad for mapred job
schedulerpoolname="mypool" #name of scheduler pool (arbitrary)
threads="20"
defaultrawfilesizelimit="524288000"
machinetype="mymachine" #name of machine (arbitrary)
costfile=/var/lib/hraven/conf/costFile
#conf directories
hadoopconfdir=${HADOOP_CONF_DIR:-$HADOOP_HOME/conf}
hbaseconfdir=${HBASE_CONF_DIR:-$HBASE_HOME/conf}
# HDFS directories for processing and loading job history data
historyRawDir=/yarn/history/done/
historyProcessingDir=/hraven/processing/
#######################################################

#If costfile is empty, fill it with default values
if [[ -z `cat $costfile` ]]; then
  echo "$machinetype.computecost=3.0" > $costfile
  echo "$machinetype.machinememory=12000" >> $costfile
fi

source $(dirname $0)/hraven-etl-env.sh

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
$home/jobFilePreprocessor.sh $hadoopconfdir $historyRawDir $historyProcessingDir $cluster $batchsize $defaultrawfilesizelimit

# Load
$home/jobFileLoader.sh $hadoopconfdir $mapredmaxsplitsize $schedulerpoolname $cluster $historyProcessingDir

# Process
$home/jobFileProcessor.sh $hbaseconfdir $schedulerpoolname $historyProcessingDir $cluster $threads $batchsize $machinetype $costfile
