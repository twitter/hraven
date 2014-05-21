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
#

# Run on the daemon node per specific cluster
# This script runs on the HBase cluster
# Usage ./jobFileProcessor.sh [hadoopconfdir]
#   [schedulerpoolname] [historyprocessingdir] [cluster] [threads] [batchsize] [machinetype] [costfile]
# a sample cost file can be found in the conf dir as sampleCostDetails.properties

if [ $# -ne 8 ]
then
  echo "Usage: `basename $0` [hbaseconfdir] [schedulerpoolname] [historyprocessingdir] [cluster] [threads] [batchsize] [machinetype] [costfile]"
  exit 1
fi

source $(dirname $0)/hraven-etl-env.sh

myscriptname=$(basename "$0" .sh)
stopfile=$HRAVEN_PID_DIR/$myscriptname.stop

if [ -f $stopfile ]; then
  echo "Error: not allowed to run. Remove $stopfile continue." 1>&2
  exit 1
fi

create_pidfile $HRAVEN_PID_DIR
trap 'cleanup_pidfile_and_exit $HRAVEN_PID_DIR' INT TERM EXIT

hadoop --config $1 jar $hravenEtlJar com.twitter.hraven.etl.JobFileProcessor -libjars=$LIBJARS -Dmapred.fairscheduler.pool=$2 -d -p $3 -c $4 -t $5 -b $6 -m $7 -z $8

