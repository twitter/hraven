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
# Usage ./jobFilePreprocessor.sh [hadoopconfdir]
#   [historyrawdir] [historyprocessingdir] [cluster] [batchsize]

if [ $# -ne 5 ]
then
  echo "Usage: `basename $0` [hadoopconfdir] [historyrawdir] [historyprocessingdir] [cluster] [batchsize]"
  exit 1
fi

home=$(dirname $0)
source $home/../../conf/hraven-env.sh
source $home/pidfiles.sh
myscriptname=$(basename "$0" .sh)
stopfile=$HRAVEN_PID_DIR/$myscriptname.stop
hravenEtlJar=$home/../../lib/hraven-etl.jar
LIBJARS=$home/../../lib/hraven-core.jar
export HADOOP_HEAPSIZE=4000
export HADOOP_CLASSPATH=$(ls $home/../../lib/commons-lang-*.jar)

if [ -f $stopfile ]; then
  echo "Error: not allowed to run. Remove $stopfile continue." 1>&2
  exit 1
fi

create_pidfile $HRAVEN_PID_DIR
trap 'cleanup_pidfile_and_exit $HRAVEN_PID_DIR' INT TERM EXIT

hadoop --config $1 jar $hravenEtlJar com.twitter.hraven.etl.JobFilePreprocessor -libjars=$LIBJARS -d -i $2 -o $3 -c $4 -b $5