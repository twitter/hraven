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

# Used to just dump out the process records in a readable form for a given cluster.
# Used for manual debugging and verification.

# Run on the daemon node per specific cluster
# Usage ./processingRecordsPrinter.sh [hbaseconfdir] [cluster]

if [ $# -ne 2 ]
then
  echo "Usage: `basename $0` [hbaseconfdir] [cluster]"
  exit 1
fi

home=$(dirname $0)
source $home/../../conf/hraven-env.sh
hravenEtlJar=$home/../../lib/hraven-etl.jar
LIBJARS=$home/../../lib/hraven-core.jar

hadoop --config $1 jar $hravenEtlJar com.twitter.hraven.etl.ProcessingRecordsPrinter -libjars=$LIBJARS -c $2
