#Copyright 2013 Twitter, Inc.
#
#Licensed under the Apache License, Version 2.0 (the "License");
#you may not use this file except in compliance with the License.
#You may obtain a copy of the License at
#
#http://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.
#!/usr/bin/env bash

# Runs command line jmx client for hadoopJobMonitor
#
# Environment Variables
#
#   VULTURE_PID_DIR   The pid files are stored. /tmp by default. Can also be defined in hadoopJobMonitor-env.sh
#   VULTURE_CONF_DIR   The conf directory, default ../src/main/resources. Can also be set via command line.
##

# Example: ./hadoopJobMonitor-admin.sh whitelist set application_234234_234 120  

if which greadlink > /dev/null; then
  READLINK=greadlink
else
  READLINK=readlink
fi
SCRIPTFILE=`$READLINK -f $0`
SCRIPTDIR=`dirname $SCRIPTFILE`

vpid=`jps | grep VultureService`
if [ $? -ne 0 ]; then
  echo "VultureService is not found!"
  exit 1
fi
vpid=`echo $vpid | awk '{print $1}'`

run() {
  java -jar $SCRIPTDIR/jmxterm-1.0-SNAPSHOT-uber.jar --noninter << EOF
  open $vpid
  bean com.twitter.hraven.hadoopJobMonitor.jmx:type=WhiteList
  run $*
EOF
}

list() {
  java -jar $SCRIPTDIR/jmxterm-1.0-SNAPSHOT-uber.jar --noninter << EOF
  open $vpid
  bean com.twitter.hraven.hadoopJobMonitor.jmx:type=WhiteList
  get Expirations
EOF
}

setExpiration() {
  appId=$1
  minutes=$2
  java -jar $SCRIPTDIR/jmxterm-1.0-SNAPSHOT-uber.jar --noninter << EOF
  open $vpid
  bean com.twitter.hraven.hadoopJobMonitor.jmx:type=WhiteList
  run expireAfterMinutes $appId $minutes
  get Expirations
EOF
}

usage() {
  echo "Usage: $0 whitelist [set appId minutes] [cmd command]"
  echo "where "
  echo "appId     is the application id e.g., application_123123123_2323"
  echo "minutes   adds the appId to the white list for next N minutes"
  echo "command   the command to be executed via jmx"
}

# if no args specified, show usage
if [ $# = 0 ]; then
  usage;
  exit 1
fi

command=$1

if [ "$command" = "whitelist" ]; then
  shift
  if [ $# = 0 ]; then
    list
    exit 0
  elif [ "$1" = "set" ]; then
    shift
    if [ $# = 2 ]; then
      setExpiration $*
      exit 0
    fi
  elif [ "$1" = "cmd" ]; then
    shift
    if [ $# -ge 1 ]; then
      run $*
      exit 0
    fi
  fi
fi

usage;
exit 1

