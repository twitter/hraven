#!/usr/bin/env bash

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

# Runs a hadoopJobMonitor as a daemon.
#
# Environment Variables
#
#   HADOOP_CLASSPATH   Where are the hadoop jar files. Empty string by default. Can also be defined in hadoopJobMonitor-env.sh
#   VULTURE_LOG_DIR   Where log files are stored.  PWD by default. Can also be defined in hadoopJobMonitor-env.sh
#   VULTURE_PID_DIR   The pid files are stored. /tmp by default. Can also be defined in hadoopJobMonitor-env.sh
#   VULTURE_CONF_DIR   The conf directory, default ../src/main/resources. Can also be set via command line.
#   VULTURE_IDENT_STRING   A string representing this instance of hadoopJobMonitor. $USER by default
##

app=hadoopJobMonitor
if which greadlink > /dev/null; then
  READLINK=greadlink
else
  READLINK=readlink
fi
SCRIPTFILE=`$READLINK -f $0`
SCRIPTDIR=`dirname $SCRIPTFILE`
export VULTURE_PREFIX=$SCRIPTDIR/..
cd $SCRIPTDIR;

VULTURE_CONF_DIR=../src/main/resources
#check to see if the conf dir is given as an optional argument
if [ $# -gt 1 ]
then
    if [ "--config" = "$1" ]
    then
        shift
        confdir=$1
        shift
        VULTURE_CONF_DIR=$confdir
    fi
fi

if [ -f "${VULTURE_CONF_DIR}/hadoopJobMonitor-env.sh" ]; then
  . "${VULTURE_CONF_DIR}/hadoopJobMonitor-env.sh"
fi

CLASSPATH=${VULTURE_CONF_DIR}:$HADOOP_CLASSPATH
CLASSPATH=$CLASSPATH:../src/main/resources
for j in ../target/hadoopJobMonitor*.jar; do
  CLASSPATH=$CLASSPATH:$j
done
for j in ../lib/*.jar; do
  CLASSPATH=$CLASSPATH:$j
done

if [ "$VULTURE_IDENT_STRING" = "" ]; then
  export VULTURE_IDENT_STRING="$USER"
fi
# get log directory
if [ "$VULTURE_LOG_DIR" = "" ]; then
  export VULTURE_LOG_DIR="$VULTURE_PREFIX/logs"
fi
if [ ! -w "$VULTURE_LOG_DIR" ] ; then
  mkdir -p "$VULTURE_LOG_DIR"
  chown $VULTURE_IDENT_STRING $VULTURE_LOG_DIR
fi
if [ "$VULTURE_PID_DIR" = "" ]; then
  VULTURE_PID_DIR=/tmp
fi
log=$VULTURE_LOG_DIR/hraven-$VULTURE_IDENT_STRING-$app-$HOSTNAME.out
pid=$VULTURE_PID_DIR/$app.pid
VULTURE_STOP_TIMEOUT=${VULTURE_STOP_TIMEOUT:-5}

VULTURE_OPTIONS="-DhadoopJobMonitor.log.dir=$VULTURE_LOG_DIR"

checkpid() {
  [ -w "$VULTURE_PID_DIR" ] ||  mkdir -p "$VULTURE_PID_DIR"

  if [ -f $pid ]; then
    if kill -0 `cat $pid` > /dev/null 2>&1; then
      echo $app running as process `cat $pid`.  Stop it first.
      exit 1
    fi
  fi
}


start() {
  checkpid;
  echo starting $app $1, logging to $log
  echo starting $app $1 >> $log
  nohup java -Xmx4096m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$VULTURE_LOG_DIR/hadoopJobMonitor.heap.dump.hprof -cp $CLASSPATH $VULTURE_OPTIONS -Dlog4j.debug -Dlog4j.configuration=log4j.properties com.twitter.hraven.hadoopJobMonitor.VultureService $* >> "$log" 2>&1 < /dev/null &
  echo $! > $pid
  sleep 1
  tail "$log"
  if ! ps -p $! > /dev/null ; then
    echo "Error: The VultureService exited!"
    exit 1
  fi
}

stop() {
  if [ -f $pid ]; then
    TARGET_PID=`cat $pid`
    if kill -0 $TARGET_PID > /dev/null 2>&1; then
      echo stopping $app
      kill $TARGET_PID
      sleep 1
      if kill -0 $TARGET_PID > /dev/null 2>&1; then
        sleep $VULTURE_STOP_TIMEOUT
        if kill -0 $TARGET_PID > /dev/null 2>&1; then
          echo "$app did not stop gracefully after $VULTURE_STOP_TIMEOUT seconds: killing with kill -9"
          kill -9 $TARGET_PID
        fi
      fi
    else
      echo no $app to stop
    fi
  else
    echo no $app to stop
  fi
}

usage() {
  echo "Usage: $0 [--config <conf-dir>] (start|stop)"
  echo "where (start|stop) is:"
  echo "start                  Start the hadoopJobMonitor service."
  echo "stop                   Stop the hadoopJobMonitor service"
}

# if no args specified, show usage
if [ $# = 0 ]; then
  usage;
  exit 1
fi

command=$1

if [ "$command" = "start" ]; then
  shift
  start $*;
elif [ "$command" = "stop" ]; then
  shift
  stop $*;
else
  usage;
  exit 1
fi

