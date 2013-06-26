#!/usr/bin/env bash
#
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

#
# Runs a command with the bin/hraven script as a daemon (background)
#
# Environment variables:
#
#  HRAVEN_HOME      - Directory from which to run hraven. Defaults to parent directory of this script.
# 
#  HRAVEN_CONF_DIR  - Alternate configuration directory. Default is ${HRAVEN_HOME}/conf.
#
#  HRAVEN_LOG_DIR   - Directory where daemon logs should be written
#
#  HRAVEN_PID_DIR   - Directory where daemon process ID will be written
#

# Default setting for heap size
JAVA_HEAP_MAX=-Xmx1000m

# Determine the bin directory
bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

# Assume bin is within the home dir
HRAVEN_HOME=${HRAVEN_HOME:-$bin/..}
# suppress noisy logging
export HBASE_ROOT_LOGGER=WARN,console

print_usage_and_exit() {
  echo "Usage: hraven-daemon.sh [--config confdir] (start|stop|restart) <command>"
  echo "where <command> is one of the following:"
  echo
  echo "rest       - run the REST server"
  echo "CLASSNAME  - run the main method on the class CLASSNAME"
  echo ""
  exit 1
}


# if no args specified, show usage
if [ $# -le 1 ]; then
  print_usage_and_exit
fi

#check to see if the conf dir is given as an optional argument
if [ $# -gt 1 ]; then
  if [ "--config" = "$1" ]; then
    shift
    confdir=$1
	  shift
	  HRAVEN_CONF_DIR=$confdir
  fi
fi

# Respect conf dir if set, or else dedault
HRAVEN_CONF_DIR=${HRAVEN_CONF_DIR:-${HRAVEN_HOME}/conf}

# get arguments
startStop=$1
shift

command=$1
shift

rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
    num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
    while [ $num -gt 1 ]; do
        prev=`expr $num - 1`
        [ -f "$log.$prev" ] && mv -f "$log.$prev" "$log.$num"
        num=$prev
    done
    mv -f "$log" "$log.$num";
    fi
}

wait_until_done ()
{
    p=$1
    cnt=${HRAVEN_SLAVE_TIMEOUT:-300}
    origcnt=$cnt
    while kill -0 $p > /dev/null 2>&1; do
      if [ $cnt -gt 1 ]; then
        cnt=`expr $cnt - 1`
        sleep 1
      else
        echo "Process did not complete after $origcnt seconds, killing."
        kill -9 $p
        exit 1
      fi
    done
    return 0
}

# setup logging details
export HRAVEN_LOG_DIR=${HRAVEN_LOG_DIR:-$HRAVEN_HOME/logs}
export HRAVEN_PID_DIR=${HRAVEN_PID_DIR:-/tmp}
HRAVEN_IDENT_STRING=${HRAVEN_IDENT_STRING:-$USER}
logprefix=hraven-${HRAVEN_IDENT_STRING}-${command}-$HOSTNAME

export HRAVEN_LOGFILE=$logprefix.log
export HRAVEN_ROOT_LOGGER="WARN,DRFA"
loglog="${HRAVEN_LOG_DIR}/${HRAVEN_LOGFILE}"
logout=$HRAVEN_LOG_DIR/$logprefix.out
pidfile=$HRAVEN_PID_DIR/$logprefix.pid


case $startStop in

  (start)
    mkdir -p "$HRAVEN_PID_DIR"
    if [ -f $pidfile ]; then
      if kill -0 `cat $pidfile` > /dev/null 2>&1; then
        echo $command running as process `cat $pidfile`.  Stop it first.
        exit 1
      fi
    fi

    rotate_log $logout
    echo starting $command, logging to $logout
    # Add to the command log file vital stats on our environment.
    echo "`date` Starting $command on `hostname`" >> $loglog
    nohup "$HRAVEN_HOME"/bin/hraven \
        --config "${HRAVEN_CONF_DIR}" \
        $command "$@" $startStop > "$logout" 2>&1 < /dev/null &
    echo $! > $pidfile
    sleep 1; head "$logout"
    ;;

  (stop)
    if [ -f $pidfile ]; then
      # kill -0 == see if the PID exists 
      if kill -0 `cat $pidfile` > /dev/null 2>&1; then
        echo -n stopping $command
        echo "`date` Terminating $command" >> $loglog
        kill `cat $pidfile` > /dev/null 2>&1
        while kill -0 `cat $pidfile` > /dev/null 2>&1; do
          echo -n "."
          sleep 1;
        done
        rm $pidfile
        echo
      else
        retval=$?
        echo no $command to stop because kill -0 of pid `cat $pidfile` failed with status $retval
      fi
    else
      echo no $command to stop because no pid file $pidfile
    fi
    ;;

  (restart)
    thiscmd=$0
    args=$@
    # stop the command
    $thiscmd --config "${HRAVEN_CONF_DIR}" stop $command $args &
    wait_until_done $!
    # wait a user-specified sleep period
    sp=${HRAVEN_RESTART_SLEEP:-3}
    if [ $sp -gt 0 ]; then
      sleep $sp
    fi
    # start the command
    $thiscmd --config "${HRAVEN_CONF_DIR}" start $command $args &
    wait_until_done $!
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac
