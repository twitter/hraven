#!/usr/bin/env bash

# Runs a vulture as a daemon.
#
# Environment Variables
#
#   VULTURE_LOG_DIR   Where log files are stored.  PWD by default.
#   VULTURE_PID_DIR   The pid files are stored. /tmp by default.
#   VULTURE_IDENT_STRING   A string representing this instance of hadoop. $USER by default
##

app=vulture
if which greadlink > /dev/null; then
  READLINK=greadlink
else
  READLINK=readlink
fi
SCRIPTFILE=`$READLINK -f $0`
SCRIPTDIR=`dirname $SCRIPTFILE`
export VULTURE_PREFIX=$SCRIPTDIR/..
cd $SCRIPTDIR;
CLASSPATH=/etc/hadoop/conf
CLASSPATH=$CLASSPATH:../src/main/resources
for j in ../target/vulture*.jar; do
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
log=$VULTURE_LOG_DIR/hadoop-$VULTURE_IDENT_STRING-$app-$HOSTNAME.out
pid=$VULTURE_PID_DIR/hadoop-$VULTURE_IDENT_STRING-$app.pid
VULTURE_STOP_TIMEOUT=${VULTURE_STOP_TIMEOUT:-5}

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
  init;
  checkpid;
  echo starting $app $1, logging to $log
  echo starting $app $1 >> $log
  nohup java -Xmx512m -cp $CLASSPATH -Dlog4j.configuration=log4j.properties com.twitter.vulture.VultureService $1 >> "$log" 2>&1 < /dev/null &
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
  echo "Usage: $0 (start|stop)"
  echo "where (start|stop) is:"
  echo "start <logpath>         Start the top service reading from the log file."
  echo "stop                    Stop the top service"
}

# if no args specified, show usage
if [ $# = 0 ]; then
  usage;
  exit 1
fi

command=$1

if [ "$command" = "start" ]; then
  shift
  if [ $# = 0 ]; then
    usage;
    exit 1
  fi
  start $*;
elif [ "$command" = "stop" ]; then
  shift
  stop $*;
else
  usage;
  exit 1
fi

