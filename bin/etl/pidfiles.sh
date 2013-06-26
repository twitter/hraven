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

# This script does not do anything by itself, but includes functions for dealing with PID file locks.
#
# Usage:
## Pull in functions to manage pid files.
# source $(dirname $0)/pidfiles.sh
## Race to get the lock
# create_pidfile
## Make sure we clean up when done (even when killed, except with -9):
# trap 'cleanup_pidfile_and_exit' INT TERM EXIT

#
# Create the process file or exit if the previous process is still running.
# Will also exit if we cannot write the pidfile, or delete a previously abandoned one.
# In case of a previously abandoned pidfile, we re-launch ourselves in the background.
#
function create_pidfile() {
  mypid=$$
  myscriptname=$(basename "$0" .sh)
  pidfile=$1/$myscriptname.pid
  # Close stderr so no garbage goes into pidfile,
  # then write mypid atomically into the PID file, or fail to write if already there
  $(exec 2>&-; set -o noclobber; echo "$mypid" > "$pidfile")
  # Check if the lockfile is present
  if [ ! -f "$pidfile" ]; then
    # pidfile should exist, if not, we failed to create so to bail out
	echo pidFile does not exist, exiting
    exit 1
  fi
  # Read the pid from the file
  currentpid=$(<"$pidfile")
  # Is the recorded pid me?
  if [ $mypid -ne $currentpid ]; then
    # It is not me. Is the process pid in the lockfile still running?
    is_already_running "$pidfile"
    if [ $? -ne 0 ]; then
      # No.  Kill the pidfile and relaunch ourselves properly.
      rm "$pidfile"
      if [ $? -ne 0 ]; then
        echo "Error: unable to delete pidfile $pidfile" 1>&2
      else
        # fork only if we can delete the pidfile to prevent fork-bomb
        $0 $@ &
      fi
    fi
    # We did not own the pid in the pidfile.
    exit
  fi
}

#
# Clean up the pidfile that we owned and exit
# After creating the pidfile, call this as:
# trap 'cleanup_pidfile_and_exit' INT TERM EXIT
#
function cleanup_pidfile_and_exit() {
  myscriptname=$(basename "$0" .sh)
  pidfile=$1/$myscriptname.pid
  if [ -f "$pidfile" ]; then
    rm "$pidfile"
    if [ $? -ne 0 ]; then
      echo "Error: unable to delete pidfile $pidfile" 1>&2
    fi
  fi
  exit
}

#
# For internal use only
#
# param: the pidfile
# returns boolean 0|1 (1=no, 0=yes)
#
function is_already_running() {
  pidfile="$1"
  if [ ! -f "$pidfile" ]; then
    # pid file does not exist
    return 1 
  fi
  pid=$(<"$pidfile")
  if [ -z "$pid" ]; then
    # pid file did not contain a pid
    return 1
  fi

  # check if a process with this pid exists and is an instance of this script
  previous=$(ps -p $pid | grep $(basename $0))
  if [ "$previous" = "" ]; then
    # There is no such process running, or the pid is not us
    return 1
  else
    return 0
  fi
}