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
# Used to configure hRaven environment

#   JAVA_HOME        The java implementation to use.  Overrides JAVA_HOME.
# export JAVA_HOME=

#   HBASE_CONF_DIR   Alternate directory from which to pick up hbase configurations. Default is ${HBASE_HOME}/conf.
#                    All other hbase configurations can be set in the standard hbase manner, or supplied here instead.
# export HBASE_CONF_DIR=

#   HADOOP_CONF_DIR  Alternate directory from which to pick up hadoop configurations. Default is ${HADOOP_HOME}/conf.
#                    All other hadoop configurations can be set in the standard hadoop manner, or supplied here instead.
# export HADOOP_CONF_DIR=

# HBASE_CLASSPATH Used in hraven-etl-env.sh
export HBASE_CLASSPATH=`hbase classpath`

# export HRAVEN_CLASSPATH=$HBASE_CLASSPATH
# export HRAVEN_CLASSPATH=`hbase --config /etc/hbase/conf-hbase-tst-dc1 classpath`
export HRAVEN_CLASSPATH=

# The maximum amount of heap to use, in MB. Default is 1000.
# export HRAVEN_HEAPSIZE=1000

# Location for process ID files for any hRaven daemons
export HRAVEN_PID_DIR=/tmp/
