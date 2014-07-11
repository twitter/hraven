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
# Used to configure hraven-etl environment

home=$(dirname $0)
source $home/../../conf/hraven-env.sh
source $home/pidfiles.sh

#check if hraven-core.jar and hraven-etl.jar exist
#if not, create symbolic links to the needed jars
libhraven=`cd $(dirname $0)/../../lib;pwd;`
if [ ! -f $libhraven/hraven-core.jar ]; then
    coreLastVersion=`ls $libhraven/hraven-core-*.jar | sort -V | tail -1`
    ln -s $coreLastVersion $libhraven/hraven-core.jar
    echo "hraven-core.jar linked to $coreLastVersion"
fi
if [ ! -f $libhraven/hraven-etl.jar ]; then
    etlLastVersion=`ls $libhraven/hraven-etl-*.jar | sort -V | tail -1`
    ln -s $etlLastVersion $libhraven/hraven-etl.jar
    echo "hraven-etl.jar linked to $etlLastVersion"
fi

# set the hraven-core jar as part of libjars and hadoop classpath
# set this here because it only pertains to the etl logic
export LIBJARS=$home/../../lib/hraven-core.jar
export HADOOP_CLASSPATH=$home/../../lib/*:$LIBJARS:$HBASE_CLASSPATH
hravenEtlJar=$home/../../lib/hraven-etl.jar
