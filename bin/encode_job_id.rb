#!./hraven org.jruby.Main
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
# Show the byte respresentation for a job ID, which is comprised of a cluster identifier and a jobid.
#
# Execute this script from the bin directory like this:
# hraven/bin$ ./encode_job_id.rb [cluster] jobid
#
# Or from anywhere like this:
# hraven$ bin/hraven org.jruby.Main bin/encode_job_id.rb [cluster] jobid

include Java

import com.twitter.hraven.datasource.JobIdConverter
import com.twitter.hraven.datasource.QualifiedJobIdConverter
import com.twitter.hraven.JobId
import com.twitter.hraven.QualifiedJobId

import org.apache.hadoop.hbase.util.Bytes

if ARGV.length == 2
  id = QualifiedJobId.new(ARGV[0], ARGV[1])
  puts Bytes.toStringBinary(QualifiedJobIdConverter.new().toBytes(id))
elsif ARGV.length == 1
  id = JobId.new(ARGV[0])
  puts Bytes.toStringBinary(JobIdConverter.new().toBytes(id))
else
  puts "Usage: encode_job_id.rb [cluster] jobid"
  exit 1
end
