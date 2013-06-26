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
# Outputs a single job's details
#
require 'optparse'
include Java

import java.lang.System
import org.apache.hadoop.hbase.HBaseConfiguration
import com.twitter.hraven.datasource.JobHistoryRawService
import com.twitter.hraven.QualifiedJobId

options = {}

OptionParser.new do |opts|
  opts.banner = "Usage: ./get_raw.rb [options] cluster jobId"

  opts.on("-t", "--type TYPE", [:conf, :history], "Raw field to output (conf, history)") do |t|
    options[:type] = t
  end
  opts.on("-f", "--file [FILENAME]", "Write the raw field to the file FILENAME") do |f|
    options[:filename] = f
  end
end.parse!

cluster = ARGV[0]
jobid = ARGV[1]

qualifiedId = QualifiedJobId.new(cluster, jobid)

conf = HBaseConfiguration.create()
service = JobHistoryRawService.new(conf)


if options[:type] == :conf
  rawConf = service.getRawJobConfiguration(qualifiedId)
  if rawConf.nil?
    puts "No job configuration found for #{qualifiedId}"
    exit 1
  end
  rawConf.writeXml(System.out)
elsif options[:type] == :history
  rawHistory = service.getRawJobHistory(qualifiedId)
  if rawHistory.nil?
    puts "No job history found for #{qualifiedId}"
    exit 1
  end
  puts rawHistory
end

service.close()
