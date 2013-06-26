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
# Outputs the most recent flow for the given user and appId
#
require 'optparse'
include Java

import java.util.Date
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.Bytes
import com.twitter.hraven.datasource.JobHistoryService
import com.twitter.hraven.datasource.JobKeyConverter
import com.twitter.hraven.rest.ObjectMapperProvider

options = {}
options[:tasks] = false
options[:limit] = 1
options[:revision] = nil
options[:json] = false
OptionParser.new do |opts|
  opts.banner = "Usage: get_flow.rb [options] cluster user app"

  opts.on("-d", "--detail", "Include job details") do |d|
    options[:detail] = True
  end
  opts.on("-l", "--limit N", Integer, "Return up to N flows (defaults to 1)") do |n|
    options[:limit] = n
  end
  opts.on("-r", "--revision [REV]", "Only match the given application version") do |r|
    options[:revision] = r
  end
  opts.on("-j", "--json", "Print retrieved flow in JSON format") do |j|
    options[:json] = j
  end
  opts.on("-s", "--start [TIME]", Integer, "Start time (in millis since epoch)") do |s|
    options[:starttime] = s
  end
  opts.on("-e", "--end [TIME]", Integer, "End time (in millis since epoch)") do |e|
    options[:endtime] = e
  end
end.parse!

def print_json(flows)
  mapper = ObjectMapperProvider.createCustomMapper
  flows_json = mapper.writeValueAsString(flows)
  puts flows_json
end

def print_text(flows, includeJobs)
  keyConv = JobKeyConverter.new
  flowcnt = 0
  flows.each { |flow|
    flowcnt += 1
    puts "Flow #{flowcnt}: #{flow.getAppId()}, run by #{flow.getUserName()} at #{Date.new(flow.getRunId())} (#{flow.getRunId}),  #{flow.getJobs().size()} jobs"
    puts
    if includeJobs
      jobcnt = 0
      flow.getJobs().each { |job|
        jobcnt += 1
        puts "Job #{jobcnt}: #{job.getJobId()}  #{job.getJobName()}  #{job.getStatus()}"
        puts "\tkey: #{Bytes.toStringBinary(keyConv.toBytes(job.getJobKey()))}"
        puts "\tsubmitted: #{job.getSubmitDate()}  launched: #{job.getLaunchDate()}  finished: #{job.getFinishDate()}  runtime: #{job.getRunTime()} ms"
        puts "\tmaps: #{job.getTotalMaps()} (#{job.getFinishedMaps()} finished / #{job.getFailedMaps()} failed)"
        puts "\treduces: #{job.getTotalReduces()} (#{job.getFinishedReduces()} finished / #{job.getFailedReduces()} failed)"
        puts
      }
    end
  }
end

cluster = ARGV[0]
user = ARGV[1]
app = ARGV[2]

conf = HBaseConfiguration.create()
#conf.set('hbase.client.scanner.caching', '1000')
service = JobHistoryService.new(conf)

flows = service.getFlowTimeSeriesStats(cluster, user, app, options[:revision], options[:starttime], options[:endtime], options[:limit], nil)
service.close()

if flows.nil?
  puts "No flows found for user: #{user}, app: #{app}"
else
  if options[:json]
    print_json(flows)
  else
    print_text(flows, options[:detail])
  end
end


