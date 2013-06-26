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

import org.apache.hadoop.hbase.HBaseConfiguration
import com.twitter.hraven.datasource.JobHistoryService

options = {}
options[:tasks] = false
OptionParser.new do |opts|
  opts.banner = "Usage: ./get_job.rb [options] cluster jobId"

  opts.on("-t", "--tasks", "Include task data") do |t|
    options[:tasks] = t
  end
end.parse!

cluster = ARGV[0]
jobid = ARGV[1]

conf = HBaseConfiguration.create()
service = JobHistoryService.new(conf)

job = service.getJobByJobID(cluster, jobid, options[:tasks])
service.close()

if job.nil?
  puts "No job found for cluster: #{cluster}, jobid: #{jobid}"
else
  puts "Job: #{job.getJobId()}  #{job.getJobName()}  #{job.getStatus()}"
  puts "\tsubmitted: #{job.getSubmitDate()}  launched: #{job.getLaunchDate()}  finished: #{job.getFinishDate()}  runtime: #{job.getRunTime()} ms"
  puts "\tmaps: #{job.getTotalMaps()} (#{job.getFinishedMaps()} finished / #{job.getFailedMaps()} failed)"
  puts "\treduces: #{job.getTotalReduces()} (#{job.getFinishedReduces()} finished / #{job.getFailedReduces()} failed)"
  if options[:tasks]
    puts "Tasks:"
    job.getTasks().each { |task|
      puts "\t#{task.getTaskId()}: #{task.getTaskAttemptId()}  type: #{task.getType()}  status: #{task.getStatus()}"
    }
  end
end


