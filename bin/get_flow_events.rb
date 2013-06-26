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

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.hadoop.hbase.HBaseConfiguration
import com.twitter.hraven.datasource.FlowEventService
import com.twitter.hraven.FlowKey

options = {}
options[:follow] = false
options[:sleepdelay] = 5
OptionParser.new do |opts|
  opts.banner = "Usage: ./get_flow_events.rb [options] cluster user app runtimestamp"

  opts.on("-f", "--follow", "Poll for new events") do |t|
    options[:follow] = t
  end
  opts.on("-s", "--sleep N", Integer, "Wait N seconds between attempts when polling (defaults to 5)") do |n|
    options[:delay] = n
  end

end.parse!

DF = SimpleDateFormat.new("MM-dd-yyyy HH:mm:ss")
def show_events(events)
  events.each{ |e|
    eventTime = DF.format(e.getTimestamp())
    puts "#{e.getFlowEventKey().getSequence()}: #{eventTime} type=#{e.getType()} data=#{e.getEventDataJSON()}"
  }
end

conf = HBaseConfiguration.create
service = FlowEventService.new(conf)

cluster = ARGV[0]
user = ARGV[1]
app = ARGV[2]
runts = ARGV[3]

fk = FlowKey.new(cluster, user, app, runts.to_i)

begin
  events = service.getFlowEvents(fk)
  show_events(events)
  
  if options[:follow]
    last_e = nil
    while true
      sleep options[:sleepdelay]
      puts "..."
      # continue from last event
      if events.size() > 0
        last_e = events.get(events.size()-1)
      end
      if !last_e.nil?
        events = service.getFlowEventsSince(last_e.getFlowEventKey())
      else
        # no events seen yet
        events = service.getFlowEvents(fk)
      end
      show_events(events)
    end
  end
ensure
  service.close() unless service.nil?
end
