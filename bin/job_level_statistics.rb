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
# Groups data in job_history table by either user or user&app and sorts data by different columns.
# For example you can find out the user/app which had most tasks/mappers, wrote most to HDFS...
# Data can be filtered by time (start and end), framework and job status.
# Data can be sorted by one column or by key.
# hbase --config /etc/hbase/conf-hbase org.jruby.Main job_level_statistics.rb -S 0 -c "total_maps total_reduces" -s 1342000000000 -e 1343000000000 -j "SUCCESS" -l 100
#
require 'optparse'
include Java

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.util.Bytes

import com.twitter.hraven.Constants
import com.twitter.hraven.datasource.JobKeyConverter

options = {}
options[:cols] =  ["total_maps", "total_reduces"]
options[:limit] = -1
options[:apps] = false
options[:sort] = nil
options[:framework] = nil
options[:jobstatus] = nil
OptionParser.new do |opts|
    opts.banner = "Usage: ./job_level_statistics.rb [options]"

    opts.on("-c", "--cols N", String, "Select the columns to do statistics on.") do |t|
        options[:cols] = t.split(" ")
    end
    opts.on("-s", "--start START", Integer, "Minumum timestamp (ms) to filter on") do |o|
        options[:start] = o
    end
    opts.on("-e", "--end END", Integer, "Maximum timestamp (ms) to filter on") do |o|
        options[:end] = o
    end
    opts.on("-l", "--limit N", Integer, "Max number of results to return") do |o|
        options[:limit] = o
    end
    opts.on("-a", "--apps", "Get information at application level. The default is user level.") do
        options[:apps] = true
    end
    opts.on("-S", "--Sort N", Integer,
	  "Sort info on this column(0 based).
           Defaults in no sorting (basicaly sorting on user/app). ") do |o|
        options[:sort] = o
    end
    opts.on("-f", "--framework FRAMEWORK", String,
          "The MR framework to filter on (PIG|SCALDING|NONE)") do |o|
        options[:framework] = o
    end
    opts.on("-j", "--jobstatus J", String, "Filter by job_status (SUCCESS|KILLED|FAILED).") do |o|
        options[:jobstatus] = o
    end
    opts.on_tail("-h", "--help", "Show this message") do
        puts opts
        exit
    end

end.parse!

class DataInfo
    attr_accessor :data, :counters, :count
    def initialize ( data, counters, count)
        @data = data
        @counters = Array.new(counters.size)
        counters.each_with_index { |val, index|
            @counters[index] = val
        }
        @count = count
    end
end

date_descriptor = "submit_time"

colnames = options[:cols].join("\t")
colbytes = options[:cols].collect {|x| Bytes.toBytes(x)}
scan = Scan.new
colbytes.each { |x|
    scan.addColumn(Constants::INFO_FAM_BYTES, x)
}
scan.addColumn(Constants::INFO_FAM_BYTES, Constants::FRAMEWORK_COLUMN_BYTES)
scan.addColumn(Constants::INFO_FAM_BYTES, Bytes.toBytes(date_descriptor))
scan.addColumn(Constants::INFO_FAM_BYTES, Bytes.toBytes("job_status"))

config = HBaseConfiguration.create()
historyTable = HTable.new(config, Constants.HISTORY_TABLE_BYTES)

# Filtering
filters = []

if options[:framework] then
    frameworkFilter = SingleColumnValueFilter.new(Constants::INFO_FAM_BYTES,
       						  Constants::FRAMEWORK_COLUMN_BYTES,
						  CompareFilter::CompareOp::EQUAL,
						  Bytes.toBytes(options[:framework]))
    frameworkFilter.setFilterIfMissing(true)
    filters << frameworkFilter
end

if options[:jobstatus] then
    jobstatusFilter = SingleColumnValueFilter.new(Constants::INFO_FAM_BYTES,
 					          Bytes.toBytes("job_status"),
 					          CompareFilter::CompareOp::EQUAL,
 						  Bytes.toBytes(options[:jobstatus]))
   filters << jobstatusFilter
end

if options[:start]
    STDERR.write "Filtering where #{date_descriptor} >= #{options[:start]}\n"
    filter = SingleColumnValueFilter.new(Constants::INFO_FAM_BYTES,
					 Bytes.toBytes(date_descriptor),
                                         CompareFilter::CompareOp::GREATER_OR_EQUAL,
					 Bytes.toBytes(options[:start]))
    filter.setFilterIfMissing(true)
    filters << filter
end
if options[:end]
    STDERR.write "Filtering where #{date_descriptor} < #{options[:end]}\n"
    filter = SingleColumnValueFilter.new(Constants::INFO_FAM_BYTES,
    					 Bytes.toBytes(date_descriptor),
                                         CompareFilter::CompareOp::LESS,
					 Bytes.toBytes(options[:end]))
    filter.setFilterIfMissing(true)
    filters << filter
end

filterList = FilterList.new(filters)
scan.setFilter(filterList)
scanner = historyTable.getScanner(scan)

keyConv = JobKeyConverter.new

lastkey = nil
info = [] # Used for storing sums for user/app and then for sorting.
sums = Array.new(colbytes.size ,0) # Keeps the sum for a user/app for each of the columns.
count = 0  # Keeps the number of jobs for each app.

scanner.each_with_index { |result, rowcnt| # Grouping rows by user/app and calculating the sums.
    break if result.nil? || result.isEmpty
    key = keyConv.fromBytes(result.getRow())
    keyfields = "#{key.getUserName()}"  # user level data
    if options[:apps] == true then
        keyfields = "#{key.getUserName()}\t#{key.getAppId()}" #app level data
    end

    if keyfields == lastkey then   # same user/app so we just add to the sums.
        # For each user defined column we add the value to the sums for this user/app.
        colbytes.each_with_index { |x, poz|
            val = result.getValue(Constants::INFO_FAM_BYTES, x)
            next if val == nil
            sums[poz] = sums[poz] + Bytes.toLong(val)
        }
        count = count + 1
        next
    end
    # If the last user/app had any valid data add them to the data.
    info.push(DataInfo.new(lastkey, sums, count)) if count != 0
    count = 0 # Then we reset everything to 0.
    sums.each_with_index{ |x, poz|
        sums[poz] = 0
    }

    colbytes.each_with_index { |x, poz|
        val = result.getValue(Constants::INFO_FAM_BYTES, x)
        next if val == nil
        sums[poz] = sums[poz] + Bytes.toLong(val)
    }
    count = count + 1
    lastkey = keyfields

}

if count != 0 then
    info.push(DataInfo.new(lastkey, sums, count))
end

#Sorting the data.
if options[:sort]  then
    #By a single column.
    tempinfo = info.sort_by{ |a|
        -a.counters[options[:sort]]
    }
    info =tempinfo
end

limit = options[:limit] || -1
STDERR.write "Limiting number of rows returned to #{limit}\n" if limit > -1

#Printing out the data
print "User/app "
options[:cols].each { |col|
    print col.to_s + " "
    }
puts "count"
info.each_with_index { |job, count|
    break if count >= limit
    print "#{job.data} : "
    job.counters.each{ |counter|
       print counter.to_s + " "
    }
       puts job.count.to_s
}
