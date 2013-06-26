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
# Prints out records from the dev.job_history table for a given framework. Optionally, filters
# can be passed and specific columnscan be requested. To see full usage run this wihtout arguments:
#
# $ hbase org.jruby.Main bin/find_framework.rb
#
# To point to a non-default HBase cluster pass --config:
#
# $ hbase --config /etc/hbase/conf-hbase-dc1 org.jruby.Main bin/find_framework.rb
#

include Java

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.util.Bytes

import com.twitter.hraven.Constants

require 'optparse'

options = { :null_number => "", :null_string => ""}

opts = OptionParser.new do |opts|
  opts.banner = "Usage: ./find_framework.rb -f [framework] <options>"

  opts.on("-f", "--framework FRAMEWORK", String,
          "The MR framework to filter on (PIG|SCALDING|NONE)") do |o|
    options[:framework] = o
  end
  opts.on("-l", "--limit N", Integer, "Max number of results to return") do |o|
    options[:limit] = o
  end
  opts.on("-c", "--columns COLUMNS", String, "List of column descriptors to return (comma-delimited)") do |o|
    options[:columns] = o.split(",")
  end
  opts.on("-s", "--start START", Integer, "Minumum timestamp (ms) to filter on") do |o|
    options[:start] = o
  end
  opts.on("-e", "--end END", Integer, "Maximum timestamp (ms) to filter on") do |o|
    options[:end] = o
  end
  opts.on("-N", "--null_number STRING", "Value to output when a null number is found (default is empty string)") do |o|
    options[:null_number] = o
  end
  opts.on("-S", "--null_string STRING", "Value to output when a null string is found (default is empty string)") do |o|
    options[:null_string] = o
  end
  opts.on_tail("-h", "--help", "Show this message") do
    puts opts
    exit
  end
end

opts.parse!(ARGV)

# TODO: there must be a better way to specify a required options
if !options[:framework]
  puts "Missing required field: framework"
  puts opts
  exit
end

framework = options[:framework]
limit = options[:limit] || -1

STDERR.write "Limiting number of rows returned to #{limit}\n" if limit > -1

if framework == "SCALDING"
  cols = options[:columns] ||
    ["c!scalding.flow.submitted.timestamp", "total_maps", "total_reduces",
     "g!FileSystemCounters!HDFS_BYTES_READ", "gm!org.apache.hadoop.mapred.Task$Counter!MAP_OUTPUT_BYTES",
     "c!cascading.app.id", "c!cascading.app.name", "c!cascading.app.version",
     "c!cascading.flow.id", "c!cascading.app.id", "c!mapred.job.name", "c!scalding.flow.class.signature"]
elsif framework == "NONE"
  cols = options[:columns] || ["c!mapred.job.name", "c!batch.desc"]
elsif framework == "PIG"
  cols = options[:columns] ||
    ["c!pig.script.submitted.timestamp", "total_maps", "total_reduces",
     "g!FileSystemCounters!HDFS_BYTES_READ", "gm!org.apache.hadoop.mapred.Task$Counter!MAP_OUTPUT_BYTES",
     "gr!FileSystemCounters!FILE_BYTES_READ",
     "g!org.apache.hadoop.mapred.JobInProgress$Counter!SLOTS_MILLIS_MAPS",
     "g!org.apache.hadoop.mapred.JobInProgress$Counter!SLOTS_MILLIS_REDUCES", "job_status",
     "c!mapred.job.name", "c!batch.desc"]
else
  puts "Unknown framework: #{framework}, must be one of PIG, SCALDING, NONE"
  exit 1
end

date_descriptor = "submit_time"
# we need to fetch date if date range is passed and that column isn't requested
if (options[:start] || options[:end])
  cols << date_descriptor if !cols.include?(date_descriptor)
end

colnames = cols.join("\t")
colbytes = cols.collect {|x| Bytes.toBytes(x)}

c = HBaseConfiguration.create()
historyTable = HTable.new(c, Constants.HISTORY_TABLE_BYTES)

scan = Scan.new
colbytes.each { |x|
  scan.addColumn(Constants::INFO_FAM_BYTES, x)
}
scan.addColumn(Constants::INFO_FAM_BYTES, Constants::FRAMEWORK_COLUMN_BYTES)

# by filtering to return only empty job IDs and _not_ setting filter if missing, we should only get rows missing jobid
frameworkFilter = SingleColumnValueFilter.new(Constants::INFO_FAM_BYTES,
    Constants::FRAMEWORK_COLUMN_BYTES, CompareFilter::CompareOp::EQUAL, Bytes.toBytes(framework))
frameworkFilter.setFilterIfMissing(true)

filters = [frameworkFilter]
STDERR.write "Filtering where framework = #{framework}\n"

# date range filters
if options[:start]
  STDERR.write "Filtering where #{date_descriptor} >= #{options[:start]}\n"
  filter = SingleColumnValueFilter.new(Constants::INFO_FAM_BYTES, Bytes.toBytes(date_descriptor),
            CompareFilter::CompareOp::GREATER_OR_EQUAL, Bytes.toBytes(options[:start]))
  filter.setFilterIfMissing(true)
  filters << filter
end
if options[:end]
  STDERR.write "Filtering where #{date_descriptor} < #{options[:end]}\n"
  filter = SingleColumnValueFilter.new(Constants::INFO_FAM_BYTES, Bytes.toBytes(date_descriptor),
            CompareFilter::CompareOp::LESS, Bytes.toBytes(options[:end]))
  filter.setFilterIfMissing(true)
  filters << filter
end

filterList = FilterList.new(filters)
scan.setFilter(filterList)

scanner = historyTable.getScanner(scan)
rowcnt = 0

puts "#{colnames}\trowkey"
scanner.each { |result|
  break if result.nil? || result.isEmpty || (limit > 0 && rowcnt >= limit)
  rowcnt += 1
  colindex = 0
  vals = colbytes.collect { |x|
    val = result.getValue(Constants::INFO_FAM_BYTES, x)
    if cols[colindex].start_with?("g!") ||
          cols[colindex].start_with?("gm!") ||
          cols[colindex].start_with?("gr!") ||
          cols[colindex].start_with?("total")
      if val.nil?
        valstr = options[:null_number]
      else
        valstr = Bytes.toLong(val)
      end
    else
      if val.nil?
        valstr = options[:null_string]
      else
        valstr = Bytes.toString(val)
      end
    end
    colindex += 1
    valstr
  }

  rowkey = Bytes.toStringBinary(result.getRow())
  values = vals.join("\t")
  puts "#{values}\t#{rowkey}"
}

STDERR.write "rows found: #{rowcnt}\n"
