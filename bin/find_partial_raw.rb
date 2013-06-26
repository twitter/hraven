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
# Prints out incomplete records from the dev.job_history table
# Incomplete records have no 'jobid' column, indicating that 
# the job history file has not been loaded
#

include Java

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.util.Bytes

import com.twitter.hraven.Constants
import com.twitter.hraven.datasource.QualifiedJobIdConverter


c = HBaseConfiguration.create()
historyTable = HTable.new(c, Constants.HISTORY_RAW_TABLE_BYTES)

scan = Scan.new
# by filtering to return only empty job conf or history and _not_ setting filter if missing, we should only get rows missing raw fields
filterList = FilterList.new(FilterList::Operator::MUST_PASS_ONE)
filterList.addFilter(SingleColumnValueFilter.new(Constants::RAW_FAM_BYTES, Constants::JOBCONF_COL_BYTES, CompareFilter::CompareOp::EQUAL, Constants::EMPTY_BYTES))
filterList.addFilter(SingleColumnValueFilter.new(Constants::RAW_FAM_BYTES, Constants::JOBHISTORY_COL_BYTES, CompareFilter::CompareOp::EQUAL, Constants::EMPTY_BYTES))
scan.setFilter(filterList)

scanner = historyTable.getScanner(scan)
rowcnt = 0
keyConv = QualifiedJobIdConverter.new

scanner.each { |result|
  break if result.nil? || result.isEmpty
  rowcnt += 1
  rowkey = Bytes.toStringBinary(result.getRow())
  jobid = keyConv.fromBytes(result.getRow())
  puts "#{rowkey}\t#{jobid}"
}

puts "Found #{rowcnt} matching jobs"
