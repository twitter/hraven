/*
Copyright 2013 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.twitter.hraven.hadoopJobMonitor.metrics;

import javax.xml.bind.annotation.adapters.XmlAdapter;

import org.apache.hadoop.metrics2.lib.MutableCounterLong;

/**
 * Enables to directly convert the Metrics classes to XML
 */
public class MutableCounterLongAdapter  extends
XmlAdapter<Long, MutableCounterLong>{

  /**
   * I care only about XML generation from metric, so this method is not used
   */
  @Override
  public MutableCounterLong unmarshal(Long v) throws Exception {
    return null;
  }

  @Override
  public Long marshal(MutableCounterLong v) throws Exception {
    return v.value();
  }


}
