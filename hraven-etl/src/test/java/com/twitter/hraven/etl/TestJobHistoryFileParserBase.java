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
package com.twitter.hraven.etl;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

import com.twitter.hraven.Constants;
import com.twitter.hraven.datasource.ProcessingException;

public class TestJobHistoryFileParserBase {

  @Test(expected=ProcessingException.class)
  public void testIncorrectGetXmxValue(){
    String xmxValue = "-XmxSOMETHINGWRONG!";
    @SuppressWarnings("unused")
    long val = JobHistoryFileParserBase.getXmxValue(xmxValue);
  }

  @Test
  public void testNullGetXmxValue(){
    String xmxValue = null;
    Long val = JobHistoryFileParserBase.getXmxValue(xmxValue);
    assertEquals(Constants.DEFAULT_XMX_SETTING, val);
  }

  @Test
  public void testGetXmxValue(){
    // check for megabyte value itself
    String xmxValue = "-Xmx500m";
    long expValue = 500;
    long actualValue = JobHistoryFileParserBase.getXmxValue(xmxValue);
    assertEquals(expValue, actualValue);
    long totalValue = JobHistoryFileParserBase.getXmxTotal(actualValue);
    assertEquals(666L, totalValue);

    // check if megabytes is returned for kilobytes
    xmxValue = "-Xmx2048K";
    actualValue = JobHistoryFileParserBase.getXmxValue(xmxValue);
    expValue = 2L;
    assertEquals(expValue, actualValue);
    totalValue = JobHistoryFileParserBase.getXmxTotal(actualValue);
    long expTotalVal = 2L;
    assertEquals(expTotalVal, totalValue);

    // check if megabytes is returned for gigabytes
    xmxValue = "-Xmx2G";
    actualValue = JobHistoryFileParserBase.getXmxValue(xmxValue);
    expValue = 2048;
    assertEquals(expValue, actualValue);
    totalValue = JobHistoryFileParserBase.getXmxTotal(actualValue);
    expTotalVal = 2730L;
    assertEquals(expTotalVal, totalValue);

    // what happens whene there are 2 Xmx settings,
    // picks the first one
    xmxValue = "-Xmx2G -Xms 1G -Xmx4G";
    actualValue = JobHistoryFileParserBase.getXmxValue(xmxValue);
    expValue = 2048;
    assertEquals(expValue, actualValue);
    totalValue = JobHistoryFileParserBase.getXmxTotal(actualValue);
    expTotalVal = 2730L;
    assertEquals(expTotalVal, totalValue);

    // check if megabytes is returned for bytes
    xmxValue = "-Xmx2097152";
    actualValue = JobHistoryFileParserBase.getXmxValue(xmxValue);
    expValue = 2L;
    assertEquals(expValue, actualValue);
    totalValue = JobHistoryFileParserBase.getXmxTotal(actualValue);
    expTotalVal = 2L;
    assertEquals(expTotalVal, totalValue);

    xmxValue = " -Xmx1024m -verbose:gc -Xloggc:/tmp/@taskid@.gc" ;
    actualValue = JobHistoryFileParserBase.getXmxValue(xmxValue);
    expValue = 1024L;
    assertEquals(expValue, actualValue);
    totalValue = JobHistoryFileParserBase.getXmxTotal(actualValue);
    expTotalVal = 1365L;
    assertEquals(expTotalVal, totalValue);
  }

  @Test
  public void testExtractXmxValue() {
    String jc = " -Xmx1024m -verbose:gc -Xloggc:/tmp/@taskid@.gc" ;
    String valStr = JobHistoryFileParserBase.extractXmxValueStr(jc);
    String expStr = "1024m";
    assertEquals(expStr, valStr);
  }

  @Test
  public void testExtractXmxValueIncorrectInput(){
    String jc = " -Xmx" ;
    String valStr = JobHistoryFileParserBase.extractXmxValueStr(jc);
    String expStr = Constants.DEFAULT_XMX_SETTING_STR;
    assertEquals(expStr, valStr);
  }

  @Test(expected=ProcessingException.class) 
  public void testGetXmxValueIncorrectInput2(){
    String jc = " -Xmx1024Q" ;
    @SuppressWarnings("unused")
    Long value = JobHistoryFileParserBase.getXmxValue(jc);
  }
}

