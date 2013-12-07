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
package com.twitter.hraven.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.twitter.hraven.Constants;

public class TestHBasePutUtil {
  @Test
  public void testGetLongValueFromPut() {
    Put p = new Put(Bytes.toBytes("key1"));
    byte[] family = Bytes.toBytes("someFamily");
    byte[] qualifier = Bytes.toBytes("someQualifier");
    long expValue = 12345L;
    p.add(family, qualifier, Bytes.toBytes(expValue));
    List<Put> list1 = new LinkedList<Put>();
    list1.add(p);
    long actualValue = HBasePutUtil.getLongValueFromPut(p, family, qualifier);
    assertEquals(expValue, actualValue);

    qualifier = Bytes.toBytes("someQualifier2");
    actualValue = HBasePutUtil.getLongValueFromStringPut(p, family, qualifier);
    assertEquals(0L, actualValue);

    String insValueStr = "abcde";
    p.add(family, qualifier, Bytes.toBytes(insValueStr));
    actualValue = HBasePutUtil.getLongValueFromStringPut(p, family, qualifier);
    assertEquals(Constants.LONG_CONVERSION_ERROR_VALUE, actualValue);
  }

  @Test
  public void testGetLongValueFromStringPut() {
    Put p = new Put(Bytes.toBytes("key1"));
    byte[] family = Bytes.toBytes("someFamily");
    byte[] qualifier = Bytes.toBytes("someQualifier");
    String expValueStr = "12345";
    p.add(family, qualifier, Bytes.toBytes(expValueStr));
    List<Put> list1 = new LinkedList<Put>();
    list1.add(p);
    long actualValue = HBasePutUtil.getLongValueFromStringPut(p, family, qualifier);
    assertEquals(Long.parseLong(expValueStr), actualValue);

    qualifier = Bytes.toBytes("someQualifier2");
    actualValue = HBasePutUtil.getLongValueFromStringPut(p, family, qualifier);
    assertEquals(0L, actualValue);

    String insValueStr = "abcde";
    p.add(family, qualifier, Bytes.toBytes(insValueStr));
    actualValue = HBasePutUtil.getLongValueFromStringPut(p, family, qualifier);
    assertEquals(Constants.LONG_CONVERSION_ERROR_VALUE, actualValue);
  }

  @Test
  public void testGetValueFromPut() {
    Put p = new Put(Bytes.toBytes("key1"));
    byte[] family = Bytes.toBytes("someFamily");
    byte[] qualifier = Bytes.toBytes("someQualifier");
    byte[] expValue = Bytes.toBytes("abcd");
    p.add(family, qualifier, expValue);
    List<Put> list1 = new LinkedList<Put>();
    list1.add(p);
    byte[] actualValue = HBasePutUtil.getValueFromPut(p, family, qualifier);
    assertEquals(Bytes.toString(expValue), Bytes.toString(actualValue));

    qualifier = Bytes.toBytes("someQualifier2");
    actualValue = HBasePutUtil.getValueFromPut(p, family, qualifier);
    assertNull(actualValue);
  }

  @Test
  public void testGetStringValueFromPut() {
    Put p = new Put(Bytes.toBytes("key1"));
    byte[] family = Bytes.toBytes("someFamily");
    byte[] qualifier = Bytes.toBytes("someQualifier");
    String expValue = "abcd";
    p.add(family, qualifier, Bytes.toBytes(expValue));
    List<Put> list1 = new LinkedList<Put>();
    list1.add(p);
    String actualValue = HBasePutUtil.getStringValueFromPut(p, family, qualifier);
    assertEquals(expValue, actualValue);

    qualifier = Bytes.toBytes("someQualifier2");
    actualValue = HBasePutUtil.getStringValueFromPut(p, family, qualifier);
    assertTrue(StringUtils.isBlank(actualValue));
  }
 
}