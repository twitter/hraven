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

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.twitter.hraven.Constants;

public class HBasePutUtil {

  private static final Log LOG = LogFactory.getLog(HBasePutUtil.class);

  /**
   * get value as long from put
   * 
   * @param hbase Put
   * @param column family
   * @param column qualifier
   * 
   * value as long,
   * -1 if it does not exist
   */
  public static long getLongValueFromPut(Put p, byte[] family, byte[] qualifier) {
    if (p.has(family, qualifier)) {
      List<KeyValue> kv = p.get(family, qualifier);
      if (kv.size() > 0) {
        // there would be only one such key value
        byte[] v = kv.get(0).getValue();
        if (v != null) {
          try {
            return Bytes.toLong(v);
          } catch (NumberFormatException nfe) {
            LOG.error("Could not convert " + Bytes.toString(qualifier)
                + " to long, this should not happen!" + nfe.getMessage());
            nfe.printStackTrace();
            // return -1 to indicate conversion error, which shouldn't really happen
            return Constants.LONG_CONVERSION_ERROR_VALUE;
          }
        }
      }
    }
    // return 0 if qualifier not found
    return 0L;
  }

  /**
   * get value as long from a string in the put, -1 if it does not exist
   */
  public static long getLongValueFromStringPut(Put p, byte[] family, byte[] qualifier) {
    if (p.has(family, qualifier)) {
      List<KeyValue> kv = p.get(family, qualifier);
      if (kv.size() > 0) {
        // there would be only one such key value
        byte[] v = kv.get(0).getValue();
        if (v != null) {
          try {
            String v1 = Bytes.toString(v);
            return Long.parseLong(v1);
          } catch (NumberFormatException nfe) {
            LOG.error("Could not convert " + Bytes.toString(qualifier)
                + " to long, this should not happen!" + nfe.getMessage());
            nfe.printStackTrace();
            // return -1 to indicate conversion error, which shouldn't really happen
            return Constants.LONG_CONVERSION_ERROR_VALUE;
          }
        }
      }
    }
    // return 0 if qualifier not found
    return 0L;
  }

  /**
   * get byte array value from put
   * null if it doesn't exist
   */
  public static byte[] getValueFromPut(Put p, byte[] family, byte[] qualifier) {
    if (p.has(family, qualifier)) {
      List<KeyValue> kv = p.get(family, qualifier);
      if (kv.size() > 0) {
        // there would be only one such key value
        return (kv.get(0).getValue());
      }
    }
    return null;
  }

  /**
   * get value as long from a string in the put,
   * return empty string if it does not exist
   */
  public static String getStringValueFromPut(Put p, byte[] family, byte[] qualifier) {
    if (p.has(family, qualifier)) {
      List<KeyValue> kv = p.get(family, qualifier);
      if (kv.size() > 0) {
        // there would be only one such key value
        byte[] v = kv.get(0).getValue();
        if (v != null) {
          return Bytes.toString(v);
        }
      }
    }
    // return empty string if family/qualifier not found
    return "";
  }

}