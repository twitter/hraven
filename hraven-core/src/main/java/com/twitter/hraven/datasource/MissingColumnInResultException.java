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
package com.twitter.hraven.datasource;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Indicates that the {@link Result} from a {@link Scan} is missing an expected
 * column.
 * <p>
 * Specifically, this exception indicates that the {@link KeyValue} returned by
 * {@link Result#getColumnLatest(byte[], byte[])} is <code>null</code> or the
 * list returned by {@link Result#getColumn(byte[], byte[]) is empty.
 */
public class MissingColumnInResultException extends Exception {

  private static final long serialVersionUID = 2561802650466866719L;

  
  private final byte [] family;
  private final byte [] qualifier;
  
  /**
   * Constructs an exception indicating that the specified column
   * @param family
   * @param qualifier
   */
  public MissingColumnInResultException(byte [] family, byte [] qualifier) {
    super("Missing column: " + Bytes.toString(qualifier) + " from column family: "
        + Bytes.toString(family));
    this.family = family;
    this.qualifier = qualifier;
  }

  /**
   * @return the family for which a column was missing.
   */
  public byte[] getFamily() {
    return family;
  }

  /**
   * @return the qualifier indicating which column was missing.
   */
  public byte[] getQualifier() {
    return qualifier;
  }
  
  

}
