/*
Copyright 2012 Twitter, Inc.

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

/**
 * This exception indicates that a row key could not be parsed successfully.
 */
public class RowKeyParseException extends Exception {

  private static final long serialVersionUID = 839389516279735249L;

  /**
   * @param message
   */
  public RowKeyParseException(String message) {
    super(message);
   }

  /**
   * @param message
   * @param cause
   */
  public RowKeyParseException(String message, Throwable cause) {
    super(message, cause);
  }
}
