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

/**
 * Base exception representing errors in data retrieval or storage.
 */
public class DataException extends Exception {

  private static final long serialVersionUID = 2406302267896675759L;

  public DataException(String message) {
    super(message);
  }

  public DataException(String message, Throwable cause) {
    super(message, cause);
  }
}
