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
package com.twitter.hraven.rest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Container class that maintains a set of results that can be used for
 * retrieving results in a paginated fashion
 */

public class PaginatedResult<T> {

  // the start row for the next page of results
  // if no more results are remaining, this will be null
  private byte[] nextStartRow;

  // the number of results to be returned per call
  private int limit;

  // request parameters & values
  private Map<String,String> requestParameters;

  // actual values that are to be returned
  private List<T> values;

  // basic constructor
  public PaginatedResult() {
    values = new ArrayList<T>();
    requestParameters = new HashMap<String, String>();
    // set the next start row to null
    // this helps the UI to know that there is no next page
    this.setNextStartRow(null);
    limit = 0;
  }

  // constructor with limit
  public PaginatedResult(int limit) {
    values = new ArrayList<T>();
    requestParameters = new HashMap<String, String>();
    this.limit = limit;
    // set the next start row to null
    // this helps the UI to know that there is no next page
    this.setNextStartRow(null);
  }

  public List<T> getValues() {
    return values;
  }

  public void setValues(List<T> inputValues) {
    this.values = inputValues;
  }

  public void addValue(T value) {
    this.values.add(value);
  }

  public byte[] getNextStartRow() {
    return nextStartRow;
  }

  public void setNextStartRow(byte[] nextStartRow) {
    this.nextStartRow = nextStartRow;
  }

  public int getLimit() {
    return limit;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  public Map<String, String> getRequestParameters() {
    return requestParameters;
  }

  public void setRequestParameters(Map<String, String> requestParameters) {
    this.requestParameters = requestParameters;
  }

  public void addRequestParameter(String param, String value) {
    this.requestParameters.put(param, value);
  }
}
