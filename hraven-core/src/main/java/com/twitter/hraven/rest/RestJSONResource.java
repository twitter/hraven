/*
Copyright 2016 Twitter, Inc.

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

/**
 * Main REST resource that handles binding the REST API to the JobHistoryService.
 *
 * TODO: better prevalidation
 * TODO: handle null results with empty json object or response code
 */
public class RestJSONResource {

  // This is rather bad, because references might trigger classloading on the clients side.
  // Should have been separate class, but leaving in place for API compatibility
  public static final String SLASH = "/" ;

}
