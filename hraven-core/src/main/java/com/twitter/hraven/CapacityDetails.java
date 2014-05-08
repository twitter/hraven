/*
Copyright 2014 Twitter, Inc.

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

package com.twitter.hraven;

import com.twitter.hraven.datasource.ProcessingException;

/**
 * Interface to store and access capacity information
 *
 * Presently stores only pool/queue capacity related numbers
 * Also provides functionality for loading
 * different schedulers currently loads fair scheduler to
 * get hadoop1 and hadoop2 Pool/Queue capacity numbers
 * Can be extended to include other capacity related information
 */

public interface CapacityDetails {

  /** get the value of the capacity attribute in that pool/queue */
  abstract long getAttribute(String queue, String attribute);

  /** load the capacity information from a file */
  abstract void loadDetails(String fileName) throws ProcessingException;

  /** number of records in capacity info */
  abstract int size();

  /** get the type of scheduler being looked at */
  abstract SchedulerTypes getSchedulerType();
}
