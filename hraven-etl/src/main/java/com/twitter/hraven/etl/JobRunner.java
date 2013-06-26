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
package com.twitter.hraven.etl;

import java.util.concurrent.Callable;

import org.apache.hadoop.mapreduce.Job;

/**
 * Can be used to run a single Hadoop job. The {@link #call()} method will block
 * until the job is complete and will return a non-null return value indicating
 * the success of the Hadoop job.
 */
public class JobRunner implements Callable<Boolean> {

  private volatile boolean isCalled = false;
  private final Job job;

  /**
   * Post processing step that gets called upon successful completion of the
   * Hadoop job.
   */
  private final Callable<Boolean> postProcessor;

  /**
   * Constructor
   * 
   * @param job
   *          to job to run in the call method.
   * @param postProcessor
   *          Post processing step that gets called upon successful completion
   *          of the Hadoop job. Can be null, in which case it will be skipped.
   *          Final results will be the return value of this final processing
   *          step.
   */
  public JobRunner(Job job, Callable<Boolean> postProcessor) {
    this.job = job;
    this.postProcessor = postProcessor;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.util.concurrent.Callable#call()
   */
  @Override
  public Boolean call() throws Exception {

    // Guard to make sure we get called only once.
    if (isCalled) {
      return false;
    } else {
      isCalled = true;
    }

    if (job == null) {
      return false;
    }

    boolean success = false;
    // Schedule the job on the JobTracker and wait for it to complete.
    try {
     success = job.waitForCompletion(true);
    } catch (InterruptedException interuptus) {
      // We're told to stop, so honor that.
      // And restore interupt status.
      Thread.currentThread().interrupt();
      // Indicate that we should NOT run the postProcessor.
      success = false;
    }
    
    if (success && (postProcessor != null)) {
      success = postProcessor.call();
    }

    return success;
  }

}
