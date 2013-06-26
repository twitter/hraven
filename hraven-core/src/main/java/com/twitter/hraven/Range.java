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
package com.twitter.hraven;

/**
 * A range of (sorted) items with a min and a max.
 */
public class Range<E> {

  /**
   * The minimum item of class {@link E} in this range.
   */
  private final E min;

  /**
   * The maximum item of class {@link E} in this range.
   */
  private final E max;

  /**
   * Constructs a range
   * 
   * @param min
   *          the minimum of this range
   * @param max
   *          the maximum of this range
   */
  public Range(E min, E max) {
    this.min = min;
    this.max = max;
  }

  /**
   * @return the min of the range
   */
  public E getMin() {
    return min;
  }

  /**
   * @return the max of the range.
   */
  public E getMax() {
    return max;
  }

}
