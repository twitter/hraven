/**
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

package com.twitter.hraven.mapreduce;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/*
 * This is based on ContextFactory.java from hadoop-2.0.x sources.
 */

/**
 * Utility methods to allow applications to deal with inconsistencies between MapReduce Context
 * Objects API between Hadoop 1.x and 2.x.
 */
public class HadoopCompat {

  private static final boolean useV21;
  private static final Constructor<?> GENERIC_COUNTER_CONSTRUCTOR;
  private static final Method GET_COUNTER_METHOD;
  private static final Method INCREMENT_COUNTER_METHOD;

  static {
    boolean v21 = true;
    final String PACKAGE = "org.apache.hadoop.mapreduce";
    try {
      Class.forName(PACKAGE + ".task.JobContextImpl");
    } catch (ClassNotFoundException cnfe) {
      v21 = false;
    }
    useV21 = v21;
    Class<?> genericCounterCls;
    try {
      if (v21) {
        genericCounterCls = Class.forName(PACKAGE + ".counters.GenericCounter");
      } else {
        genericCounterCls = Class.forName("org.apache.hadoop.mapred.Counters$Counter");
      }
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Can't find class", e);
    }
    try {
      GENERIC_COUNTER_CONSTRUCTOR =
          genericCounterCls.getDeclaredConstructor(String.class, String.class, Long.TYPE);
      GENERIC_COUNTER_CONSTRUCTOR.setAccessible(true);

      if (useV21) {
        Method get_counter;
        try {
          get_counter =
              Class.forName(PACKAGE + ".TaskAttemptContext").getMethod("getCounter", String.class,
                String.class);
        } catch (Exception e) {
          get_counter =
              Class.forName(PACKAGE + ".TaskInputOutputContext").getMethod("getCounter",
                String.class, String.class);
        }
        GET_COUNTER_METHOD = get_counter;
      } else {
        GET_COUNTER_METHOD =
            Class.forName(PACKAGE + ".TaskInputOutputContext").getMethod("getCounter",
              String.class, String.class);
      }
      INCREMENT_COUNTER_METHOD =
          Class.forName(PACKAGE + ".Counter").getMethod("increment", Long.TYPE);
    } catch (SecurityException e) {
      throw new IllegalArgumentException("Can't run constructor ", e);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("Can't find constructor ", e);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Can't find class", e);
    }
  }

  /**
   * True if runtime Hadoop version is 2.x, false otherwise.
   */
  public static boolean isVersion2x() {
    return useV21;
  }

  /**
   * @return with Hadoop 2 : <code>new GenericCounter(args)</code>,<br>
   *         with Hadoop 1 : <code>new Counter(args)</code>
   */
  public static Counter newGenericCounter(String name, String displayName, long value) {
    try {
      return (Counter) GENERIC_COUNTER_CONSTRUCTOR.newInstance(name, displayName, value);
    } catch (InstantiationException e) {
      throw new IllegalArgumentException("Can't instantiate Counter", e);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Can't instantiate Counter", e);
    } catch (InvocationTargetException e) {
      throw new IllegalArgumentException("Can't instantiate Counter", e);
    }
  }

  /**
   * Invokes a method and rethrows any exception as runtime excetpions.
   */
  private static Object invoke(Method method, Object obj, Object... args) {
    try {
      return method.invoke(obj, args);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Can't invoke method " + method.getName(), e);
    } catch (InvocationTargetException e) {
      throw new IllegalArgumentException("Can't invoke method " + method.getName(), e);
    }
  }


  /**
   * Invoke getCounter() on TaskInputOutputContext. Works with both Hadoop 1 and 2.
   */
  public static Counter getCounter(TaskInputOutputContext context, String groupName,
      String counterName) {
    return (Counter) invoke(GET_COUNTER_METHOD, context, groupName, counterName);
  }

  /**
   * Increment the counter. Works with both Hadoop 1 and 2
   */
  public static void incrementCounter(Counter counter, long increment) {
    invoke(INCREMENT_COUNTER_METHOD, counter, increment);
  }
}