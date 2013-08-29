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
package com.twitter.hraven;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(
  include=JsonSerialize.Inclusion.NON_NULL
)
public class CounterMap implements Iterable<Counter> {
  private final Map<String,Map<String,Counter>> internalMap = new HashMap<String,Map<String,Counter>>();

  public Set<String> getGroups() {
    return internalMap.keySet();
  }

  public Map<String,Counter> getGroup(String group) {
    return internalMap.get(group);
  }

  public Counter getCounter(String group, String name) {
    Map<String,Counter> groupCounters = getGroup(group);
    if (groupCounters != null) {
      return groupCounters.get(name);
    }

    return null;
  }

  public Counter add(Counter counter) {
    Map<String,Counter> groupCounters = internalMap.get(counter.getGroup());
    if (groupCounters == null) {
      groupCounters = new HashMap<String, Counter>();
      internalMap.put(counter.getGroup(), groupCounters);
    }
    return groupCounters.put(counter.getKey(), counter);
  }

  public void addAll(Iterable<Counter> counters) {
    if (counters != null) {
      for (Counter c : counters) {
        add(c);
      }
    }
  }

  /**
   * Returns an iterator over all the contained {@link Counter} instances for all groups.
   * Note that the {@code remove()} operation <emphasis>is not supported</emphasis> in the returned
   * {@code Iterator}.
   * @return
   */
  @Override
  public Iterator<Counter> iterator() {
    return new Iterator<Counter>() {
      private Iterator<Map.Entry<String,Map<String,Counter>>> groupIter =
          internalMap.entrySet().iterator();
      private Iterator<Map.Entry<String,Counter>> currentGroupIter = null;

      @Override
      public boolean hasNext() {
        // advance current group if necessary
        if ((currentGroupIter == null || !currentGroupIter.hasNext()) && groupIter.hasNext()) {
          currentGroupIter = groupIter.next().getValue().entrySet().iterator();
        }
        return currentGroupIter != null && currentGroupIter.hasNext();
      }

      @Override
      public Counter next() {
        if (!hasNext()) {
          throw new NoSuchElementException("No more elements in iterator");
        }
        return currentGroupIter.next().getValue();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("remove() is not supported by CounterMap");
      }
    };
  }
}
