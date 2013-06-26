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

/**
 * Key class representing rows in the {@link Constants#FLOW_EVENT_TABLE} table.
 */
public class FlowEventKey extends FlowKey {
  private int sequence;

  public FlowEventKey(FlowKey flowKey, int sequence) {
    super(flowKey);
    this.sequence = sequence;
  }

  public FlowEventKey(String cluster, String user, String appId, long runId, int sequence) {
    super(cluster, user, appId, runId);
    this.sequence = sequence;
  }

  public int getSequence() {
    return this.sequence;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof FlowEventKey)) {
      return false;
    }
    FlowEventKey otherKey = (FlowEventKey)other;
    return super.equals(other) && this.sequence == otherKey.sequence;
  }
}
