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
 * Represents an event generated during flow execution
 */
public class FlowEvent {
  private FlowEventKey key;
  private long timestamp;
  private Framework framework;
  private String type;
  private String eventDataJSON;

  public FlowEvent(FlowEventKey key) {
    this.key = key;
  }

  public FlowEventKey getFlowEventKey() {
    return this.key;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public Framework getFramework() {
    return framework;
  }

  public void setFramework(Framework framework) {
    this.framework = framework;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getEventDataJSON() {
    return eventDataJSON;
  }

  public void setEventDataJSON(String eventDataJSON) {
    this.eventDataJSON = eventDataJSON;
  }
}
