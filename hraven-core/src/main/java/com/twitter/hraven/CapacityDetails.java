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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.InvalidPropertiesFormatException;
import java.util.Map;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.twitter.hraven.datasource.ProcessingException;

/**
 * Represents the capacity information
 * Presently stores only pool/queue capacity related numbers
 * Also provides functionality for loading fair scheduler to get
 * hadoop1 and hadoop2 Pool/Queue capacity numbers
 *
 * Can be extended to include other capacity related information
 */
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class CapacityDetails {

  private static Log LOG = LogFactory.getLog(CapacityDetails.class);
  private long minResources;
  private long minMaps;
  private long minReduces;

  private enum FAIR_SCHEDULER_ATTRIBUTES {
    minResources, minMaps, minReduces
  };

  public CapacityDetails() {
  }

  public CapacityDetails(long minResources, long minMaps, long minReduces) {
    this.minResources = minResources;
    this.minMaps = minMaps;
    this.minReduces = minReduces;
  }

  public long getMinResources() {
    return minResources;
  }

  public void setMinResources(long minResources) {
    this.minResources = minResources;
  }

  public long getMinMaps() {
    return minMaps;
  }

  public void setMinMaps(long minMaps) {
    this.minMaps = minMaps;
  }

  public long getMinReduces() {
    return minReduces;
  }

  public void setMinReduces(long minReduces) {
    this.minReduces = minReduces;
  }

  /**
   * processes the xml document for different attributes, namely minResources, minMaps and minReduces
   * @param attribute
   * @param capacityInfo
   * @param doc
   * @return
   */
  static Map<String, CapacityDetails> processXmlDoc(FAIR_SCHEDULER_ATTRIBUTES attribute,
      Map<String, CapacityDetails> capacityInfo, Document doc) {
    String key = "";
    Node aNode = null;
    long value = 0L;
    NodeList descNodes = doc.getElementsByTagName(attribute.toString());
    for (int i = 0; i < descNodes.getLength(); i++) {
      aNode = descNodes.item(i);
      key = getKey(aNode);
      if (StringUtils.isBlank(key)) {
        LOG.error("Error in processing xml for " + attribute.toString() + " of "
            + aNode.getNodeName());
        continue;
      }
      value = getValue(aNode);
      CapacityDetails cd = getCapacityDetails(key, capacityInfo);
      switch (attribute) {
      case minResources:
        cd.setMinResources(value);
        break;
      case minMaps:
        cd.setMinMaps(value);
        break;
      case minReduces:
        cd.setMinReduces(value);
        break;
      default:
        LOG.error("unknown attribute in fair scheduler : " + attribute);
        break;
      }
      capacityInfo.put(key, cd);
    }
    return capacityInfo;
  }

  /**
   * loads the hadoop fair scheduler config file to process the queue capacity numbers
   * @return map of queue name to capacity
   * @throws ProcessingException
   */
  public static Map<String, CapacityDetails> loadCapacityDetailsFromFairScheduler(
      String fileName, Map<String, CapacityDetails> capacityInfo) throws ProcessingException {

    try {
      LOG.info("Loading fair scheduler:" + fileName);
      URL url = new URL(fileName);
      URLConnection connection = url.openConnection();
      Document doc = parseXML(connection.getInputStream());
      capacityInfo = processXmlDoc(FAIR_SCHEDULER_ATTRIBUTES.minResources, capacityInfo, doc);
      capacityInfo = processXmlDoc(FAIR_SCHEDULER_ATTRIBUTES.minMaps, capacityInfo, doc);
      capacityInfo = processXmlDoc(FAIR_SCHEDULER_ATTRIBUTES.minReduces, capacityInfo, doc);
    } catch (NumberFormatException nfe) {
      LOG.error("Caught NumberFormatException: " + nfe.getMessage());
    } catch (InvalidPropertiesFormatException e) {
      LOG.error("Caught InvalidPropertiesFormatException: " + e.getMessage());
    } catch (FileNotFoundException e) {
      LOG.error("Caught FileNotFound: " + e.toString());
    } catch (IOException e) {
      LOG.error("Caught IOE " + e.toString());
    }
    if (LOG.isDebugEnabled()) {
      for (Map.Entry<String, CapacityDetails> kv : capacityInfo.entrySet()) {
        LOG.debug(" key value" + kv.getKey() + ": minResources=" + kv.getValue().getMinResources()
            + ": minMaps=" + kv.getValue().getMinMaps() + ": minReduces="
            + kv.getValue().getMinReduces());
      }
    }
    return capacityInfo;
  }

  /**
   * gets the text representation of the queue/pool name as per the XML node definition
   * @param XML Node found in fair scheduler
   * @return queue/pool name
   */
  static String getKey(Node aNode) {
    String key = null;
    Node parent = aNode.getParentNode();
    if (parent != null) {
      NamedNodeMap attributes = parent.getAttributes();
      if (attributes != null) {
        Node n = attributes.getNamedItem("name");
        if (n != null) {
          key = n.getTextContent();
        }
      }
    }
    return key;
  }

  /**
   * gets the value of the pool/queue capacity as a long
   * @param XML node
   * @return value as long
   */
  private static long getValue(Node aNode) {
    long value = 0L;
    try {
      value = Long.parseLong(aNode.getTextContent());
    } catch (NumberFormatException nfe) {
      LOG.error(" caught " + nfe.getMessage() + " for " + aNode.getNodeName());
    }
    return value;
  }

  /**
   * checks if capacity details are already available in the map,
   * if yes, returns that
   * else returns new capacity details object
   * @param queue/pool name as key
   * @param capacityInfo map
   * @return capacity details for that queue/pool
   */
  private static CapacityDetails getCapacityDetails(String key,
      Map<String, CapacityDetails> capacityInfo) {
    CapacityDetails cd = null;
    if (capacityInfo.containsKey(key)) {
      cd = capacityInfo.get(key);
    } else {
      cd = new CapacityDetails();
    }
    return cd;
  }

  /** 
   * parses the input stream as an XML document
   * @param inputstream
   * @return xml document
   * @throws ProcessingException
   */
  public static Document parseXML(InputStream stream) throws ProcessingException {
    DocumentBuilderFactory objDocumentBuilderFactory = null;
    DocumentBuilder objDocumentBuilder = null;
    Document doc = null;
    try {
      objDocumentBuilderFactory = DocumentBuilderFactory.newInstance();
      objDocumentBuilder = objDocumentBuilderFactory.newDocumentBuilder();
      doc = objDocumentBuilder.parse(stream);
    } catch (Exception ex) {
      LOG.error(" here " + ex.getMessage());
      throw new ProcessingException(ex.getMessage());
    }
    return doc;
  }

}