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
package com.twitter.hraven.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Writer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.twitter.hraven.ClientObjectMapper;
import com.twitter.hraven.rest.ObjectMapperProvider;

/**
 * Helper class used in the rest client.
 */
// TODO: Remove this class.
@Deprecated
public class JSONUtil {

  /**
  * Writes object to the writer as JSON using Jackson and adds a new-line before flushing.
  * @param writer the writer to write the JSON to
  * @param object the object to write as JSON
  * @throws IOException if the object can't be serialized as JSON or written to the writer
  */
  public static void writeJson(Writer writer, Object object) throws IOException {
    ObjectMapper om = ObjectMapperProvider.createCustomMapper();

    om.configure(SerializationFeature.INDENT_OUTPUT, true);
    om.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

    writer.write(om.writeValueAsString(object));
    writer.write("\n");
    writer.flush();
  }

  public static void writeJson(String fileName, Object object) throws IOException {
    JSONUtil.writeJson(new PrintWriter(fileName), object);
  }

  public static Object readJson(InputStream inputStream, TypeReference type) throws IOException {
    ObjectMapper om = ClientObjectMapper.createCustomMapper();
    om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    return om.readValue(inputStream, type);
  }
}
