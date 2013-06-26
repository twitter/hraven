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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.deser.CustomDeserializerFactory;
import org.codehaus.jackson.map.deser.StdDeserializerProvider;

/**
 * Custom Jackson ObjectMapper factory that knows how to deserialize json back into objects. This
 * class lives in the same package of the object model so we can add setter methods and constructors
 * as package-private as needed to the object model.
 */
// This is used in JSONUtil and the comment above not-withstanding we probably need to (re)move this
// from the top-level package.
@Deprecated
public class ClientObjectMapper {

  public static ObjectMapper createCustomMapper() {
    ObjectMapper result = new ObjectMapper();
    CustomDeserializerFactory deserializerFactory = new CustomDeserializerFactory();

    deserializerFactory.addSpecificMapping(Configuration.class, new ConfigurationDeserializer());
    deserializerFactory.addSpecificMapping(CounterMap.class, new CounterDeserializer());

    result.setDeserializerProvider(new StdDeserializerProvider(deserializerFactory));
    return result;
  } 

  /**
   * Custom serializer for Configuration object. We don't want to serialize the classLoader.
   */
  private static class ConfigurationDeserializer extends JsonDeserializer<Configuration> {

    @Override
    public Configuration deserialize(JsonParser jsonParser,
                                     DeserializationContext deserializationContext)
                                     throws IOException {
      Configuration conf = new Configuration();

      JsonToken token;
      while ((token = jsonParser.nextToken()) != JsonToken.END_OBJECT) {
        if (token != JsonToken.VALUE_STRING) { continue; } // all deserialized values are strings
        conf.set(jsonParser.getCurrentName(), jsonParser.getText());
      }

      return conf;
    }
  }

  /**
   * Custom serializer for Configuration object. We don't want to serialize the classLoader.
   */
  private static class CounterDeserializer extends JsonDeserializer<CounterMap> {

    @Override
    public CounterMap deserialize(JsonParser jsonParser,
                                  DeserializationContext deserializationContext)
                                  throws IOException {
      CounterMap counterMap = new CounterMap();

      JsonToken token;
      while ((token = jsonParser.nextToken()) != JsonToken.END_OBJECT) {
        assertToken(token, JsonToken.FIELD_NAME);
        String group = jsonParser.getCurrentName();

        assertToken(jsonParser.nextToken(), JsonToken.START_OBJECT);
        while ((token = jsonParser.nextToken()) != JsonToken.END_OBJECT) {
          if (token != JsonToken.VALUE_NUMBER_INT) {
            continue; // all deserialized values are ints
          }

          Counter counter =
              new Counter(group, jsonParser.getCurrentName(), jsonParser.getLongValue());
          counterMap.add(counter);
        }
      }
      return counterMap;
    }
  }

  private static void assertToken(JsonToken found, JsonToken expected) {
    if (expected != found) {
      throw new IllegalStateException("Expecting JsonToken to be " + expected.asString() +
        ", but found JsonToken=" + found.asString());
    }
  }
}
