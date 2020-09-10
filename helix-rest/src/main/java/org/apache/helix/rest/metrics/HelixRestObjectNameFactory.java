package org.apache.helix.rest.metrics;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Hashtable;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.codahale.metrics.jmx.JmxReporter;
import com.codahale.metrics.jmx.ObjectNameFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class HelixRestObjectNameFactory implements ObjectNameFactory {
  private static final Logger LOG = LoggerFactory.getLogger(JmxReporter.class);

  private static final String KEY_NAME = "name";
  private static final String KEY_TYPE = "type";
  private static final String KEY_NAMESPACE = "namespace";

  public ObjectName createName(String type, String domainNameSpace, String name) {
    String[] splits = domainNameSpace.split(":");
    String domain = splits[0];
    String namespace = splits[1];

    try {
      Hashtable<String, String> properties = new Hashtable<>();
      properties.put(KEY_NAME, name);
      properties.put(KEY_TYPE, type);
      properties.put(KEY_NAMESPACE, namespace);

      /*
       * The only way we can find out if we need to quote the properties is by
       * checking an ObjectName that we've constructed.
       */
      ObjectName objectName = new ObjectName(domain, properties);
      if (objectName.isDomainPattern()) {
        domain = ObjectName.quote(domain);
      }

      if (objectName.isPropertyValuePattern(KEY_NAME)) {
        properties.put(KEY_NAME, ObjectName.quote(name));
      }

      if (objectName.isPropertyValuePattern(KEY_TYPE)) {
        properties.put(KEY_TYPE, ObjectName.quote(type));
      }

      if (objectName.isPropertyValuePattern(KEY_NAMESPACE)) {
        properties.put(KEY_NAMESPACE, ObjectName.quote(namespace));
      }
      objectName = new ObjectName(domain, properties);

      return objectName;
    } catch (MalformedObjectNameException e) {
      try {
        return new ObjectName(domain, KEY_NAME, ObjectName.quote(name));
      } catch (MalformedObjectNameException e1) {
        LOG.warn("Unable to register {} {}", type, name, e1);
        throw new RuntimeException(e1);
      }
    }
  }
}
