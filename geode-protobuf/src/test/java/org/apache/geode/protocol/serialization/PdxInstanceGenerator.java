/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.protocol.serialization;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE_USE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.pholser.junit.quickcheck.generator.GenerationStatus;
import com.pholser.junit.quickcheck.generator.Generator;
import com.pholser.junit.quickcheck.generator.GeneratorConfiguration;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;

public class PdxInstanceGenerator extends Generator {


  private static final Map<Class<?>, Method> supportedTypes;
  public static final GenerationStatus.Key DEPTH = new GenerationStatus.Key("depth", Integer.class);

  static {
    HashMap<Class<?>, Method> types = new HashMap<>();
    Method[] methods = PdxInstanceFactory.class.getDeclaredMethods();
    for (Method method : methods) {
      if (method.getName().startsWith("write") && method.getParameterTypes().length == 2) {
        Class<?> type = method.getParameterTypes()[1];

        if (type == Object.class) {
          types.put(PdxInstance.class, method);
          types.put(ArrayList.class, method);
        } else {
          types.put(type, method);
        }
      }
    }

    supportedTypes = Collections.unmodifiableMap(types);

  }
  private Set<Class<?>> allowedFieldTypes;
  private String className = "NO_CLASS";


  public PdxInstanceGenerator() {
    super(PdxInstance.class);
  }

  @Override
  public Object generate(SourceOfRandomness random, GenerationStatus status) {

    Map<Class<?>, Method> writeMethods = getAllowedWriteMethods();

    int numFields = random.nextInt(0, 20);
    PdxInstanceFactory factory = CacheFactory.getAnyInstance().createPdxInstanceFactory(className);
    Set<String> fieldNames =
        new HashSet<>(gen().type(String.class).times(numFields).generate(random, status));
    for (String fieldName : fieldNames) {
      Map.Entry<Class<?>, Method> writeMethod = random.choose(writeMethods.entrySet());
      Class<?> type = writeMethod.getKey();
      Method method = writeMethod.getValue();
      Object value = null;
      if (type == PdxInstance.class) {
        int depth = (int) status.valueOf(DEPTH).orElse(0);
        if (depth < status.size()) {
          status.setValue(DEPTH, depth + 1);
          value = generate(random, status);
        }
      } else if (type == ArrayList.class) {
        int depth = (int) status.valueOf(DEPTH).orElse(0);
        if (depth < status.size()) {
          status.setValue(DEPTH, depth + 1);
          ArrayList<PdxInstance> list = new ArrayList<>();
          list.add((PdxInstance) generate(random, status));
          value = list;
        }
      } else {
        value = gen().type(type).generate(random, status);
      }
      try {
        method.invoke(factory, fieldName, value);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new IllegalStateException(e);
      }
    }

    return factory.create();
  }

  private Map<Class<?>, Method> getAllowedWriteMethods() {
    final Map<Class<?>, Method> writeMethods = new HashMap<>(supportedTypes);
    writeMethods.keySet().retainAll(allowedFieldTypes);

    if (writeMethods.size() != allowedFieldTypes.size()) {
      HashSet<Class<?>> classes = new HashSet<>(allowedFieldTypes);
      classes.removeAll(supportedTypes.keySet());
      throw new IllegalStateException("Cannot generate value of types " + classes);
    }

    return writeMethods;
  }

  public void configure(FieldTypes fieldTypes) {
    this.allowedFieldTypes = new HashSet<>(Arrays.asList(fieldTypes.value()));
  }

  public void configure(ClassName className) {
    this.className = className.value();
  }

  @Target({PARAMETER, FIELD, ANNOTATION_TYPE, TYPE_USE})
  @Retention(RUNTIME)
  @GeneratorConfiguration
  public @interface FieldTypes {
    Class<?>[] value();
  }

  @Target({PARAMETER, FIELD, ANNOTATION_TYPE, TYPE_USE})
  @Retention(RUNTIME)
  @GeneratorConfiguration
  public @interface ClassName {
    String value();
  }
}
