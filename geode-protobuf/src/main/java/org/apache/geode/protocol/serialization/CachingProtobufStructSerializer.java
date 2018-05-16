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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import com.google.protobuf.UnsafeByteOperations;

import org.apache.geode.cache.Cache;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.CachedString;
import org.apache.geode.internal.protocol.protobuf.v1.CachingStruct;
import org.apache.geode.internal.protocol.protobuf.v1.Field;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;

public class CachingProtobufStructSerializer implements ValueSerializer {
  private Cache cache;
  private final Map<String, CachedString> writeCache = new ConcurrentHashMap<>();
  private final Map<Integer, String> readCache = new ConcurrentHashMap<>();

  @Override
  public ByteString serialize(Object object) throws IOException {
    return serializeStruct(object).toByteString();
  }

  CachingStruct serializeStruct(Object object) {

    PdxInstance pdxInstance = (PdxInstance) object;

    CachingStruct.Builder structBuilder = CachingStruct.newBuilder();
    structBuilder.setTypeName(cacheWrite(pdxInstance.getClassName()));

    for (String fieldName : pdxInstance.getFieldNames()) {
      Object value = pdxInstance.getField(fieldName);
      Field serialized = serializeField(fieldName, value);
      structBuilder.addFields(serialized);
    }


    return structBuilder.build();
  }

  private Field serializeField(String fieldName, Object value) {
    Field.Builder builder = Field.newBuilder();
    builder.setFieldName(cacheWrite(fieldName));
    if (value instanceof String) {
      builder.setEncodedValue(
          BasicTypes.EncodedValue.newBuilder().setStringResult((String) value).build());
    } else if (value instanceof Boolean) {
      builder.setEncodedValue(
          BasicTypes.EncodedValue.newBuilder().setBooleanResult((Boolean) value).build());
    } else if (value instanceof Integer) {
      builder.setEncodedValue(
          BasicTypes.EncodedValue.newBuilder().setIntResult((Integer) value).build());
    } else if (value instanceof Byte) {
      builder.setEncodedValue(
          BasicTypes.EncodedValue.newBuilder().setByteResult((Byte) value).build());
    } else if (value instanceof Long) {
      builder.setEncodedValue(
          BasicTypes.EncodedValue.newBuilder().setLongResult((Long) value).build());
    } else if (value instanceof Double) {
      builder.setEncodedValue(
          BasicTypes.EncodedValue.newBuilder().setDoubleResult((Double) value).build());
    } else if (value instanceof byte[]) {
      builder.setEncodedValue(BasicTypes.EncodedValue.newBuilder()
          .setBinaryResult(UnsafeByteOperations.unsafeWrap((byte[]) value)).build());
    } else if (value instanceof PdxInstance) {
      builder.setStructValue(serializeStruct(value));
    } else if (value == null) {
      builder.setEncodedValue(
          BasicTypes.EncodedValue.newBuilder().setNullResult(NullValue.NULL_VALUE).build());
    } else {
      throw new IllegalStateException(
          "Don't know how to translate object of type " + value.getClass() + ": " + value);
    }
    return builder.build();
  }

  private CachedString cacheWrite(final String string) {
    CachedString result;
    if (string.isEmpty()) {
      result = CachedString.getDefaultInstance();
    } else {
      CachedString cachedValue = writeCache.get(string);
      if (cachedValue != null) {
        result = cachedValue;
      } else {
        int id = writeCache.size() + 1;
        writeCache.put(string, CachedString.newBuilder().setId(id).build());
        result = CachedString.newBuilder().setId(id).setValue(string).build();
      }
    }

    return result;
  }

  private String cacheRead(final CachedString fieldName) {
    String value = fieldName.getValue();
    int id = fieldName.getId();
    if (id == 0) {
      return value;
    }

    if (!value.isEmpty()) {
      readCache.put(id, value);
      return value;
    }

    return readCache.get(id);
  }

  @Override
  public Object deserialize(ByteString bytes) throws IOException, ClassNotFoundException {
    CachingStruct struct = CachingStruct.parseFrom(bytes);
    return deserialize(struct);
  }

  private Object deserialize(CachingStruct struct) {

    String typeName = cacheRead(struct.getTypeName());
    PdxInstanceFactory pdxInstanceFactory = cache.createPdxInstanceFactory(typeName);

    for (Field field : struct.getFieldsList()) {
      String fieldName = cacheRead(field.getFieldName());
      Object value = deserializeField(field);
      if (value instanceof String) {
        pdxInstanceFactory.writeString(fieldName, (String) value);
      } else if (value instanceof Boolean) {
        pdxInstanceFactory.writeBoolean(fieldName, (Boolean) value);
      } else if (value instanceof Integer) {
        pdxInstanceFactory.writeInt(fieldName, (Integer) value);
      } else if (value instanceof Byte) {
        pdxInstanceFactory.writeByte(fieldName, (Byte) value);
      } else if (value instanceof Long) {
        pdxInstanceFactory.writeLong(fieldName, (Long) value);
      } else if (value instanceof byte[]) {
        pdxInstanceFactory.writeByteArray(fieldName, (byte[]) value);
      } else if (value instanceof Double) {
        pdxInstanceFactory.writeDouble(fieldName, (Double) value);
      } else if (value instanceof PdxInstance) {
        pdxInstanceFactory.writeObject(fieldName, value);
      } else if (value instanceof List) {
        pdxInstanceFactory.writeObject(fieldName, value);
      } else if (value == null) {
        pdxInstanceFactory.writeObject(fieldName, null);
      } else {
        throw new IllegalStateException(
            "Don't know how to translate object of type " + value.getClass() + ": " + value);
      }
    }

    return pdxInstanceFactory.create();
  }

  private Object deserializeField(Field value) {
    switch (value.getValueCase()) {
      case ENCODEDVALUE:
        return new ProtobufSerializationService().decode(value.getEncodedValue());
      case STRUCTVALUE:
        return deserialize(value.getStructValue());
      default:
        throw new IllegalStateException(
            "Don't know how to translate object of type " + value.getValueCase() + ": " + value);
    }
  }

  @Override
  public void init(Cache cache) {
    this.cache = cache;

  }
}
