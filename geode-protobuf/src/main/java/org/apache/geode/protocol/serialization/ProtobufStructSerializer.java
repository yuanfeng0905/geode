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
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import com.google.protobuf.UnsafeByteOperations;

import org.apache.geode.cache.Cache;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ListValue;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.Struct;
import org.apache.geode.internal.protocol.protobuf.v1.Value;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;

public class ProtobufStructSerializer implements ValueSerializer {
  static final String PROTOBUF_STRUCT = "__PROTOBUF_STRUCT_AS_PDX";
  private Cache cache;

  @Override
  public ByteString serialize(Object object) throws IOException {
    return serializeStruct(object).toByteString();
  }

  Struct serializeStruct(Object object) {

    PdxInstance pdxInstance = (PdxInstance) object;

    Struct.Builder structBuilder = Struct.newBuilder();
    for (String fieldName : pdxInstance.getFieldNames()) {
      Object value = pdxInstance.getField(fieldName);
      Value serialized = serializeValue(value);
      structBuilder.putFields(fieldName, serialized);
    }

    return structBuilder.build();
  }

  private Value serializeValue(Object value) {
    Value.Builder builder = Value.newBuilder();
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
    } else if (value instanceof List) {
      List<Value> values =
          ((List<Object>) value).stream().map(this::serializeValue).collect(Collectors.toList());
      builder.setListValue(ListValue.newBuilder().addAllValues(values).build());
    } else if (value == null) {
      builder.setEncodedValue(
          BasicTypes.EncodedValue.newBuilder().setNullResult(NullValue.NULL_VALUE).build());
    } else {
      throw new IllegalStateException(
          "Don't know how to translate object of type " + value.getClass() + ": " + value);
    }
    return builder.build();
  }

  @Override
  public Object deserialize(ByteString bytes) throws IOException, ClassNotFoundException {
    Struct struct = Struct.parseFrom(bytes);
    return deserialize(struct);
  }

  private Object deserialize(Struct struct) {
    PdxInstanceFactory pdxInstanceFactory = cache.createPdxInstanceFactory(PROTOBUF_STRUCT);

    for (Map.Entry<String, Value> field : struct.getFieldsMap().entrySet()) {
      String fieldName = field.getKey();
      Object value = deserialize(field.getValue());
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

  private List<?> deserialize(List<Value> listValue) {
    return listValue.stream().map(this::deserialize).collect(Collectors.toList());
  }

  private Object deserialize(Value value) {
    switch (value.getKindCase()) {
      case ENCODEDVALUE:
        return new ProtobufSerializationService().decode(value.getEncodedValue());
      case STRUCTVALUE:
        return deserialize(value.getStructValue());
      case LISTVALUE:
        return deserialize(value.getListValue().getValuesList());
      default:
        throw new IllegalStateException(
            "Don't know how to translate object of type " + value.getKindCase() + ": " + value);
    }
  }

  @Override
  public void init(Cache cache) {
    this.cache = cache;

  }
}
