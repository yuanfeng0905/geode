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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.protobuf.ByteString;
import com.pholser.junit.quickcheck.From;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.When;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ListValue;
import org.apache.geode.internal.protocol.protobuf.v1.Struct;
import org.apache.geode.internal.protocol.protobuf.v1.Value;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.UnitTest;

@RunWith(JUnitQuickcheck.class)
@Category(IntegrationTest.class)
public class ProtobufStructSerializerTest {

  private ProtobufStructSerializer serializer;
  private static Cache cache;

  @BeforeClass
  public static void createCache() {
    cache = new CacheFactory().set(ConfigurationProperties.LOG_LEVEL, "error")
        .setPdxReadSerialized(true).create();
  }

  @Before
  public void createSerializer() {
    serializer = new ProtobufStructSerializer();
    serializer.init(cache);
  }

  @AfterClass
  public static void tearDown() {
    cache.close();
  }

  @Test
  public void testDeserialize() throws IOException, ClassNotFoundException {
    Struct struct = structWithStringField();
    ByteString bytes = struct.toByteString();
    PdxInstance value = (PdxInstance) serializer.deserialize(bytes);

    assertEquals("value", value.getField("field1"));
  }

  private Struct structWithStringField() {
    return Struct.newBuilder()
        .putFields("field1", Value.newBuilder()
            .setEncodedValue(BasicTypes.EncodedValue.newBuilder().setStringResult("value")).build())
        .build();
  }

  @Test
  public void testSerialize() throws IOException, ClassNotFoundException {
    PdxInstance value = pdxWithStringField();
    ByteString bytes = serializer.serialize(value);
    Struct struct = Struct.parseFrom(bytes);

    assertEquals("value", struct.getFieldsMap().get("field1").getEncodedValue().getStringResult());
  }

  private PdxInstance pdxWithStringField() {
    return cache.createPdxInstanceFactory(ProtobufStructSerializer.PROTOBUF_STRUCT)
        .writeString("field1", "value").create();
  }

  @Test
  public void canSerializeWithNestedPdxInstance() throws IOException, ClassNotFoundException {
    PdxInstance value = cache.createPdxInstanceFactory(ProtobufStructSerializer.PROTOBUF_STRUCT)
        .writeObject("field1", pdxWithStringField()).create();
    ByteString bytes = serializer.serialize(value);
    Struct struct = Struct.parseFrom(bytes);

    assertEquals("value", struct.getFieldsMap().get("field1").getStructValue().getFieldsMap()
        .get("field1").getEncodedValue().getStringResult());
  }

  @Test
  public void canSerializeWithNestedList() throws IOException, ClassNotFoundException {
    ArrayList<PdxInstance> list = new ArrayList<>();
    list.add(pdxWithStringField());
    PdxInstance value = cache.createPdxInstanceFactory(ProtobufStructSerializer.PROTOBUF_STRUCT)
        .writeObject("field2", list).create();
    ByteString bytes = serializer.serialize(value);
    Struct struct = Struct.parseFrom(bytes);

    assertEquals(Struct.newBuilder()
        .putFields("field2",
            Value.newBuilder()
                .setListValue(ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStructValue(structWithStringField())))
                .build())
        .build(), struct);
  }

  @Test
  public void canDeserializeWithNestedStruct() throws IOException, ClassNotFoundException {
    Struct.Builder builder = Struct.newBuilder();
    builder.putFields("field1", Value.newBuilder().setStructValue(structWithStringField()).build());
    ByteString bytes = builder.build().toByteString();
    PdxInstance value = (PdxInstance) serializer.deserialize(bytes);

    PdxInstance nested = (PdxInstance) value.getField("field1");
    assertEquals("value", nested.getField("field1"));
  }

  @Test
  public void canDeserializeWithNestedList() throws IOException, ClassNotFoundException {
    Struct.Builder builder = Struct.newBuilder();
    builder.putFields("field1",
        Value.newBuilder()
            .setListValue(ListValue.newBuilder().addValues(Value.newBuilder()
                .setEncodedValue(BasicTypes.EncodedValue.newBuilder().setStringResult("value"))))
            .build());
    ByteString bytes = builder.build().toByteString();
    PdxInstance value = (PdxInstance) serializer.deserialize(bytes);

    List<String> nested = (List<String>) value.getField("field1");
    assertEquals(Arrays.asList("value"), nested);
  }


  @Property(trials = 10)
  public void testSymmetry(
      @PdxInstanceGenerator.ClassName(ProtobufStructSerializer.PROTOBUF_STRUCT) @PdxInstanceGenerator.FieldTypes({
          String.class, int.class, long.class, byte.class, byte[].class, double.class,
          PdxInstance.class,
          ArrayList.class}) @From(PdxInstanceGenerator.class) PdxInstance original)
      throws IOException, ClassNotFoundException {
    ByteString bytes = serializer.serialize(original);
    PdxInstance actual = (PdxInstance) serializer.deserialize(bytes);
    assertThat(original).isEqualTo(actual);
    assertEquals(actual, original);
  }
}
