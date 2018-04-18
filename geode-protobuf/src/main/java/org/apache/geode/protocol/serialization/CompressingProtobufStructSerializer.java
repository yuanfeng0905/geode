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

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import org.iq80.snappy.Snappy;

import org.apache.geode.cache.Cache;
import org.apache.geode.internal.protocol.protobuf.v1.Struct;

public class CompressingProtobufStructSerializer implements ValueSerializer {
  private final ProtobufStructSerializer delegate = new ProtobufStructSerializer();

  @Override
  public ByteString serialize(Object object) throws IOException {
    Struct uncompressed = delegate.serializeStruct(object);
    byte[] compressed = Snappy.compress(uncompressed.toByteArray());
    return UnsafeByteOperations.unsafeWrap(compressed);
  }

  @Override
  public Object deserialize(ByteString bytes) throws IOException, ClassNotFoundException {
    byte[] compressed = bytes.toByteArray();
    byte[] uncompressed = Snappy.uncompress(compressed, 0, compressed.length);
    ByteString byteString = UnsafeByteOperations.unsafeWrap(uncompressed);
    return delegate.deserialize(byteString);
  }

  @Override
  public void init(Cache cache) {
    delegate.init(cache);

  }
}
