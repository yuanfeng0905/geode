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
package org.apache.geode.distributed.internal.membership.gms.tcpmessenger.netty.client;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.geode.distributed.internal.DistributionMessage;

public class NettyClientManager {
  private final Map<InetSocketAddress, NettyClient> clients = new ConcurrentHashMap<>();
  private final ClientChannelFactory factory;

  NettyClientManager(ClientChannelFactory factory) {
    this.factory = factory;
  }

  public void send(InetSocketAddress destination, Object message) {
    NettyClient client = clients.computeIfAbsent(destination, (dest) -> NettyClient.connect(dest, factory));
    client.send(message);
  }

  public void shutdown(InetSocketAddress clientAddress) {
    NettyClient client = clients.get(clientAddress);
    client.close();
  }
}
