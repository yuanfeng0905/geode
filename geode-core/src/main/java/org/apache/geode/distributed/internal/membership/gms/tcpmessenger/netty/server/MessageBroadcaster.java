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
package org.apache.geode.distributed.internal.membership.gms.tcpmessenger.netty.server;

import java.util.HashMap;
import java.util.Map;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.membership.gms.interfaces.MessageHandler;

class MessageBroadcaster extends SimpleChannelInboundHandler<DistributionMessage> {
  private Map<Class<? extends DistributionMessage>, MessageHandler> handlers = new HashMap<>();


  @Override
  protected void channelRead0(ChannelHandlerContext ctx, DistributionMessage msg) throws Exception {
    MessageHandler handler = handlers.get(msg.getClass());
    if (handler == null) {
      throw new IllegalStateException("Unexpected message " + msg);
    }
    handler.processMessage(msg);
  }

  public void addHandler(Class clazz, MessageHandler handler) {
    this.handlers.put(clazz, handler);
  }
}
