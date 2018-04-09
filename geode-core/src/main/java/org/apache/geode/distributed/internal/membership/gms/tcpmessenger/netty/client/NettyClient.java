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
import java.util.List;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.apache.geode.distributed.internal.membership.gms.tcpmessenger.netty.DataSerializableEncoder;

public class NettyClient {

  private final InetSocketAddress destination;
  private final ChannelFuture channelFuture;

  private NettyClient(InetSocketAddress destination, ClientChannelFactory factory) {
    this.destination = destination;
    EventLoopGroup workerGroup = new NioEventLoopGroup();
    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(workerGroup);
    bootstrap.channel(NioSocketChannel.class);
    bootstrap.handler(new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) throws Exception {
        factory.makeChannel(ch, List<ChannelHandler> handlers);
        ch.pipeline().addLast(new DataSerializableEncoder());
      }
    });
    channelFuture = bootstrap.connect(destination);
  }

  public void send(Object message) {
    try {
      channelFuture.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    channelFuture.channel().writeAndFlush(message);

  }


  public static NettyClient connect(InetSocketAddress destination) {
    return new NettyClient(destination);
  }

  public void close() {
    channelFuture.channel().close();
  }

  public static NettyClient connect(InetSocketAddress dest, ClientChannelFactory factory) {
    return null;
  }
}
