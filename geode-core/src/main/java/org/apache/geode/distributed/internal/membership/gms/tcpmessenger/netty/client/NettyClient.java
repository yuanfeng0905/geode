package org.apache.geode.distributed.internal.membership.gms.tcpmessenger.netty.client;

import java.net.InetSocketAddress;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.membership.gms.tcpmessenger.netty.DataSerializableEncoder;

public class NettyClient {

  private final InetSocketAddress destination;
  private final ChannelFuture channelFuture;

  private NettyClient(InetSocketAddress destination) {
    this.destination = destination;
    EventLoopGroup workerGroup = new NioEventLoopGroup();
    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(workerGroup);
    bootstrap.channel(NioSocketChannel.class);
    bootstrap.handler(new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(new DataSerializableEncoder());
      }
    });
    channelFuture = bootstrap.connect(destination);
  }

  public void send(Object message) {
    channelFuture.channel().writeAndFlush(message);

  }


  public static NettyClient connect(InetSocketAddress destination) {
    NettyClient client = new NettyClient(destination);
    return client;
  }

  public void close() {
    channelFuture.channel().close();
  }
}
