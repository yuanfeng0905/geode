package org.apache.geode.distributed.internal.membership.gms.tcpmessenger.netty.server;

import java.net.InetSocketAddress;
import java.util.Map;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.membership.gms.interfaces.MessageHandler;
import org.apache.geode.distributed.internal.membership.gms.tcpmessenger.netty.DataSerializableDecoder;
import org.apache.geode.internal.net.SocketCreator;

public class NettyServer {

  private ChannelFuture channelFuture;
  private EventLoopGroup acceptorGroup;
  private EventLoopGroup workerGroup;
  private MessageBroadcaster broadcaster = new MessageBroadcaster();

  public void start() throws InterruptedException {
    acceptorGroup = new NioEventLoopGroup();
    workerGroup = new NioEventLoopGroup();

    ServerBootstrap bootstrap = new ServerBootstrap();
    bootstrap.localAddress("localhost", 0)
        .group(acceptorGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                          ch.pipeline().addLast(new DataSerializableDecoder());
                          ch.pipeline().addLast(broadcaster);

                        }
                      });

            channelFuture = bootstrap.bind().sync();
  }

  public InetSocketAddress getAddress() {
    return (InetSocketAddress) channelFuture.channel().localAddress();
  }

  public void addHandler(
      Class<? extends DistributionMessage> clazz,
      MessageHandler handler) {
    broadcaster.addHandler(clazz, handler);
  }

  public void close() {
    channelFuture.channel().close();
    acceptorGroup.shutdownGracefully();
    workerGroup.shutdownGracefully();
  }

}
