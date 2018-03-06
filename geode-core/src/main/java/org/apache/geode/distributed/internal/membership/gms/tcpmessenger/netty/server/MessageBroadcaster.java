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
  protected void channelRead0(ChannelHandlerContext ctx, DistributionMessage msg)
      throws Exception {
    MessageHandler handler = handlers.get(msg);
    if(handler == null) {
      throw new IllegalStateException("Unexpected message " + msg);
    }
    handler.processMessage(msg);
  }

  public void addHandler(Class clazz, MessageHandler handler) {
    this.handlers.put(clazz, handler);
  }
}
