package org.apache.geode.distributed.internal.membership.gms.tcpmessenger.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import org.apache.geode.DataSerializer;

public class DataSerializableEncoder extends MessageToByteEncoder<Object> {
  @Override
  protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
    DataSerializer.writeObject(msg, new ByteBufOutputStream(out));
  }
}
