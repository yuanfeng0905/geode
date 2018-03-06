package org.apache.geode.distributed.internal.membership.gms.tcpmessenger.netty;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;

import org.apache.geode.DataSerializer;

/**
 * Message decoder that converts bytes into an object using the DataSerializer.
 *
 * DataSerializer does not actually include a length, so we have to try to deserialize
 * fail, and retry the deserialization if there are not enough bytes. Thats what ReplayingDecoder does
 * We could consider putting a length on the wire instead.
 */
public class DataSerializableDecoder extends ReplayingDecoder<Void> {
  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
      throws Exception {
    Object message = DataSerializer.readObject(new ByteBufInputStream(in));
    out.add(message);
    return;
  }
}
