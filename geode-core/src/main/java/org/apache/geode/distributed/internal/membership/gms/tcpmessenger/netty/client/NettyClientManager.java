package org.apache.geode.distributed.internal.membership.gms.tcpmessenger.netty.client;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.geode.distributed.internal.DistributionMessage;

public class NettyClientManager {
  private final Map<InetSocketAddress, NettyClient> clients = new ConcurrentHashMap<>();

  public void send(InetSocketAddress destination, Object message) {
    NettyClient client = clients.computeIfAbsent(destination, NettyClient::connect);
    client.send(message);
  }

  public void shutdown(InetSocketAddress clientAddress) {
    NettyClient client = clients.get(clientAddress);
    client.close();
  }
}
