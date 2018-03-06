package org.apache.geode.distributed.internal.membership.gms.tcpmessenger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import org.junit.Test;

import org.apache.geode.distributed.internal.DistributionMessage;

public class NettyTcpMessengerTest {

  @Test
  public void testSend() {
    NettyTcpMessenger messenger = new NettyTcpMessenger();
    DistributionMessage message = mock(DistributionMessage.class);
    messenger.send(message);
  }

}