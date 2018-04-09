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

package org.apache.geode.distributed.internal.membership.gms.tcpmessenger;

import static org.junit.Assert.assertEquals;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.membership.gms.tcpmessenger.netty.client.NettyClient;
import org.apache.geode.distributed.internal.membership.gms.tcpmessenger.netty.server.NettyServer;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class NettyTcpMessengerTest {

  @Test
  public void roundTrip() throws Exception {
    final NettyServer nettyServer = new NettyServer();
    final AtomicReference<DistributionMessage> messageReference = new AtomicReference<>();

    final TestMessage testMessage = new TestMessage(26);

    nettyServer.addHandler(TestMessage.class, (message) -> messageReference.set(message));
    nettyServer.start();
    final NettyClient nettyClient = NettyClient.connect(nettyServer.getAddress());

    nettyClient.send(testMessage);
    Awaitility.await().atMost(7, TimeUnit.SECONDS)
        .until(() -> assertEquals(testMessage, messageReference.get()));
  }


  public static class TestMessage extends DistributionMessage {
    private int value;

    public TestMessage() {}

    TestMessage(int value) {
      this.value = value;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof TestMessage && ((TestMessage) obj).value == value;
    }

    @Override
    public int getProcessorType() {
      return 0;
    }

    @Override
    protected void process(ClusterDistributionManager dm) {

    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeInt(value);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      value = in.readInt();
    }

    @Override
    public int getDSFID() {
      return NO_FIXED_ID;
    }
  }
}
