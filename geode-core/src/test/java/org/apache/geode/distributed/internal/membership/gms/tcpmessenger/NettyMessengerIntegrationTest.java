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

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.jgroups.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.NetView;
import org.apache.geode.distributed.internal.membership.gms.GMSMember;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.distributed.internal.membership.gms.interfaces.JoinLeave;
import org.apache.geode.distributed.internal.membership.gms.membership.GMSJoinLeave;
import org.apache.geode.distributed.internal.membership.gms.messenger.JGAddress;
import org.apache.geode.internal.Version;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class NettyMessengerIntegrationTest {
  NettyTcpMessenger messenger;

  // Mocks
  private Services services;
  private JoinLeave joinLeave;

  @Before
  public void setUp() {
    joinLeave = mock(GMSJoinLeave.class);
    services = mock(Services.class);
    when(services.getJoinLeave()).thenReturn(joinLeave);

    messenger = new NettyTcpMessenger();
    messenger.init(services);
    messenger.start();
    messenger.started();
  }

  private NetView createView() {
    InternalDistributedMember sender = messenger.getMemberID();
    List<InternalDistributedMember> mbrs = new ArrayList<>();
    mbrs.add(sender);
    mbrs.add(createAddress(100));
    mbrs.add(createAddress(101));
    NetView v = new NetView(sender, 1, mbrs);
    return v;
  }

  /**
   * creates an InternalDistributedMember address that can be used with the doctored FIXME JGroups
   * channel. This includes a logical (UUID) address and a physical (IpAddress) address.
   *
   * @param port the port to use for the new address
   */
  private InternalDistributedMember createAddress(int port) {
    GMSMember gms = new GMSMember("localhost", port);
    gms.setUUID(UUID.randomUUID());
    gms.setVmKind(ClusterDistributionManager.NORMAL_DM_TYPE);
    gms.setVersionOrdinal(Version.CURRENT_ORDINAL);
    return new InternalDistributedMember(gms);
  }


  @Test
  public void ioExceptionInitiatesSuspectProcessing() throws Exception {
    NetView v = createView();
    when(joinLeave.getView()).thenReturn(v);
    messenger.installView(v);
    messenger.handleJGroupsIOException(new IOException("je m'en fiche"),
        new JGAddress(v.getMembers().get(1)));
    verify(healthMonitor).suspect(isA(InternalDistributedMember.class), isA(String.class));
  }
}
