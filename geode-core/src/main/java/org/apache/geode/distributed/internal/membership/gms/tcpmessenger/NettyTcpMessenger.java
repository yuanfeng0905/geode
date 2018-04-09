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

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.geode.distributed.DurableClientAttributes;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.MemberAttributes;
import org.apache.geode.distributed.internal.membership.NetView;
import org.apache.geode.distributed.internal.membership.QuorumChecker;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.distributed.internal.membership.gms.interfaces.MessageHandler;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Messenger;
import org.apache.geode.distributed.internal.membership.gms.tcpmessenger.netty.client.NettyClientManager;
import org.apache.geode.distributed.internal.membership.gms.tcpmessenger.netty.server.NettyServer;
import org.apache.geode.internal.OSProcess;

public class NettyTcpMessenger implements Messenger {
  private Services services;
  private final NettyServer server = new NettyServer();
  private final NettyClientManager client = new NettyClientManager();
  private InternalDistributedMember localAddress;
  private NetView currentView;

  @Override
  public void addHandler(Class c, MessageHandler h) {
    server.addHandler(c, h);
  }

  @Override
  public Set<InternalDistributedMember> send(DistributionMessage m, NetView alternateView) {
    return send(m);
  }

  @Override
  public Set<InternalDistributedMember> send(DistributionMessage m) {
    for (InternalDistributedMember member : m.getRecipients()) {
      client.send(new InetSocketAddress(member.getHost(), member.getPort()), m);
    }

    return Collections.emptySet();
  }

  @Override
  public Set<InternalDistributedMember> sendUnreliably(DistributionMessage m) {
    return send(m);
  }

  @Override
  public InternalDistributedMember getMemberID() {
    return localAddress;
  }

  @Override
  public QuorumChecker getQuorumChecker() {
    return null;
  }

  @Override
  public boolean testMulticast(long timeout) throws InterruptedException {
    return false;
  }

  @Override
  public void getMessageState(InternalDistributedMember member, Map state,
      boolean includeMulticast) {

  }

  @Override
  public void waitForMessageState(InternalDistributedMember member, Map state)
      throws InterruptedException {

  }

  @Override
  public byte[] getPublicKey(InternalDistributedMember mbr) {
    return new byte[0];
  }

  @Override
  public void setPublicKey(byte[] publickey, InternalDistributedMember mbr) {

  }

  @Override
  public void setClusterSecretKey(byte[] clusterSecretKey) {
    throw new IllegalStateException();

  }

  @Override
  public byte[] getClusterSecretKey() {
    throw new IllegalStateException();
  }

  @Override
  public int getRequestId() {
    return 0;
  }

  @Override
  public void initClusterKey() {

  }

  @Override
  public void init(Services services) {
    this.services = services;

  }

  @Override
  public void start() {
    try {
      server.start();
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    }

    InetSocketAddress serverAddress = server.getAddress();

    // This junk all came from JGroupsMessenger. Refactor into a common class?
    DistributionConfig config = services.getConfig().getDistributionConfig();
    boolean isLocator = (services.getConfig().getTransport()
        .getVmKind() == ClusterDistributionManager.LOCATOR_DM_TYPE)
        || !services.getConfig().getDistributionConfig().getStartLocator().isEmpty();

    // establish the DistributedSystem's address
    DurableClientAttributes dca = null;
    if (config.getDurableClientId() != null) {
      dca = new DurableClientAttributes(config.getDurableClientId(),
          config.getDurableClientTimeout());
    }
    MemberAttributes attr = new MemberAttributes(-1/* dcPort - not known at this time */,
        OSProcess.getId(), services.getConfig().getTransport().getVmKind(),
        -1/* view id - not known at this time */, config.getName(),
        MemberAttributes.parseGroups(config.getRoles(), config.getGroups()), dca);
    localAddress = new InternalDistributedMember(serverAddress.getAddress(),
        serverAddress.getPort(), config.getEnableNetworkPartitionDetection(), isLocator, attr);

  }

  @Override
  public void started() {

  }

  @Override
  public void stop() {

  }

  @Override
  public void stopped() {

  }

  @Override
  public void installView(NetView v) {
    Set<InternalDistributedMember> crashedMembers = v.getActualCrashedMembers(currentView);
    this.currentView = v;
    for (InternalDistributedMember member : crashedMembers) {
      client.shutdown(new InetSocketAddress(member.getHost(), member.getPort()));
    }

  }

  @Override
  public void beSick() {

  }

  @Override
  public void playDead() {

  }

  @Override
  public void beHealthy() {

  }

  @Override
  public void emergencyClose() {

  }

  @Override
  public void memberSuspected(InternalDistributedMember initiator,
      InternalDistributedMember suspect, String reason) {

  }
}
