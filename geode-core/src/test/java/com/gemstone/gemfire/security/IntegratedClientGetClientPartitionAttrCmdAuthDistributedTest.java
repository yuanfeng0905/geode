/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.security;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.client.internal.GetClientPartitionAttributesOp;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class IntegratedClientGetClientPartitionAttrCmdAuthDistributedTest extends AbstractIntegratedClientAuthDistributedTest {

  @Test
  public void testGetClientPartitionAttrCmd() {
    client1.invoke("logging in stranger", () -> {
      ClientCache cache = new ClientCacheFactory(createClientProperties("stranger", "1234567"))
        .setPoolSubscriptionEnabled(true)
        .addPoolServer("localhost", serverPort)
        .create();

      Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);

      assertNotAuthorized(() -> GetClientPartitionAttributesOp.execute((PoolImpl)cache.getDefaultPool(), REGION_NAME), "CLUSTER:READ");
    });

    client2.invoke("logging in super-user with correct password", () -> {
      ClientCache cache = new ClientCacheFactory(createClientProperties("super-user", "1234567"))
        .setPoolSubscriptionEnabled(true)
        .addPoolServer("localhost", serverPort)
        .create();

      Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);

      GetClientPartitionAttributesOp.execute((PoolImpl)cache.getDefaultPool(), REGION_NAME);
    });
  }
}


