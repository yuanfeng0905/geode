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
package org.apache.geode.management.internal.cli.shell;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CommandRequest;
import org.apache.geode.management.internal.cli.CommandResponseBuilder;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.shell.core.CommandMarker;

/**
 * GfshExecutionStrategyTest - Includes tests to for GfshExecutionStrategyTest
 */
@Category(UnitTest.class)
public class GfshExecutionStrategyJUnitTest {
  private static final String COMMAND1_SUCESS = "Command1 Executed successfully";
  private static final String COMMAND2_SUCESS = "Command2 Executed successfully";

  private Gfsh gfsh;
  private GfshParseResult parsedCommand;
  private GfshExecutionStrategy gfshExecutionStrategy;

  @Before
  public void before() {
    gfsh = mock(Gfsh.class);
    parsedCommand = mock(GfshParseResult.class);
    gfshExecutionStrategy = new GfshExecutionStrategy(gfsh);
  }

  /**
   * tests execute offline command
   */
  @Test
  public void testOfflineCommand() throws Exception {
    when(parsedCommand.getMethod()).thenReturn(Commands.class.getDeclaredMethod("offlineCommand"));
    when(parsedCommand.getInstance()).thenReturn(new Commands());
    Result result = (Result) gfshExecutionStrategy.execute(parsedCommand);
    assertThat(result.nextLine().trim()).isEqualTo(COMMAND1_SUCESS);
  }

  /**
   * tests execute online command
   */
  @Test
  public void testOnLineCommandWhenGfshisOffLine() throws Exception {
    when(parsedCommand.getMethod()).thenReturn(Commands.class.getDeclaredMethod("onlineCommand"));
    when(parsedCommand.getInstance()).thenReturn(new Commands());
    when(gfsh.isConnectedAndReady()).thenReturn(false);
    Result result = (Result) gfshExecutionStrategy.execute(parsedCommand);
    assertThat(result).isNull();
  }

  @Test
  public void testOnLineCommandWhenGfshisOnLine() throws Exception {
    when(parsedCommand.getMethod()).thenReturn(Commands.class.getDeclaredMethod("onlineCommand"));
    when(parsedCommand.getInstance()).thenReturn(new Commands());
    when(gfsh.isConnectedAndReady()).thenReturn(true);
    OperationInvoker invoker = mock(OperationInvoker.class);

    Result offLineResult = new Commands().onlineCommand();
    String jsonResult = CommandResponseBuilder.createCommandResponseJson("memberName",
        (CommandResult) offLineResult);
    when(invoker.processCommand(any(CommandRequest.class))).thenReturn(jsonResult);
    when(gfsh.getOperationInvoker()).thenReturn(invoker);
    Result result = (Result) gfshExecutionStrategy.execute(parsedCommand);
    assertThat(result.nextLine().trim()).isEqualTo(COMMAND2_SUCESS);
  }

  /**
   * represents class for dummy methods
   */
  public static class Commands implements CommandMarker {
    @CliMetaData(shellOnly = true)
    public Result offlineCommand() {
      return ResultBuilder.createInfoResult(COMMAND1_SUCESS);
    }

    @CliMetaData(shellOnly = false)
    public Result onlineCommand() {
      return ResultBuilder.createInfoResult(COMMAND2_SUCESS);
    }
  }
}