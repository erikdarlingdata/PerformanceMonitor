/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.FrontDoor;
using Xunit;

namespace Darling.Tests;

/// <summary>#6840: the front door's tokens resolve like a named MCP token does, and none of them reaches a string.</summary>
public sealed class FrontDoorSecretsTests
{
    private const string Secret = "SECRET-TOKEN-6840";

    private sealed class ListLogger : ILogger
    {
        public List<string> Lines { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => Lines.Add(formatter(state, exception));
    }

    private static FrontDoorConfig Config(FrontDoorStoreConfig store)
    {
        var block = new FrontDoorConfig { Enabled = true };
        block.Stores.Add(store);
        return block;
    }

    [Fact]
    public void APlainToken_Resolves_PerRole_WithNoFallbackFromAgentToOperator()
    {
        var secrets = new DarlingFrontDoorSecrets(Config(new FrontDoorStoreConfig
        {
            Name = "use1",
            McpToken = "op-token",
            WebToken = "web-token",
        }));

        Assert.Equal("op-token", secrets.McpToken("use1", agentRole: false));
        Assert.Null(secrets.McpToken("use1", agentRole: true));
        Assert.Equal("web-token", secrets.WebToken("use1"));
    }

    [Fact]
    public void TheAgentToken_IsItsOwn()
    {
        var secrets = new DarlingFrontDoorSecrets(Config(new FrontDoorStoreConfig { Name = "use1", McpToken = "op", McpAgentToken = "agent" }));
        Assert.Equal("agent", secrets.McpToken("use1", agentRole: true));
        Assert.Equal("op", secrets.McpToken("use1", agentRole: false));
    }

    [Fact]
    public void AStoreName_IsMatchedIgnoringCase_AndAnUnknownStoreAnswersNull()
    {
        var secrets = new DarlingFrontDoorSecrets(Config(new FrontDoorStoreConfig { Name = "Use1", McpToken = "op" }));
        Assert.Equal("op", secrets.McpToken("USE1", agentRole: false));
        Assert.Null(secrets.McpToken("other", agentRole: false));
        Assert.Null(secrets.WebToken("use1"));
    }

    [Fact]
    public void AnEnvReference_IsResolved()
    {
        var name = "DARLING_FD_TEST_" + Guid.NewGuid().ToString("N");
        Environment.SetEnvironmentVariable(name, Secret);
        try
        {
            var secrets = new DarlingFrontDoorSecrets(Config(new FrontDoorStoreConfig { Name = "use1", McpToken = "env:" + name }));
            Assert.Equal(Secret, secrets.McpToken("use1", agentRole: false));
        }
        finally
        {
            Environment.SetEnvironmentVariable(name, null);
        }
    }

    [Fact]
    public void AFileReference_IsResolved()
    {
        var path = Path.Combine(Path.GetTempPath(), "darling-fd-" + Guid.NewGuid().ToString("N") + ".txt");
        File.WriteAllText(path, Secret + Environment.NewLine);
        try
        {
            var secrets = new DarlingFrontDoorSecrets(Config(new FrontDoorStoreConfig { Name = "use1", WebToken = "file:" + path }));
            Assert.Equal(Secret, secrets.WebToken("use1"));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void AnEncryptedToken_WinsOverAPlainOne()
    {
        if (!OperatingSystem.IsWindows())
        {
            Assert.Skip("DPAPI is Windows only.");
        }

        var secrets = new DarlingFrontDoorSecrets(Config(new FrontDoorStoreConfig
        {
            Name = "use1",
            McpEncryptedToken = DarlingSecrets.Protect(Secret),
            McpToken = "plain",
        }));

        Assert.Equal(Secret, secrets.McpToken("use1", agentRole: false));
    }

    [Fact]
    public void AnUnresolvableToken_AnswersNull_AndTheLogNamesTheStoreNotTheValue()
    {
        var logger = new ListLogger();
        var name = "DARLING_FD_TEST_UNSET_" + Guid.NewGuid().ToString("N");
        var secrets = new DarlingFrontDoorSecrets(
            Config(new FrontDoorStoreConfig { Name = "use1", McpToken = "env:" + name, McpAgentToken = "agent-" + Secret }),
            logger);

        Assert.Null(secrets.McpToken("use1", agentRole: false));
        Assert.Null(secrets.McpToken("use1", agentRole: false));

        /* Once per store and kind, not once per call, and the resolved agent token (a different kind) is unaffected. */
        Assert.Single(logger.Lines);
        Assert.Contains("use1", logger.Lines[0]);
        Assert.Equal("agent-" + Secret, secrets.McpToken("use1", agentRole: true));
        Assert.All(logger.Lines, l => Assert.DoesNotContain(Secret, l, StringComparison.Ordinal));
    }

    [Fact]
    public void AResolvedToken_NeverPrints()
    {
        var value = new FrontDoorSecretValue(Secret);
        Assert.DoesNotContain(Secret, value.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain(Secret, $"{value}", StringComparison.Ordinal);

        var secrets = new DarlingFrontDoorSecrets(Config(new FrontDoorStoreConfig { Name = "use1", McpToken = Secret }));
        Assert.DoesNotContain(Secret, secrets.ToString(), StringComparison.Ordinal);
    }
}
