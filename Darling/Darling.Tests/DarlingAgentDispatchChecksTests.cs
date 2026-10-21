/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6263 (part of #5632): the setups where <c>mcp.allowInvestigations</c> is on but no agent can work, as the start-up log
/// and <c>--validate-config</c> report them, and the connect-time remediation login lines.
/// </summary>
public sealed class DarlingAgentDispatchChecksTests
{
    private const string GoodToken = "agent-dispatch-token-0123456789abcdef";

    private static McpConfig On(bool writes = false) => new() { AllowInvestigations = true, AllowWrites = writes };

    private static string[] Messages(McpConfig mcp, bool enabled, string? token, string? unreadable, int? routes,
        DarlingAgentDispatchChecks.AgentDispatchSeverity? only = null) =>
        DarlingAgentDispatchChecks.Evaluate(mcp, enabled, token, unreadable, routes)
            .Where(f => only is null || f.Severity == only)
            .Select(f => f.Message)
            .ToArray();

    [Fact]
    public void InvestigationsOff_SaysNothing_EvenWhenEverythingElseIsWrong()
    {
        Assert.Empty(Messages(new McpConfig { AllowWrites = true }, enabled: false, token: null, unreadable: null, routes: 0));
    }

    [Fact]
    public void WorkingSetup_HasNoFindings()
    {
        Assert.Empty(Messages(On(), enabled: true, GoodToken, null, routes: 2));
    }

    [Fact]
    public void McpOff_IsAProblem_ThatNamesTheFix()
    {
        var found = Messages(On(), enabled: false, GoodToken, null, routes: 1);
        var message = Assert.Single(found);
        Assert.Equal(DarlingAgentDispatchChecks.McpOffMessage, message);
        Assert.Contains("--enable-mcp", message, StringComparison.Ordinal);
    }

    [Fact]
    public void NoToken_IsAProblem_ThatNamesTheKeys()
    {
        var message = Assert.Single(Messages(On(), enabled: true, token: "", unreadable: null, routes: 1));
        Assert.Contains("mcp.network.encryptedToken", message, StringComparison.Ordinal);
        Assert.Contains("mcp.network.token", message, StringComparison.Ordinal);
    }

    [Fact]
    public void ShortToken_IsAProblem()
    {
        var message = Assert.Single(Messages(On(), enabled: true, token: "short", unreadable: null, routes: 1));
        Assert.Contains("investigation tools stay off", message, StringComparison.Ordinal);
    }

    [Fact]
    public void UnreadableToken_IsAProblem_ThatSaysWhy()
    {
        var message = Assert.Single(Messages(On(), enabled: true, token: "", unreadable: "the file is missing", routes: 1));
        Assert.Contains("the file is missing", message, StringComparison.Ordinal);
    }

    [Fact]
    public void WritesAndInvestigationsBothOn_SaysWritesWin_AndDoesNotAlsoComplainAboutTheToken()
    {
        var found = Messages(On(writes: true), enabled: true, token: "", unreadable: null, routes: 0);
        var message = Assert.Single(found);
        Assert.Equal(DarlingAgentDispatchChecks.WritesWinMessage, message);
        Assert.Contains("mcp.allowWrites wins", message, StringComparison.Ordinal);
        Assert.Contains("set mcp.allowWrites to false", message, StringComparison.Ordinal);
    }

    [Fact]
    public void NoAgentRoute_IsAHint_NotAProblem()
    {
        var hints = Messages(On(), enabled: true, GoodToken, null, routes: 0, DarlingAgentDispatchChecks.AgentDispatchSeverity.Hint);
        Assert.Equal(DarlingAgentDispatchChecks.NoAgentRouteMessage, Assert.Single(hints));
        Assert.Empty(Messages(On(), enabled: true, GoodToken, null, routes: 0, DarlingAgentDispatchChecks.AgentDispatchSeverity.Problem));
    }

    [Fact]
    public void UnknownRouteCount_GivesNoHint()
    {
        Assert.Empty(Messages(On(), enabled: true, GoodToken, null, routes: null));
    }

    [Fact]
    public void ResolveToken_ReadsAnEnvReference_AndReportsNoneAsEmpty()
    {
        Assert.Equal(("", null), DarlingAgentDispatchChecks.ResolveToken(new McpConfig()));

        var name = "DARLING_TEST_6263_TOKEN_" + Guid.NewGuid().ToString("N");
        Environment.SetEnvironmentVariable(name, GoodToken);
        try
        {
            var mcp = new McpConfig { Network = new McpNetworkConfig { Token = "env:" + name } };
            Assert.Equal((GoodToken, null), DarlingAgentDispatchChecks.ResolveToken(mcp));
        }
        finally
        {
            Environment.SetEnvironmentVariable(name, null);
        }
    }

    [Fact]
    public async Task ValidateConfig_Seam_ReportsProblemsOnError_AndExitsAsAProblem()
    {
        var config = new DarlingConfig { Mcp = On(writes: true) };
        config.Mcp.Enabled = false;
        var output = new StringWriter();
        var error = new StringWriter();

        var problem = await DarlingCliCommands.ReportAgentDispatchAsync(config, output, error, TestContext.Current.CancellationToken);

        Assert.True(problem);
        Assert.Contains("Agent dispatch cannot work", error.ToString(), StringComparison.Ordinal);
        Assert.Contains(DarlingAgentDispatchChecks.WritesWinMessage, error.ToString(), StringComparison.Ordinal);
        Assert.Contains(DarlingAgentDispatchChecks.McpOffMessage, error.ToString(), StringComparison.Ordinal);
        Assert.Equal("", output.ToString());
    }

    [Fact]
    public async Task ValidateConfig_Seam_SaysNothing_WhenInvestigationsAreOff()
    {
        var config = new DarlingConfig();
        var output = new StringWriter();
        var error = new StringWriter();

        var problem = await DarlingCliCommands.ReportAgentDispatchAsync(config, output, error, TestContext.Current.CancellationToken);

        Assert.False(problem);
        Assert.Equal("", output.ToString() + error.ToString());
    }

    [Fact]
    public async Task ValidateConfig_Seam_NoToken_NamesTheKeys()
    {
        var config = new DarlingConfig { Mcp = On() };
        config.Mcp.Enabled = true;
        var error = new StringWriter();

        var problem = await DarlingCliCommands.ReportAgentDispatchAsync(config, new StringWriter(), error, TestContext.Current.CancellationToken);

        Assert.True(problem);
        Assert.Contains(DarlingAgentDispatchChecks.NoTokenMessage, error.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void RemediationLoginLine_SqlServer_NamesLevel3AndTheSwitch_AndDoesNotSayItIsUnused()
    {
        var logger = new CapturingTestLogger();
        var server = new MonitoredServer
        {
            Name = "alpha-sql-01",
            Host = "alpha-sql-01",
            RemediationUsername = "pm_remediate",
            RemediationEncryptedPassword = "env:PM_REMEDIATE_PASSWORD",
        };

        DarlingServerConnector.WarnIfRemediationCredentialIsInert(server, logger);

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Information));
        Assert.Contains("Level 3", logger.Joined, StringComparison.Ordinal);
        Assert.Contains("switch", logger.Joined, StringComparison.Ordinal);
        Assert.DoesNotContain("Nothing will use it", logger.Joined, StringComparison.Ordinal);
    }

    [Fact]
    public void RemediationLoginLine_PostgreSql_SaysKillSessionUsesIt_AndDoesNotTellYouToRemoveIt()
    {
        var logger = new CapturingTestLogger();
        var server = new MonitoredServer
        {
            Name = "alpha-pg-01",
            Host = "alpha-pg-01",
            Engine = "postgres",
            RemediationUsername = "pm_remediate",
            RemediationEncryptedPassword = "env:PM_REMEDIATE_PASSWORD",
        };

        DarlingServerConnector.WarnIfRemediationCredentialIsInert(server, logger);

        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
        Assert.Contains("kill_session", logger.Joined, StringComparison.Ordinal);
        Assert.DoesNotContain("Remove it", logger.Joined, StringComparison.Ordinal);
        Assert.DoesNotContain("nothing on a PostgreSQL target", logger.Joined, StringComparison.Ordinal);
    }
}
