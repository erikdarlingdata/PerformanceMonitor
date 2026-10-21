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
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the one gated test here mints its own scratch database through ScratchPostgres and never touches another
   database on the server. */
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
    public void McpOff_IsAWarning_ThatNamesTheFix()
    {
        var found = Messages(On(), enabled: false, GoodToken, null, routes: 1);
        var message = Assert.Single(found);
        Assert.Equal(DarlingAgentDispatchChecks.McpOffMessage, message);
        Assert.Contains("--enable-mcp", message, StringComparison.Ordinal);
    }

    [Fact]
    public void NoToken_IsAWarning_ThatNamesTheKeys()
    {
        var message = Assert.Single(Messages(On(), enabled: true, token: "", unreadable: null, routes: 1));
        Assert.Contains("mcp.network.encryptedToken", message, StringComparison.Ordinal);
        Assert.Contains("mcp.network.token", message, StringComparison.Ordinal);
    }

    [Fact]
    public void ShortToken_IsAWarning()
    {
        var message = Assert.Single(Messages(On(), enabled: true, token: "short", unreadable: null, routes: 1));
        Assert.Contains("investigation tools stay off", message, StringComparison.Ordinal);
    }

    [Fact]
    public void UnreadableToken_IsAWarning_ThatSaysWhy()
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
    public void NoAgentRoute_IsANote_NotAWarning()
    {
        var notes = Messages(On(), enabled: true, GoodToken, null, routes: 0, DarlingAgentDispatchChecks.AgentDispatchSeverity.Note);
        Assert.Equal(DarlingAgentDispatchChecks.NoAgentRouteMessage, Assert.Single(notes));
        Assert.Empty(Messages(On(), enabled: true, GoodToken, null, routes: 0, DarlingAgentDispatchChecks.AgentDispatchSeverity.Warning));
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
    public async Task ValidateConfig_Seam_PrintsWarningsOnOutput_ForEveryConfigThatRanBefore()
    {
        var config = new DarlingConfig { Mcp = On(writes: true) };
        config.Mcp.Enabled = false;
        var output = new StringWriter();

        await DarlingCliCommands.ReportAgentDispatchAsync(config, output, TestContext.Current.CancellationToken);

        var lines = output.ToString().Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        Assert.Contains("WARNING: " + DarlingAgentDispatchChecks.WritesWinMessage, lines);
        Assert.Contains("WARNING: " + DarlingAgentDispatchChecks.McpOffMessage, lines);
    }

    [Fact]
    public async Task ValidateConfig_Seam_SaysNothing_WhenInvestigationsAreOff()
    {
        var config = new DarlingConfig();
        var output = new StringWriter();

        await DarlingCliCommands.ReportAgentDispatchAsync(config, output, TestContext.Current.CancellationToken);

        Assert.Equal("", output.ToString());
    }

    [Fact]
    public async Task ValidateConfig_Seam_NoToken_WarnsAndNamesTheKeys()
    {
        var config = new DarlingConfig { Mcp = On() };
        config.Mcp.Enabled = true;
        var output = new StringWriter();

        await DarlingCliCommands.ReportAgentDispatchAsync(config, output, TestContext.Current.CancellationToken);

        Assert.Contains("WARNING: " + DarlingAgentDispatchChecks.NoTokenMessage, output.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task ValidateConfigAsync_WithAllThreeBadSetups_PrintsWarnings_AndStillExitsZero_Gated()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to run the live --validate-config exit-code test.");

        var ct = TestContext.Current.CancellationToken;
        /* #1776 own-store: ScratchPostgres makes this class's own database. */
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await using var switchOff = new NpgsqlCommand("UPDATE config.config_service SET mcp_enabled = FALSE WHERE id = 1", connection);
            await switchOff.ExecuteNonQueryAsync(ct);
        }

        var root = Directory.CreateTempSubdirectory("darling-validate-dispatch-");
        try
        {
            /* Investigations and writes both on, no token, MCP off (the store's switch is off): the three
               setups that ran before this check existed. An upgrade must not start failing a validation on them. */
            var configPath = Path.Combine(root.FullName, "darling.json");
            await File.WriteAllTextAsync(configPath, $$"""
                {
                  "postgres": { "connectionString": {{JsonSerializer.Serialize(scratch.ConnectionString)}} },
                  "mcp": { "enabled": false, "allowInvestigations": true, "allowWrites": true },
                  "servers": [ { "name": "alpha-sql-01", "host": "alpha-sql-01" } ]
                }
                """);

            var output = new StringWriter();
            var error = new StringWriter();
            var exit = await DarlingCliCommands.ValidateConfigAsync(configPath, output, error, ct);

            Assert.Equal(0, exit);
            Assert.Contains("WARNING: " + DarlingAgentDispatchChecks.WritesWinMessage, output.ToString(), StringComparison.Ordinal);
            Assert.Contains("WARNING: " + DarlingAgentDispatchChecks.McpOffMessage, output.ToString(), StringComparison.Ordinal);
            Assert.DoesNotContain("cannot work", error.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(root.FullName, recursive: true);
        }
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
