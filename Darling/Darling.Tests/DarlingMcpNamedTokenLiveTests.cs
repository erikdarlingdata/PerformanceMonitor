/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// A named agent token is the agent's identity (#6542), through the real MCP pipeline on a live store: a claim made with the token
/// records the token's name, and a claim that names another agent is refused as <c>agent_id_not_token</c>, journaled under the
/// token's name, and leaves the request unclaimed.
/// </summary>
public sealed class DarlingMcpNamedTokenLiveTests
{
    private const string AgentToken = "agent-token-value-bbbbbbbbbbbbbbb-2";
    private const string OperatorToken = "operator-token-value-aaaaaaaaaaaa-1";
    private const int ServerId = 7_654_201;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static InvestigationAlert Alert() =>
        new(1, ServerId, "NamedTokenSrv", "blocking", "Blocking", DateTime.UtcNow.AddSeconds(-5), new[] { "named-token" }, null, 20, 100,
            LeaseMinutes: 15, ExpiryMinutes: 240);

    /// <summary>The text of a tools/call answer: the tool's own JSON.</summary>
    private static JsonElement AnswerOf(string body)
    {
        var dataLine = body.Split('\n').FirstOrDefault(l => l.StartsWith("data:", StringComparison.Ordinal));
        using var document = JsonDocument.Parse(dataLine is null ? body : dataLine["data:".Length..].Trim());
        var text = document.RootElement.GetProperty("result").GetProperty("content")[0].GetProperty("text").GetString()!;
        return JsonDocument.Parse(text).RootElement.Clone();
    }

    private static async Task<string?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    [Fact]
    public async Task AClaimWithAnAgentToken_RecordsTheTokensName_AndAnotherAgentIdIsRefused()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the named token live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, "NamedTokenSrv", ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var store = new InvestigationRequestStore(dataSource, 30, () => DateTime.UtcNow);
        var requestId = (await store.OpenOrJoinAsync(Alert(), ct)).RequestId!.Value;

        var mcp = new McpConfig
        {
            Tokens = new List<McpTokenEntry>
            {
                new McpTokenEntry { Name = "ops-lead", Token = OperatorToken, Role = "operator" },
                new McpTokenEntry { Name = "triage-bot", Token = AgentToken, Role = "agent" },
            },
        };
        var tokens = DarlingMcpTokenSet.Resolve(mcp, "", DarlingMcpWritePolicy.Effective.Off, null);
        using var server = await DarlingMcpWritePolicyTests.BuildServer(mcp, "", scratch.ConnectionString, tokens);

        /* Another agent's name: refused, nothing claimed, and the journal names the token, not the name it passed. */
        var (_, refusedBody) = await DarlingMcpWritePolicyTests.ToolsCallAsync(
            server, "claim_investigation_request", $"{{\"request_id\":\"{requestId}\",\"agent_id\":\"someone-else\"}}", AgentToken);
        var refused = AnswerOf(refusedBody);
        Assert.Equal("invalid", refused.GetProperty("status").GetString());
        Assert.Contains("triage-bot", refused.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Equal("", await ScalarAsync(connection,
            $"SELECT coalesce(claimed_by, '') FROM config.investigation_requests WHERE request_id = '{requestId}'", ct));
        Assert.Equal("1", await ScalarAsync(connection,
            $"SELECT count(*) FROM config.investigation_journal WHERE request_id = '{requestId}' AND event = 'refused' " +
            $"AND actor = 'agent:triage-bot' AND reason = 'claim:{DarlingMcpInvestigationTools.AgentIdNotToken}'", ct));
        Assert.Equal("0", await ScalarAsync(connection,
            "SELECT count(*) FROM config.investigation_journal WHERE actor = 'agent:someone-else'", ct));

        /* The token's own name claims. */
        var (_, claimedBody) = await DarlingMcpWritePolicyTests.ToolsCallAsync(
            server, "claim_investigation_request", $"{{\"request_id\":\"{requestId}\",\"agent_id\":\"triage-bot\"}}", AgentToken);
        Assert.Equal("claimed", AnswerOf(claimedBody).GetProperty("status").GetString());
        Assert.Equal("triage-bot", await ScalarAsync(connection,
            $"SELECT claimed_by FROM config.investigation_requests WHERE request_id = '{requestId}'", ct));
        Assert.Equal("1", await ScalarAsync(connection,
            $"SELECT count(*) FROM config.investigation_journal WHERE request_id = '{requestId}' AND event = 'claimed' AND actor = 'agent:triage-bot'", ct));
    }
}
