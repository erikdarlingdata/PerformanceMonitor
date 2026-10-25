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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT
   [Collection("live-postgres")]. */

/// <summary>
/// A host serving two named agent tokens beside the legacy token, called through the real tools/call route (#6542). Each call below
/// reaches the tools only if the host hands them the served policy and the served agent names, so a dropped argument anywhere on that
/// path (the host's registration, or a tool's own service parameter) fails one of these.
/// </summary>
public sealed class InvestigationNamedTokenHostLiveTests
{
    private const string First = "triage-bot";
    private const string Second = "other-bot";
    private const string FirstBearer = "agent-token-value-host-live-first-0001";
    private const string SecondBearer = "agent-token-value-host-live-second-002";
    private const string LegacyBearer = "legacy-token-value-host-live-0003";
    private const int ServerId = 7_654_401;

    private const string Evidence =
        "[{\"tool\":\"get_blocking\",\"arguments\":{},\"key_numbers\":\"412 s\",\"basis\":\"verified\"}]";

    private sealed record Ctx(Microsoft.AspNetCore.TestHost.TestServer Server, NpgsqlConnection Connection, string RequestId, string ClaimId,
        CancellationToken Ct);

    private static async Task RunAsync(Func<Ctx, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the named token host tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, "HostLiveSrv", ct);
        await ExecAsync(connection, "INSERT INTO config.config_notification_routes (metric_match, agent_level) VALUES ('Blocking', 3)", ct);

        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        var store = new InvestigationRequestStore(source, 30, () => DateTime.UtcNow);
        var alert = new InvestigationAlert(1, ServerId, "HostLiveSrv", "blocking", "Blocking", DateTime.UtcNow.AddSeconds(-5),
            Array.Empty<string>(), null, 20, 100, LeaseMinutes: 15, ExpiryMinutes: 240) { AutonomyLevel = 3 };
        var requestId = (await store.OpenOrJoinAsync(alert, ct)).RequestId!.Value;

        var mcp = new McpConfig
        {
            AllowInvestigations = true,
            Tokens = new List<McpTokenEntry>
            {
                new() { Name = First, Token = FirstBearer, Role = "agent" },
                new() { Name = Second, Token = SecondBearer, Role = "agent" },
            },
        };
        var legacy = DarlingMcpWritePolicy.ResolveForStart(mcp, LegacyBearer, out _);
        var tokens = DarlingMcpTokenSet.Resolve(mcp, LegacyBearer, legacy, null);
        var owner = new StoreOwnerSource();
        owner.Publish(source);
        using var server = await DarlingMcpWritePolicyTests.BuildServer(mcp, LegacyBearer, scratch.ConnectionString, tokens, owner: owner);

        /* The first token claims through the host, as an agent does. */
        var claimed = await CallAsync(server, "claim_investigation_request", new JsonObject { ["request_id"] = requestId.ToString(), ["agent_id"] = First }, FirstBearer);
        Assert.Equal("claimed", claimed.GetProperty("status").GetString());
        var claimId = claimed.GetProperty("claim_id").GetString()!;

        await body(new Ctx(server, connection, requestId.ToString(), claimId, ct));
    }

    private static async Task<JsonElement> CallAsync(Microsoft.AspNetCore.TestHost.TestServer server, string tool, JsonObject arguments, string bearer)
    {
        var (_, body) = await DarlingMcpWritePolicyTests.ToolsCallAsync(server, tool, arguments.ToJsonString(), bearer);
        var dataLine = body.Split('\n').FirstOrDefault(l => l.StartsWith("data:", StringComparison.Ordinal));
        using var document = JsonDocument.Parse(dataLine is null ? body : dataLine["data:".Length..].Trim());
        var text = document.RootElement.GetProperty("result").GetProperty("content")[0].GetProperty("text").GetString()!;
        return JsonDocument.Parse(text).RootElement.Clone();
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    /// <summary>How many refused rows carry <paramref name="reason"/> under <paramref name="actor"/>.</summary>
    private static Task<string?> RefusedAsync(Ctx c, string actor, string reason) =>
        ScalarAsync(c.Connection,
            $"SELECT count(*) FROM config.investigation_journal WHERE event = 'refused' AND actor = 'agent:{actor}' AND reason = '{reason}'", c.Ct);

    private JsonObject SubmitArguments(Ctx c) => new()
    {
        ["request_id"] = c.RequestId,
        ["claim_id"] = c.ClaimId,
        ["report_format"] = 1,
        ["verdict"] = "real",
        ["root_cause"] = "a cause",
        ["evidence"] = Evidence,
        ["summary"] = "a summary",
    };

    [Fact]
    public async Task TheSecondToken_SubmittingOnTheFirstTokensClaim_IsRefusedAndJournaledAsTheSecond()
    {
        await RunAsync(async c =>
        {
            var answer = await CallAsync(c.Server, "submit_investigation_report", SubmitArguments(c), SecondBearer);

            Assert.Equal(DarlingMcpInvestigationTools.ClaimNotToken, answer.GetProperty("status").GetString());
            Assert.Equal("1", await RefusedAsync(c, Second, "submit:" + DarlingMcpInvestigationTools.ClaimNotToken));
            Assert.Equal("0", await ScalarAsync(c.Connection, "SELECT count(*) FROM config.investigation_reports", c.Ct));
        });
    }

    [Fact]
    public async Task TheSecondToken_ReadingEvidenceOnTheFirstTokensClaim_IsRefusedAndJournaledAsTheSecond()
    {
        await RunAsync(async c =>
        {
            var answer = await CallAsync(c.Server, "get_investigation_evidence",
                new JsonObject { ["request_id"] = c.RequestId, ["claim_id"] = c.ClaimId }, SecondBearer);

            Assert.Equal(DarlingMcpInvestigationTools.ClaimNotToken, answer.GetProperty("status").GetString());
            Assert.Equal("1", await RefusedAsync(c, Second, "evidence:" + DarlingMcpInvestigationTools.ClaimNotToken));
        });
    }

    [Fact]
    public async Task TheLegacyToken_ClaimingUnderAServedTokensName_IsRefused_AndNothingIsJournaledAsThatToken()
    {
        await RunAsync(async c =>
        {
            var answer = await CallAsync(c.Server, "claim_investigation_request",
                new JsonObject { ["request_id"] = c.RequestId, ["agent_id"] = Second }, LegacyBearer);

            Assert.Equal(DarlingMcpInvestigationTools.AgentIdIsNamedToken, answer.GetProperty("status").GetString());
            Assert.Equal("1", await RefusedAsync(c, "invalid", "claim:" + DarlingMcpInvestigationTools.AgentIdIsNamedToken));
            Assert.Equal("0", await ScalarAsync(c.Connection, $"SELECT count(*) FROM config.investigation_journal WHERE actor = 'agent:{Second}'", c.Ct));
        });
    }

    [Fact]
    public async Task TheLegacyToken_UsingAServedTokensClaim_IsRefused_ForSubmitAndEvidence()
    {
        await RunAsync(async c =>
        {
            var submit = await CallAsync(c.Server, "submit_investigation_report", SubmitArguments(c), LegacyBearer);
            Assert.Equal(DarlingMcpInvestigationTools.AgentIdIsNamedToken, submit.GetProperty("status").GetString());
            Assert.Equal("0", await ScalarAsync(c.Connection, "SELECT count(*) FROM config.investigation_reports", c.Ct));

            var evidence = await CallAsync(c.Server, "get_investigation_evidence",
                new JsonObject { ["request_id"] = c.RequestId, ["claim_id"] = c.ClaimId }, LegacyBearer);
            Assert.Equal(DarlingMcpInvestigationTools.AgentIdIsNamedToken, evidence.GetProperty("status").GetString());

            Assert.Equal("1", await RefusedAsync(c, "invalid", "submit:" + DarlingMcpInvestigationTools.AgentIdIsNamedToken));
            Assert.Equal("1", await RefusedAsync(c, "invalid", "evidence:" + DarlingMcpInvestigationTools.AgentIdIsNamedToken));
        });
    }

    [Fact]
    public async Task TheLegacyTokens_Level2Actions_AsAServedTokensName_AreRefused_OnBothGates()
    {
        await RunAsync(async c =>
        {
            var action = AgentActionArgumentTests.Arg(c.RequestId, c.ClaimId, agentId: First, reason: "identity check");

            /* The agent_action gate, reached through the seat the host registers. */
            var mute = await CallAsync(c.Server, "create_mute_rule",
                new JsonObject { ["server_id"] = ServerId, ["metric_name"] = "Blocking", ["agent_action"] = action }, LegacyBearer);
            Assert.Equal("refused", mute.GetProperty("status").GetString());
            Assert.Equal(DarlingMcpInvestigationTools.AgentIdIsNamedToken, mute.GetProperty("reason").GetString());

            /* The server action gate, reached through the plan force tools' served policy. */
            var kill = await CallAsync(c.Server, DarlingMcpPlanForceTools.KillToolName, new JsonObject
            {
                ["server_name"] = "HostLiveSrv",
                ["session_id"] = 87,
                ["login_time"] = "2026-10-01T12:00:00.0000000Z",
                ["agent_action"] = action,
                ["dry_run"] = true,
            }, LegacyBearer);
            Assert.Contains(DarlingMcpInvestigationTools.AgentIdIsNamedToken, kill.GetRawText(), StringComparison.Ordinal);

            Assert.Equal("2", await RefusedAsync(c, "invalid", DarlingMcpInvestigationTools.AgentIdIsNamedToken));
            Assert.Equal("0", await ScalarAsync(c.Connection,
                $"SELECT count(*) FROM config.investigation_journal WHERE actor = 'agent:{First}' AND event = 'refused'", c.Ct));
        });
    }
}
