/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT
   [Collection("live-postgres")]. */

/// <summary>
/// #6542 part 2 (items 7 and 8 of #6556's security review): a named agent token is the agent's identity at every gate. A mismatched
/// <c>agent_id</c> at <see cref="DarlingAgentActionGate"/> and at <see cref="ServerActionGate"/> (force, unforce, evict and kill) is refused
/// as <c>agent_id_not_token</c> and journaled under the token's name. A refusal made before the gate (a PostgreSQL target, a bad handle,
/// a mute with no metric, a bad argument) is journaled under the token's name too, never under the <c>agent_id</c> the caller passed.
/// No SQL Server is needed: every refusal here comes before a remediation login is opened.
/// </summary>
public sealed class AgentTokenIdentityGateLiveTests
{
    private const string Token = "triage-bot";
    private const string Other = "someone-else";

    /* A fixed clock for every store a refusal goes through: the once-key ends in the hour, so a run that crossed an hour between two
       calls would write a second row. The same way AgentActionGateTests fixes _now. */
    private static readonly DateTime Fixed = new(2026, 10, 1, 12, 30, 0, DateTimeKind.Utc);
    private static DateTime FixedNow() => Fixed;

    private static readonly DarlingMcpWritePolicy.Effective AgentPolicy = new(false, false, true, Token);

    private sealed record Ctx(NpgsqlDataSource Source, NpgsqlConnection Connection, string RequestId, string ClaimId, CancellationToken Ct, string ConnectionString = "")
    {
        public string Arg(string agentId) => AgentActionArgumentTests.Arg(RequestId, ClaimId, agentId: agentId, reason: "identity check");
    }

    private static async Task RunAsync(Func<Ctx, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the agent token identity gate tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await ExecAsync(connection,
            "INSERT INTO config.config_monitored_servers (server_id, name, host, encrypt_mode, trust_server_certificate) "
            + "VALUES (7, 'srv-7', 'sql-host', 'Optional', TRUE), (8, 'srv-8', 'pg-host', 'Optional', TRUE)", ct);
        await ExecAsync(connection, "INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, sql_major_version, engine_kind, created_date, modified_date) "
            + "VALUES (7, 'srv-7', 'srv-7', TRUE, 16, 'sqlserver', now(), now()), (8, 'srv-8', 'srv-8', TRUE, NULL, 'postgres', now(), now())", ct);
        await ExecAsync(connection, "INSERT INTO config.config_notification_routes (metric_match, agent_level, action_approval) VALUES ('Blocking', 3, 'off')", ct);

        var builder = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath };
        await using var source = NpgsqlDataSource.Create(builder.ConnectionString);
        var store = new InvestigationRequestStore(source, 30);
        var alert = new InvestigationAlert(1, 7, "srv-7", "blocking", "Blocking", DateTime.UtcNow, Array.Empty<string>(), null, 20, 100,
            LeaseMinutes: 15, ExpiryMinutes: 240) { AutonomyLevel = 3 };
        var opened = await store.OpenOrJoinAsync(alert, ct);
        var claim = await store.ClaimAsync(opened.RequestId!.Value, Token, ct);
        await body(new Ctx(source, connection, opened.RequestId.Value.ToString(), claim.ClaimId!.Value.ToString(), ct, builder.ConnectionString));
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

    private static string? ReasonOf(string answer) => (string?)JsonNode.Parse(answer)!["reason"];

    /// <summary>The refused rows for <paramref name="tool"/> and <paramref name="code"/>, as "token|other": how many name the token and
    /// how many name the agent_id the caller passed.</summary>
    private static async Task<string?> RefusedActorsAsync(Ctx c, string tool, string code) =>
        await ScalarAsync(c.Connection,
            "SELECT count(*) FILTER (WHERE actor = 'agent:" + Token + "')::text || '|' || count(*) FILTER (WHERE actor = 'agent:" + Other + "')::text "
            + $"FROM config.investigation_journal WHERE event = 'refused' AND reason = '{code}' AND detail ->> 'tool' = '{tool}'", c.Ct);

    [Fact]
    public async Task TheLevel2Gate_RefusesAnotherAgentsId_AndJournalsTheTokensName()
    {
        await RunAsync(async c =>
        {
            var store = new InvestigationRequestStore(c.Source, 30);
            var wrote = false;
            var outcome = await DarlingAgentActionGate.RunAsync(AgentPolicy, c.Source, store, NullLogger.Instance, "create_mute_rule", MuteRuleImage.Kind,
                c.Arg(Other), new AgentActionTarget(7, "srv-7", null, null), null,
                (_, _, _) => Task.FromResult<string?>(null),
                (_, _, _) => { wrote = true; return Task.FromResult<string?>("{}"); }, c.Ct);

            Assert.Equal(DarlingMcpInvestigationTools.AgentIdNotToken, outcome.Refusal?.Reason);
            Assert.False(wrote);
            Assert.Equal("1|0", await RefusedActorsAsync(c, "create_mute_rule", DarlingMcpInvestigationTools.AgentIdNotToken));
        });
    }

    [Theory]
    [InlineData(DarlingMcpPlanForceTools.ForceToolName)]
    [InlineData(DarlingMcpPlanForceTools.UnforceToolName)]
    [InlineData(DarlingMcpPlanForceTools.EvictToolName)]
    [InlineData(DarlingMcpPlanForceTools.KillToolName)]
    public async Task TheServerActionGate_RefusesAnotherAgentsId_ForEachTool_AndJournalsTheTokensName(string tool)
    {
        await RunAsync(async c =>
        {
            var answer = await CallAsync(c, tool, "srv-7", Other);

            Assert.Equal(DarlingMcpInvestigationTools.AgentIdNotToken, ReasonOf(answer));
            Assert.Equal("1|0", await RefusedActorsAsync(c, tool, DarlingMcpInvestigationTools.AgentIdNotToken));
            Assert.Equal("0", await ScalarAsync(c.Connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'action_called'", c.Ct));
        });
    }

    [Theory]
    [InlineData(DarlingMcpPlanForceTools.ForceToolName)]
    [InlineData(DarlingMcpPlanForceTools.EvictToolName)]
    public async Task APostgresTargetRefusal_IsJournaledUnderTheTokensName_NotTheAgentIdPassed(string tool)
    {
        await RunAsync(async c =>
        {
            var answer = await CallAsync(c, tool, "srv-8", Other);

            Assert.Equal(DarlingMcpPlanForceTools.PostgresTarget, ReasonOf(answer));
            Assert.Equal("1|0", await RefusedActorsAsync(c, tool, DarlingMcpPlanForceTools.PostgresTarget));
        });
    }

    [Fact]
    public async Task EveryRefusalBeforeTheGate_IsJournaledUnderTheTokensName()
    {
        await RunAsync(async c =>
        {
            /* RefuseBeforeGateAsync: a handle that does not parse. */
            var evict = await DarlingMcpPlanForceTools.RunEvictAsync(c.Source, c.Source, null, "srv-7", "not-a-handle", c.Arg(Other), true, c.Ct, Token);
            Assert.Equal(PerformanceMonitor.Darling.Remediation.RemediationResult.Reasons.BadHandle, ReasonOf(evict));
            Assert.Equal("1|0", await RefusedActorsAsync(c, DarlingMcpPlanForceTools.EvictToolName, PerformanceMonitor.Darling.Remediation.RemediationResult.Reasons.BadHandle));

            /* RefuseEarlyAsync: create_mute_rule with no metric. */
            var seat = new DarlingAgentActionSeat(AgentPolicy);
            var mute = await DarlingMcpAgentActions.CreateMuteAsync(seat, c.Source, null, c.Arg(Other), null, null, null, null, null, null,
                "why", null, 7, null, null, null, c.Ct);
            Assert.Equal(DarlingMcpAgentActions.MuteNeedsMetric, ReasonOf(mute));
            Assert.Equal("1|0", await RefusedActorsAsync(c, "create_mute_rule", DarlingMcpAgentActions.MuteNeedsMetric));

            /* LooseAgentId: an argument the gate cannot parse (no evidence) still names the token, not its agent_id. */
            var bad = new JsonObject { ["request_id"] = c.RequestId, ["claim_id"] = c.ClaimId, ["agent_id"] = Other, ["reason"] = "x" }.ToJsonString();
            var store = new InvestigationRequestStore(c.Source, 30);
            var outcome = await DarlingAgentActionGate.RunAsync(AgentPolicy, c.Source, store, NullLogger.Instance, "update_mute_rule", MuteRuleImage.Kind,
                bad, new AgentActionTarget(7, "srv-7", null, null), null,
                (_, _, _) => Task.FromResult<string?>(null), (_, _, _) => Task.FromResult<string?>("{}"), c.Ct);
            Assert.NotNull(outcome.Refusal);
            Assert.Equal("1|0", await RefusedActorsAsync(c, "update_mute_rule", outcome.Refusal!.Reason));

            Assert.Equal("0", await ScalarAsync(c.Connection, $"SELECT count(*) FROM config.investigation_journal WHERE actor = 'agent:{Other}'", c.Ct));
        });
    }

    /// <summary>An agent_action the gate cannot parse (no evidence) is refused at each place that reads it first, and the row names the
    /// token, not the agent_id the argument carried and not the invalid-actor placeholder.</summary>
    [Theory]
    [InlineData(DarlingMcpPlanForceTools.ForceToolName, "srv-7")]
    [InlineData(DarlingMcpPlanForceTools.ForceToolName, "srv-8")]
    [InlineData(DarlingMcpPlanForceTools.EvictToolName, "srv-7")]
    [InlineData(DarlingMcpPlanForceTools.EvictToolName, "srv-8")]
    public async Task ARefusedBadAgentAction_IsJournaledUnderTheTokensName(string tool, string server)
    {
        await RunAsync(async c =>
        {
            var answer = await CallBadAsync(c, tool, server, Token);

            Assert.Equal("1|0", await RefusedActorsAsync(c, tool, ReasonOf(answer)!));
            Assert.Equal("0", await ScalarAsync(c.Connection, $"SELECT count(*) FROM config.investigation_journal WHERE actor = 'agent:{InvestigationRules.InvalidAgentId}'", c.Ct));
        });
    }

    [Theory]
    [InlineData(DarlingMcpPlanForceTools.ForceToolName, "srv-7")]
    [InlineData(DarlingMcpPlanForceTools.ForceToolName, "srv-8")]
    [InlineData(DarlingMcpPlanForceTools.EvictToolName, "srv-7")]
    [InlineData(DarlingMcpPlanForceTools.EvictToolName, "srv-8")]
    public async Task TwoTokensRefusedBadAgentActions_AreTwoJournalRows(string tool, string server)
    {
        await RunAsync(async c =>
        {
            var first = await CallBadAsync(c, tool, server, Token);
            await CallBadAsync(c, tool, server, "second-bot");
            await CallBadAsync(c, tool, server, Token);

            Assert.Equal(
                "2",
                await ScalarAsync(c.Connection,
                    $"SELECT count(*) FROM config.investigation_journal WHERE event = 'refused' AND reason = '{ReasonOf(first)}' AND detail ->> 'tool' = '{tool}'", c.Ct));
            Assert.Equal("1", await ScalarAsync(c.Connection,
                $"SELECT count(*) FROM config.investigation_journal WHERE event = 'refused' AND actor = 'agent:second-bot' AND detail ->> 'tool' = '{tool}'", c.Ct));
        });
    }

    private const string ValidHandle = "0x06000700AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    /// <summary>A call whose agent_action does not parse (no evidence), on the store clock <see cref="Fixed"/>. <paramref name="handle"/> picks
    /// the evict path: a valid handle reaches the server action gate, a bad one is refused before it.</summary>
    private static Task<string> CallBadAsync(Ctx c, string tool, string server, string? tokenName, string agentId = Other, string handle = ValidHandle)
    {
        var bad = new JsonObject { ["request_id"] = c.RequestId, ["claim_id"] = c.ClaimId, ["agent_id"] = agentId, ["reason"] = "x" }.ToJsonString();
        return tool == DarlingMcpPlanForceTools.ForceToolName
            ? DarlingMcpPlanForceTools.RunAsync(c.Source, c.Source, null, true, server, "AppDb", 11, 2, bad, true, c.Ct, tokenName, FixedNow)
            : DarlingMcpPlanForceTools.RunEvictAsync(c.Source, c.Source, null, server, handle, bad, true, c.Ct, tokenName, FixedNow);
    }

    private static Task<string?> RefusedRowsAsync(Ctx c, string tool, string extraWhere = "") =>
        ScalarAsync(c.Connection,
            $"SELECT count(*) FROM config.investigation_journal WHERE event = 'refused' AND detail ->> 'tool' = '{tool}' {extraWhere}", c.Ct);

    /// <summary>With no named agent token (the legacy token, or a named operator token) the agent_id is free text, so the once-key must not
    /// be built on it: refused calls that differ only in the agent_id they carry journal ONE row for the hour, not one per id.</summary>
    [Theory]
    [InlineData(DarlingMcpPlanForceTools.ForceToolName, ValidHandle)]
    [InlineData(DarlingMcpPlanForceTools.EvictToolName, ValidHandle)]
    [InlineData(DarlingMcpPlanForceTools.EvictToolName, "not-a-handle")]
    public async Task RefusedCalls_WithNoNamedToken_AndDifferentAgentIds_AreOneJournalRow(string tool, string handle)
    {
        await RunAsync(async c =>
        {
            await CallBadAsync(c, tool, "srv-7", null, "agent-one", handle);
            await CallBadAsync(c, tool, "srv-7", null, "agent-two", handle);
            await CallBadAsync(c, tool, "srv-7", null, "agent-three", handle);

            Assert.Equal("1", await RefusedRowsAsync(c, tool));
        });
    }

    /// <summary>With no token name, a refused argument that does not parse still journals the agent_id and the request id it carried: the
    /// fallbacks at ServerActionGate and the two places in DarlingMcpPlanForceTools. Dropping the agent_id fallback writes the invalid-actor
    /// placeholder; dropping the request id fallback writes a row with no request.</summary>
    [Theory]
    [InlineData(DarlingMcpPlanForceTools.ForceToolName, ValidHandle)]
    [InlineData(DarlingMcpPlanForceTools.EvictToolName, ValidHandle)]
    [InlineData(DarlingMcpPlanForceTools.EvictToolName, "not-a-handle")]
    public async Task ARefusedBadAgentAction_WithNoToken_IsJournaledUnderTheAgentIdAndRequestItCarried(string tool, string handle)
    {
        await RunAsync(async c =>
        {
            var answer = await CallBadAsync(c, tool, "srv-7", null, Other, handle);

            Assert.Equal("1", await RefusedRowsAsync(c, tool, $"AND reason = '{ReasonOf(answer)}' AND actor = 'agent:{Other}' AND request_id = '{c.RequestId}'"));
        });
    }

    /// <summary>The two store paths (Level 2 gate and server action gate) take the token name from the call: with none, refusals that differ
    /// only in the agent_id journal one row, each under its own agent_id; with a named token they stay one row per token.</summary>
    [Fact]
    public async Task TheStorePaths_KeyARefusalOnTheVerifiedToken_NotOnTheAgentId()
    {
        await RunAsync(async c =>
        {
            var store = new InvestigationRequestStore(c.Source, 30, FixedNow);
            var unbound = new DarlingMcpWritePolicy.Effective(false, false, true, null);
            foreach (var agentId in new[] { "agent-one", "agent-two" })
            {
                var outcome = await DarlingAgentActionGate.RunAsync(unbound, c.Source, store, NullLogger.Instance, "update_mute_rule", MuteRuleImage.Kind,
                    c.Arg(agentId), new AgentActionTarget(7, "srv-7", null, null), null,
                    (_, _, _) => Task.FromResult<string?>(null), (_, _, _) => Task.FromResult<string?>("{}"), c.Ct);
                Assert.NotNull(outcome.Refusal);
            }

            Assert.Equal("1", await RefusedRowsAsync(c, "update_mute_rule"));

            foreach (var agentId in new[] { "agent-one", "agent-two" })
            {
                var decision = await ServerActionGate.AuthorizeAsync(store, c.Source, NullLogger.Instance, DarlingMcpPlanForceTools.ForceToolName,
                    ServerActionNames.ForcePlan, c.Arg(agentId), new AgentActionTarget(7, "srv-7", "2", "AppDb"), c.Ct);
                Assert.NotNull(decision.Refusal);
            }

            Assert.Equal("1", await RefusedRowsAsync(c, DarlingMcpPlanForceTools.ForceToolName));
        });
    }

    /// <summary>L3 of the #6593 review: each kill refusal made before the gate journals the token's name, not the agent_id passed. A case fails
    /// when its call site drops agentTokenName: the row then names agent:someone-else.</summary>
    [Theory]
    [InlineData("bad_session")]
    [InlineData("bad_login_time")]
    [InlineData("azure_no_database")]
    [InlineData("pg_bad_backend_id")]
    [InlineData("pg_pid_mismatch")]
    public async Task EachKillRefusalBeforeTheGate_IsJournaledUnderTheTokensName(string kind)
    {
        await RunAsync(async c =>
        {
            const string LoginTime = "2026-10-01T12:00:00.0000000Z";
            if (kind == "azure_no_database")
                await ExecAsync(c.Connection, "UPDATE collect.servers SET sql_engine_edition = 5 WHERE server_id = 7", c.Ct);

            var (server, session, login, backend, code) = kind switch
            {
                "bad_session" => ("srv-7", (int?)0, LoginTime, (string?)null, PerformanceMonitor.Darling.Remediation.RemediationResult.Reasons.BadSession),
                "bad_login_time" => ("srv-7", 87, "not-a-time", null, DarlingMcpPlanForceTools.BadLoginTime),
                "azure_no_database" => ("srv-7", 87, LoginTime, null, DarlingMcpPlanForceTools.AzureDatabaseRequired),
                "pg_bad_backend_id" => ("srv-8", 42, null, "not-digits", DarlingMcpPlanForceTools.BadBackendId),
                _ => ("srv-8", 43, (string?)null, "17000000000000042", DarlingMcpPlanForceTools.BadBackendId),
            };

            var answer = await DarlingMcpPlanForceTools.RunKillAsync(
                c.Source, c.Source, null, server, session, login, backend, c.Arg(Other), true, c.Ct, null, Token);

            Assert.Equal(code, ReasonOf(answer));
            Assert.Equal("1|0", await RefusedActorsAsync(c, DarlingMcpPlanForceTools.KillToolName, code));
        });
    }

    /// <summary>#6635 round 3: each recompile_module refusal made before the gate journals the token's name, not the agent_id passed. A case
    /// fails when its call site drops agentTokenName: the row then names agent:someone-else.</summary>
    [Theory]
    [InlineData("bad_module_name")]
    [InlineData("bad_database")]
    [InlineData("postgres_target")]
    public async Task EachRecompileModuleRefusalBeforeTheGate_IsJournaledUnderTheTokensName(string kind)
    {
        await RunAsync(async c =>
        {
            var (server, database, module, code) = kind switch
            {
                "bad_module_name" => ("srv-7", "AppDb", "dbo", PerformanceMonitor.Darling.Remediation.RemediationResult.Reasons.BadModuleName),
                "bad_database" => ("srv-7", "", "dbo.p", PerformanceMonitor.Darling.Remediation.RemediationResult.Reasons.BadDatabase),
                _ => ("srv-8", "AppDb", "dbo.p", DarlingMcpPlanForceTools.PostgresTarget),
            };

            var answer = await DarlingMcpPlanForceTools.RunRecompileAsync(
                c.Source, c.Source, null, server, database, module, c.Arg(Other), true, c.Ct, Token);

            Assert.Equal(code, ReasonOf(answer));
            Assert.Equal("1|0", await RefusedActorsAsync(c, DarlingMcpPlanForceTools.RecompileToolName, code));
        });
    }

    /// <summary>L3 of the #6593 review: the kill_session wrapper passes the served policy's token name. A tools/call through the host with a
    /// named agent token, refused before the gate (session_id 0), journals the token's name. It fails when the wrapper drops
    /// served?.Policy.AgentTokenName.</summary>
    [Fact]
    public async Task AKillCallThroughTheHost_WithANamedAgentToken_JournalsTheTokensName()
    {
        await RunAsync(async c =>
        {
            const string agentBearer = "agent-token-value-identity-gate-cccc-3";
            var mcp = new PerformanceMonitor.Darling.Service.McpConfig
            {
                Tokens = new System.Collections.Generic.List<PerformanceMonitor.Darling.Service.McpTokenEntry>
                {
                    new() { Name = "ops-lead", Token = "operator-token-value-identity-gate-dd-4", Role = "operator" },
                    new() { Name = Token, Token = agentBearer, Role = "agent" },
                },
            };
            var legacy = DarlingMcpWritePolicy.ResolveForStart(mcp, "", out _);
            var tokens = DarlingMcpTokenSet.Resolve(mcp, "", legacy, null);
            using var server = await DarlingMcpWritePolicyTests.BuildServer(mcp, "", c.ConnectionString, tokens: tokens);

            var arguments = new JsonObject
            {
                ["server_name"] = "srv-7",
                ["session_id"] = 0,
                ["login_time"] = "2026-10-01T12:00:00.0000000Z",
                ["agent_action"] = c.Arg(Other),
            }.ToJsonString();
            var (_, body) = await DarlingMcpWritePolicyTests.ToolsCallAsync(server, DarlingMcpPlanForceTools.KillToolName, arguments, agentBearer);

            Assert.Contains(PerformanceMonitor.Darling.Remediation.RemediationResult.Reasons.BadSession, body, StringComparison.Ordinal);
            Assert.Equal("1|0", await RefusedActorsAsync(c, DarlingMcpPlanForceTools.KillToolName,
                PerformanceMonitor.Darling.Remediation.RemediationResult.Reasons.BadSession));
        });
    }

    private static Task<string> CallAsync(Ctx c, string tool, string server, string agentId) => tool switch
    {
        DarlingMcpPlanForceTools.ForceToolName or DarlingMcpPlanForceTools.UnforceToolName => DarlingMcpPlanForceTools.RunAsync(
            c.Source, c.Source, null, tool == DarlingMcpPlanForceTools.ForceToolName, server, "AppDb", 11, 2, c.Arg(agentId), true, c.Ct, Token),
        DarlingMcpPlanForceTools.EvictToolName => DarlingMcpPlanForceTools.RunEvictAsync(
            c.Source, c.Source, null, server, "0x06000700AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", c.Arg(agentId), true, c.Ct, Token),
        _ => DarlingMcpPlanForceTools.RunKillAsync(
            c.Source, c.Source, null, server, 87, "2026-10-01T12:00:00.0000000Z", null, c.Arg(agentId), true, c.Ct, null, Token),
    };
}
