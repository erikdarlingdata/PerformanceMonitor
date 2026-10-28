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
using System.Reflection;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5915: what the investigations arm serves, what an agent can never reach, and the protected names. Pure: behaviour
/// of the policy, the protected-name rule and the brief, not a census of source text (#5833).
/// </summary>
[Trait("Stage", "Guard")]
public sealed class AgentLevel2ToolPolicyTests
{
    private static readonly string[] ActionTools =
    {
        "create_mute_rule", "update_mute_rule", "delete_mute_rule", "set_mute_rule_enabled",
        "create_custom_alert_rule", "update_custom_alert_rule", "delete_custom_alert_rule", "set_alert_threshold_override", "merge_deadlock_signatures",
        "force_query_store_plan", "unforce_query_store_plan", "evict_cached_plan", "kill_session",
        "recompile_module", "create_plan_guide", "drop_plan_guide",
    };

    private static readonly DarlingMcpWritePolicy.Effective AgentArm = new(false, false, Investigations: true);

    private static List<McpServerTool> AgentServed()
    {
        var all = new McpServerPrimitiveCollection<McpServerTool>();
        foreach (var tool in McpToolsListBudgetTests.BuildServedTools().Tools)
            all.Add(tool);
        return DarlingMcpWritePolicy.FilterToolCollection(all, AgentArm).ToList();
    }

    [Fact]
    public void TheInvestigationsArm_ServesExactlyTheActionTools_OfTheWrites()
    {
        var served = AgentServed().Select(t => t.ProtocolTool.Name).ToList();
        var writes = served.Where(DarlingMcpWritePolicy.IsWrite).OrderBy(n => n, StringComparer.Ordinal).ToList();
        Assert.Equal(ActionTools.OrderBy(n => n, StringComparer.Ordinal).ToList(), writes);
        Assert.Contains("submit_investigation_report", served);
    }

    [Theory]
    [InlineData("update_alert_settings")]
    [InlineData("set_notification_route_enabled")]
    [InlineData("set_notification_route_fallback")]
    [InlineData("delete_notification_route")]
    [InlineData("mute_analysis_finding")]
    [InlineData("unmute_analysis_finding")]
    [InlineData("create_custom_view")]
    [InlineData("edit_server")]
    [InlineData("assign_server_tag")]
    public void NoAgentPath_WritesAnEnableFlag_AlertsEnabled_SelfAlertsOrAnExclusionList(string tool)
    {
        /* The settings that hold those writes are not offered under the investigations arm, so a call to one fails as unknown. */
        Assert.False(AgentArm.Offers(tool));
        Assert.DoesNotContain(AgentServed(), t => t.ProtocolTool.Name == tool);
    }

    [Fact]
    public void TheServedActionTools_HaveNoParameter_ThatWritesAnAlertEnableFlagASelfAlertOrAnExclusionList()
    {
        foreach (var tool in AgentServed().Where(t => ActionTools.Contains(t.ProtocolTool.Name)))
        {
            var properties = tool.ProtocolTool.InputSchema.GetProperty("properties").EnumerateObject().Select(p => p.Name).ToList();
            Assert.DoesNotContain(properties, p => p.Contains("alerts_enabled", StringComparison.Ordinal)
                || p.Contains("self_alerts", StringComparison.Ordinal) || p.Contains("exclu", StringComparison.OrdinalIgnoreCase));
            Assert.Contains("agent_action", properties);
            /* The seat and the logger are DI services: a client can never supply the policy. */
            Assert.DoesNotContain("agentSeat", properties);
            Assert.DoesNotContain("logger", properties);
        }
    }

    [Fact]
    public void TheInstructionsCensus_UnderTheInvestigationsArm_CountsTheActionToolsAsServed_AndStillSaysTheWritesAreOff()
    {
        var catalog = DarlingMcpWritePolicy.ToolNames(McpToolsListBudgetTests.BuildServedTools().Tools);
        var served = AgentServed().Select(t => t.ProtocolTool.Name).ToList();
        var writes = catalog.Count(n => DarlingMcpWritePolicy.IsWrite(n) && !DarlingMcpWritePolicy.IsServerAdmin(n));
        var admin = catalog.Count(DarlingMcpWritePolicy.IsServerAdmin);
        var text = DarlingMcpInstructions.Build(DarlingPeerDirectory.Snapshot.Empty, catalog, served);
        Assert.Contains($"the write tools are off, so the {writes + admin - ActionTools.Length} that change monitoring configuration are not served", text, StringComparison.Ordinal);
        Assert.Contains($"the {ActionTools.Length} mute, custom-rule, threshold override, signature merge, plan force, plan evict, session kill, recompile and plan guide tools are served only to an agent with an `agent_action` (Level 2; the last five listed need Level 3 and the server's own switch)", text, StringComparison.Ordinal);
    }

    [Fact]
    public void TheWritesArm_StillServesEveryWriteTool_AndTheActionToolsAreNotAgentOnly()
    {
        var all = new McpServerPrimitiveCollection<McpServerTool>();
        foreach (var tool in McpToolsListBudgetTests.BuildServedTools().Tools)
            all.Add(tool);
        var writes = DarlingMcpWritePolicy.ToolNames(DarlingMcpWritePolicy.FilterToolCollection(all, new DarlingMcpWritePolicy.Effective(true, true)));
        Assert.All(ActionTools, name => Assert.Contains(name, writes));
        Assert.Contains("update_alert_settings", writes);
    }

    [Theory]
    [InlineData("Collection Stopped")]
    [InlineData("Capture Down")]
    [InlineData("Notification Channel Failing")]
    [InlineData("Notification Channel Recovered")]
    [InlineData("Stale Mute Rules")]
    [InlineData("Custom Alert Rules Unhealthy")]
    [InlineData("Store Disk Pressure")]
    [InlineData("Web TLS Certificate Expiring")]
    public void ASelfMonitorOrSelfAlertMetric_IsProtected(string metric) =>
        Assert.True(DarlingMcpAgentActions.IsProtectedMetric(metric));

    [Fact]
    public void EverySelfAlertMetricConstant_IsProtected_ExceptTheTenAnAgentMayMute()
    {
        var unprotected = new[]
        {
            DarlingSelfAlertEvaluator.AgFailoverMetric, DarlingSelfAlertEvaluator.AgReplicaDisconnectedMetric,
            DarlingSelfAlertEvaluator.AgReplicaReconnectedMetric, DarlingSelfAlertEvaluator.AgSyncFellBehindMetric,
            DarlingSelfAlertEvaluator.AgDatabaseSuspendedMetric, DarlingSelfAlertEvaluator.AgentDownMetric,
            DarlingSelfAlertEvaluator.IdentityCapacityMetric,
            DarlingSelfAlertEvaluator.CollectorCostDigestMetric, DarlingSelfAlertEvaluator.FleetSweepRollupMetric,
            DarlingSelfAlertEvaluator.AnalysisSinglesDigestMetric,
        };
        var constants = typeof(DarlingSelfAlertEvaluator)
            .GetFields(BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string) && f.Name.EndsWith("Metric", StringComparison.Ordinal))
            .Select(f => (string)f.GetRawConstantValue()!)
            .ToList();
        Assert.True(constants.Count > 40, "the reflection found the self alert constants");
        foreach (var metric in constants.Where(m => !unprotected.Contains(m)))
            Assert.True(DarlingMcpAgentActions.IsProtectedMetric(metric), metric);
        foreach (var metric in unprotected)
            Assert.False(DarlingMcpAgentActions.IsProtectedMetric(metric), metric);
        Assert.False(DarlingMcpAgentActions.IsProtectedMetric("Blocking Detected"));
    }

    private static InvestigationBriefSources Sources(int level) => new(
        new InvestigationBriefRequest(Guid.NewGuid(), 7, "SQLPROD01", false, "blocking", "claimed",
            DateTime.UtcNow.AddHours(4), DateTime.UtcNow.AddMinutes(15), 1, "Blocking", null, level),
        Array.Empty<InvestigationBriefAlert>(), Array.Empty<InvestigationBriefFinding>(), Array.Empty<InvestigationEarlierReport>(),
        Array.Empty<InvestigationRecentChange>(), Array.Empty<InvestigationBriefNote>(), Array.Empty<InvestigationProfileText>());

    [Fact]
    public void ALevelTwoBrief_NamesTheTools_AndSaysChangeFirstReportLast_ALevelOneBriefIsUnchanged()
    {
        var two = JsonNode.Parse(InvestigationBriefComposer.Compose(Sources(2), DarlingInvestigationBriefTools.Tools, McpDataDetail.Full,
            DateTime.UtcNow))!.AsObject();
        Assert.Equal(2, (int)two["autonomy_level"]!);
        var allowed = two["allowed_actions"]!.AsArray().Select(n => (string)n!).ToList();
        Assert.Contains(allowed, a => a.StartsWith("change first, report last:", StringComparison.Ordinal) && a.Contains("create_mute_rule", StringComparison.Ordinal));
        Assert.Contains(two["read_rules"]!.AsArray().Select(n => (string)n!), r => r.Contains("writes refuse after submit", StringComparison.Ordinal));
        /* action_proposals stays refused at every level: the report contract is unchanged. */
        Assert.Equal("submit_investigation_report", (string)two["report_contract"]!["tool"]!);

        var one = JsonNode.Parse(InvestigationBriefComposer.Compose(Sources(1), DarlingInvestigationBriefTools.Tools, McpDataDetail.Full,
            DateTime.UtcNow))!.AsObject();
        Assert.Equal(1, (int)one["autonomy_level"]!);
        Assert.Equal(2, one["allowed_actions"]!.AsArray().Count);
        Assert.Contains(one["read_rules"]!.AsArray().Select(n => (string)n!), r => r.StartsWith("You only read and report.", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("known batch", "known batch [agent:agent-a request {R}]")]
    [InlineData(null, "[agent:agent-a request {R}]")]
    [InlineData("  ", "[agent:agent-a request {R}]")]
    /* The reason read back from the rule already ends with an agent suffix (this agent's, another's, or two stacked by the old
       build): one suffix, this action's, comes out. */
    [InlineData("known batch [agent:agent-a request {R}]", "known batch [agent:agent-a request {R}]")]
    [InlineData("known batch [agent:agent-b request 7f3a9c1e-0000-4000-8000-000000000001]", "known batch [agent:agent-a request {R}]")]
    [InlineData("known batch [agent:agent-a request {R}] [agent:agent-a request {R}]", "known batch [agent:agent-a request {R}]")]
    [InlineData("[agent:agent-a request {R}]", "[agent:agent-a request {R}]")]
    /* A bracket in the middle of the reason is the agent's own text, not a suffix. */
    [InlineData("see [agent:x request y] note", "see [agent:x request y] note [agent:agent-a request {R}]")]
    public void WithSuffix_PutsExactlyOneAttributionSuffix_OnTheReason(string? reason, string expected)
    {
        var request = Guid.NewGuid();
        Assert.Null(InvestigationToolCore.ParseAgentAction(AgentActionArgumentTests.Arg(request.ToString()), out var action));
        Assert.Equal(expected.Replace("{R}", request.ToString()), DarlingMcpAgentActions.WithSuffix(reason?.Replace("{R}", request.ToString()), action!));
    }

    [Fact]
    public void AReportWithActionProposals_IsStillRefused()
    {
        var problems = InvestigationToolCore.ValidateReport(Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), "real", "cause",
            "[{\"tool\":\"get_blocking\",\"arguments\":{},\"key_numbers\":\"1\"}]", "summary", null, "[{\"tool\":\"create_mute_rule\"}]",
            out _, out _, out _);
        Assert.Contains(problems, p => p.StartsWith("action_proposals:", StringComparison.Ordinal));
    }
}

public sealed class AgentMuteStartRefusalTests
{
    private static readonly DarlingAgentActionSeat Seat = new(new DarlingMcpWritePolicy.Effective(false, false, Investigations: true));

    [Fact]
    public async Task AnAgentMute_WithAStartsAt_IsRefusedBeforeAnythingIsRead()
    {
        /* A data source that was never opened: the refusal comes before any store access, so nothing connects. */
        await using var unused = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Database=none;Username=none;Timeout=1");

        var create = await DarlingMcpAgentActions.CreateMuteAsync(Seat, unused, null, null, null, "High CPU", null, null, null, null,
            "patching", "2026-10-30T00:00:00Z", 7, null, null, "2026-10-29T00:00:00Z", TestContext.Current.CancellationToken);
        var update = await DarlingMcpAgentActions.UpdateMuteAsync(Seat, unused, null, null, "rule-1",
            "{\"starts_at_utc\":\"2026-10-29T00:00:00Z\"}", TestContext.Current.CancellationToken);

        Assert.Equal("invalid", System.Text.Json.Nodes.JsonNode.Parse(create)!["status"]!.GetValue<string>());
        Assert.Contains("starts_at is not available", create, StringComparison.Ordinal);
        Assert.Contains("starts_at is not available", update, StringComparison.Ordinal);
    }
}

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT
   [Collection("live-postgres")]. */

/// <summary>
/// #5915: the seven Level 2 tools against a live store, through the real tool methods: every kind rule refuses, a pass is
/// attributed and journaled, and the writes arm is unchanged.
/// </summary>
public sealed class AgentLevel2ToolLiveTests
{
    private static readonly DateTime T0 = new(2026, 10, 16, 12, 0, 0, DateTimeKind.Utc);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly DarlingAgentActionSeat AgentSeat = new(new DarlingMcpWritePolicy.Effective(false, false, Investigations: true));

    private const string Scope7 = "\"scope\":{\"mode\":\"servers\",\"servers\":[\"srv-7\"]}";
    private const string Scope8 = "\"scope\":{\"mode\":\"servers\",\"servers\":[\"srv-8\"]}";

    private static string Definition(string scope) =>
        "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"hours\":1}," +
        "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1000}" + (scope.Length == 0 ? "" : "," + scope) + "}";

    private sealed record Ctx(NpgsqlDataSource Source, NpgsqlConnection Connection, string Arg, Guid Request, CancellationToken Ct);

    private static async Task RunAsync(Func<Ctx, Task> body, int routeLevel = 2)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the Level 2 tool live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);

        foreach (var id in new[] { 7, 8 })
        {
            await ExecAsync(connection, $@"INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ({id}, 'srv-{id}', 'srv-{id}', TRUE, 15, now(), now())", ct);
        }

        await ExecAsync(connection, $"INSERT INTO config.config_notification_routes (metric_match, agent_level) VALUES ('Blocking', {routeLevel})", ct);
        var now = DateTime.UtcNow;
        var store = new InvestigationRequestStore(source, 30);
        var alert = new InvestigationAlert(1, 7, "srv-7", "blocking", "Blocking", now, Array.Empty<string>(), null, 20, 100,
            LeaseMinutes: 15, ExpiryMinutes: 240) { AutonomyLevel = routeLevel };
        var opened = await store.OpenOrJoinAsync(alert, ct);
        var claim = await store.ClaimAsync(opened.RequestId!.Value, "agent-a", ct);
        var arg = AgentActionArgumentTests.Arg(opened.RequestId.Value.ToString(), claim.ClaimId!.Value.ToString());
        await body(new Ctx(source, connection, arg, opened.RequestId.Value, ct));
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

    private static JsonObject Parse(string answer) => JsonNode.Parse(answer)!.AsObject();

    private static Task<string> CreateMute(Ctx c, string? metric = "Blocking Detected", int? serverId = 7, string? arg = null,
        string? reason = "known batch", DarlingAgentActionSeat? seat = null) =>
        DarlingMcpAlertTools.CreateMuteRule(c.Source, null, metric, null, null, null, null, reason, null, serverId, null, null,
            arg ?? c.Arg, seat ?? AgentSeat);

    private static async Task<string> SeedMuteAsync(Ctx c, int? serverId, string? metric, bool enabled = true)
    {
        var id = Guid.NewGuid().ToString();
        await ExecAsync(c.Connection, $@"INSERT INTO config_mute_rules (id, enabled, created_at_utc, server_id, server_name, metric_name)
VALUES ('{id}', {(enabled ? "TRUE" : "FALSE")}, now() AT TIME ZONE 'utc', {(serverId is null ? "NULL" : serverId.ToString())}, {(serverId is null ? "NULL" : $"'srv-{serverId}'")}, {(metric is null ? "NULL" : $"'{metric}'")})", c.Ct);
        return id;
    }

    private static async Task<string?> RefusedCountAsync(Ctx c, string code) =>
        await ScalarAsync(c.Connection, $"SELECT count(*) FROM config.investigation_journal WHERE request_id = '{c.Request}' AND event = 'refused' AND reason = '{code}'", c.Ct);

    private static async Task<string?> MuteCountAsync(Ctx c) => await ScalarAsync(c.Connection, "SELECT count(*) FROM config_mute_rules", c.Ct);

    [Fact]
    public async Task ACreate_OnTheRequestsServer_IsAttributed_ScopedToThatServerOnly_AndJournaled()
    {
        await RunAsync(async c =>
        {
            var answer = Parse(await CreateMute(c));
            Assert.Equal("created", (string?)answer["status"]);
            var id = (string)answer["mute_rule"]!["id"]!;
            Assert.Equal("7", await ScalarAsync(c.Connection, $"SELECT server_id FROM config_mute_rules WHERE id = '{id}'", c.Ct));
            Assert.Equal($"known batch [agent:agent-a request {c.Request}]",
                await ScalarAsync(c.Connection, $"SELECT reason FROM config_mute_rules WHERE id = '{id}'", c.Ct));
            Assert.Equal("action_called|action_done", await ScalarAsync(c.Connection,
                $"SELECT string_agg(event, '|' ORDER BY journal_id) FROM config.investigation_journal WHERE request_id = '{c.Request}' AND event LIKE 'action_%'", c.Ct));
            Assert.Equal("create_mute_rule", await ScalarAsync(c.Connection,
                $"SELECT detail->>'tool' FROM config.investigation_journal WHERE request_id = '{c.Request}' AND event = 'action_called'", c.Ct));
        });
    }

    [Theory]
    [InlineData(null, 7)]
    [InlineData("Blocking Detected", null)]
    [InlineData("  ", 7)]
    public async Task AMuteWithNoMetric_OrNoServer_IsRefused_MuteNeedsMetric(string? metric, int? serverId)
    {
        await RunAsync(async c =>
        {
            var refused = Parse(await CreateMute(c, metric, serverId));
            Assert.Equal("refused", (string?)refused["status"]);
            Assert.Equal("mute_needs_metric", (string?)refused["reason"]);
            Assert.Equal("0", await MuteCountAsync(c));
            Assert.Equal("1", await RefusedCountAsync(c, "mute_needs_metric"));
        });
    }

    [Fact]
    public async Task AMuteOnAnotherServer_IsRefused_OtherServer()
    {
        await RunAsync(async c =>
        {
            var refused = Parse(await CreateMute(c, serverId: 8));
            Assert.Equal(AgentActionCodes.OtherServer, (string?)refused["reason"]);
            Assert.Equal("0", await MuteCountAsync(c));
        });
    }

    [Theory]
    [InlineData("Collection Stopped")]
    [InlineData("Notification Channel Failing")]
    [InlineData("Notification Channel Recovered")]
    [InlineData("Stale Mute Rules")]
    [InlineData("Store Disk Pressure")]
    public async Task AMuteOnAProtectedName_IsRefused_SelfMonitorMetric(string metric)
    {
        await RunAsync(async c =>
        {
            var refused = Parse(await CreateMute(c, metric));
            Assert.Equal("self_monitor_metric", (string?)refused["reason"]);
            Assert.Equal("0", await MuteCountAsync(c));
        });
    }

    [Theory]
    [InlineData("Agent Not Running")]
    [InlineData("Identity Capacity")]
    [InlineData("Collector Cost Digest")]
    [InlineData("AG Failover")]
    public async Task AMuteOnAnUncoveredSelfAlert_IsAllowed(string metric)
    {
        await RunAsync(async c => Assert.Equal("created", (string?)Parse(await CreateMute(c, metric))["status"]));
    }

    [Fact]
    public async Task AnUpdate_OnTheRequestsServer_KeepsTheSuffix_AndARefusedUpdateChangesNothing()
    {
        await RunAsync(async c =>
        {
            var id = await SeedMuteAsync(c, 7, "Blocking Detected");
            async Task<string> Update(string changes, string? who = null) =>
                await DarlingMcpAlertTools.UpdateMuteRule(c.Source, id, changes, who ?? c.Arg, AgentSeat);

            Assert.Equal("updated", (string?)Parse(await Update("{\"reason\":\"root cause found\"}"))["status"]);
            Assert.Equal($"root cause found [agent:agent-a request {c.Request}]",
                await ScalarAsync(c.Connection, $"SELECT reason FROM config_mute_rules WHERE id = '{id}'", c.Ct));

            Assert.Equal("self_monitor_metric", (string?)Parse(await Update("{\"metric_name\":\"Collection Stopped\"}"))["reason"]);
            Assert.Equal("mute_needs_metric", (string?)Parse(await Update("{\"metric_name\":null}"))["reason"]);
            Assert.Equal("mute_needs_metric", (string?)Parse(await Update("{\"server_id\":null}"))["reason"]);
            Assert.Equal(AgentActionCodes.OtherServer, (string?)Parse(await Update("{\"server_id\":8}"))["reason"]);
            Assert.Equal("Blocking Detected", await ScalarAsync(c.Connection, $"SELECT metric_name FROM config_mute_rules WHERE id = '{id}'", c.Ct));
            Assert.Equal("7", await ScalarAsync(c.Connection, $"SELECT server_id FROM config_mute_rules WHERE id = '{id}'", c.Ct));
        });
    }

    [Fact]
    public async Task AnUpdateDeleteOrEnable_OfAMuteOnAnotherServer_OrOnNoServer_IsRefused_OtherServer()
    {
        await RunAsync(async c =>
        {
            foreach (var serverId in new int?[] { 8, null })
            {
                var id = await SeedMuteAsync(c, serverId, "Blocking Detected", enabled: false);
                Assert.Equal(AgentActionCodes.OtherServer, (string?)Parse(await DarlingMcpAlertTools.UpdateMuteRule(c.Source, id, "{\"reason\":\"x\"}", c.Arg, AgentSeat))["reason"]);
                Assert.Equal(AgentActionCodes.OtherServer, (string?)Parse(await DarlingMcpAlertTools.DeleteMuteRule(c.Source, id, c.Arg, AgentSeat))["reason"]);
                Assert.Equal(AgentActionCodes.OtherServer, (string?)Parse(await DarlingMcpAlertTools.SetMuteRuleEnabled(c.Source, id, true, c.Arg, AgentSeat))["reason"]);
                Assert.Equal("1", await ScalarAsync(c.Connection, $"SELECT count(*) FROM config_mute_rules WHERE id = '{id}' AND enabled = FALSE", c.Ct));
            }
        });
    }

    [Fact]
    public async Task ADelete_OfAMuteOnTheRequestsServer_Works_AndIsJournaledWithItsBeforeImage()
    {
        await RunAsync(async c =>
        {
            var id = await SeedMuteAsync(c, 7, "Blocking Detected");
            Assert.Equal("deleted", (string?)Parse(await DarlingMcpAlertTools.DeleteMuteRule(c.Source, id, c.Arg, AgentSeat))["status"]);
            Assert.Equal("0", await MuteCountAsync(c));
            Assert.Equal("Blocking Detected", await ScalarAsync(c.Connection,
                $"SELECT detail->'before'->>'metric_name' FROM config.investigation_journal WHERE request_id = '{c.Request}' AND event = 'action_called'", c.Ct));
        });
    }

    [Fact]
    public async Task AnEnable_FollowsTheMuteRules_AndADisableIsAlwaysAllowed()
    {
        await RunAsync(async c =>
        {
            var ok = await SeedMuteAsync(c, 7, "Blocking Detected", enabled: false);
            Assert.Equal("updated", (string?)Parse(await DarlingMcpAlertTools.SetMuteRuleEnabled(c.Source, ok, true, c.Arg, AgentSeat))["status"]);
            Assert.Equal("updated", (string?)Parse(await DarlingMcpAlertTools.SetMuteRuleEnabled(c.Source, ok, false, c.Arg, AgentSeat))["status"]);

            var noMetric = await SeedMuteAsync(c, 7, null, enabled: false);
            Assert.Equal("mute_needs_metric", (string?)Parse(await DarlingMcpAlertTools.SetMuteRuleEnabled(c.Source, noMetric, true, c.Arg, AgentSeat))["reason"]);

            var protectedMute = await SeedMuteAsync(c, 7, "Collection Stopped", enabled: false);
            Assert.Equal("self_monitor_metric", (string?)Parse(await DarlingMcpAlertTools.SetMuteRuleEnabled(c.Source, protectedMute, true, c.Arg, AgentSeat))["reason"]);
            Assert.Equal("1", await ScalarAsync(c.Connection, $"SELECT count(*) FROM config_mute_rules WHERE id = '{protectedMute}' AND enabled = FALSE", c.Ct));
        });
    }

    [Fact]
    public async Task TheGateCodes_RefuseThroughTheTool_AndChangeNothing()
    {
        await RunAsync(async c =>
        {
            Assert.Equal(AgentActionCodes.Required, (string?)Parse(await CreateMute(c, arg: ""))["reason"]);

            var wrongAgent = AgentActionArgumentTests.Arg(c.Request.ToString(), Guid.NewGuid().ToString());
            Assert.Equal(AgentActionCodes.ClaimNotLive, (string?)Parse(await CreateMute(c, arg: wrongAgent))["reason"]);

            await ExecAsync(c.Connection, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT DO NOTHING", c.Ct);
            await ExecAsync(c.Connection, "UPDATE config.config_service SET agent_actions_enabled = FALSE", c.Ct);
            var off = await CreateMute(c);
            Assert.True(off.Contains("actions_off", StringComparison.Ordinal), off);
            await ExecAsync(c.Connection, "UPDATE config.config_service SET agent_actions_enabled = TRUE", c.Ct);

            await ExecAsync(c.Connection, "DELETE FROM config.config_notification_routes", c.Ct);
            Assert.Equal(AgentActionCodes.NotLevel2, (string?)Parse(await CreateMute(c))["reason"]);
            Assert.Equal("0", await MuteCountAsync(c));
        });
    }

    [Fact]
    public async Task ALevelOneRequest_IsRefused_NotLevel2_ByTheTool()
    {
        await RunAsync(async c =>
        {
            Assert.Equal(AgentActionCodes.NotLevel2, (string?)Parse(await CreateMute(c))["reason"]);
            Assert.Equal("0", await MuteCountAsync(c));
        }, routeLevel: 1);
    }

    [Fact]
    public async Task TheWritesArm_IgnoresAgentAction_AndKeepsItsWholeFleetMute()
    {
        await RunAsync(async c =>
        {
            var writes = new DarlingAgentActionSeat(new DarlingMcpWritePolicy.Effective(true, false));
            var answer = Parse(await DarlingMcpAlertTools.CreateMuteRule(c.Source, null, null, null, null, null, null, "fleet quiet", null,
                null, null, null, null, writes));
            Assert.Equal("created", (string?)answer["status"]);
            Assert.Equal("0", await ScalarAsync(c.Connection, "SELECT count(*) FROM config.investigation_journal WHERE event LIKE 'action_%'", c.Ct));
        });
    }

    /* ------------------------------------------------------------------ custom alert rules */

    private static Task<string> CreateRule(Ctx c, string scope, string name = "agent rule", bool enabled = true) =>
        DarlingMcpCustomAlertTools.CreateCustomAlertRule(c.Source, name, Definition(scope), null, enabled, c.Arg, AgentSeat);

    [Fact]
    public async Task ACustomRule_ScopedToTheRequestsServer_IsCreated_StampedAgent_AndJournaled()
    {
        await RunAsync(async c =>
        {
            var created = Parse(await CreateRule(c, Scope7));
            Assert.Equal("agent rule", (string?)created["name"]);
            Assert.Equal("agent:agent-a", await ScalarAsync(c.Connection, "SELECT updated_by FROM config.custom_alert_rules", c.Ct));
            Assert.Equal("action_called|action_done", await ScalarAsync(c.Connection,
                $"SELECT string_agg(event, '|' ORDER BY journal_id) FROM config.investigation_journal WHERE request_id = '{c.Request}' AND event LIKE 'action_%'", c.Ct));
        });
    }

    [Theory]
    [InlineData("")]
    [InlineData("\"scope\":{\"mode\":\"all\"}")]
    [InlineData("\"scope\":{\"mode\":\"servers\",\"servers\":[\"srv-8\"]}")]
    [InlineData("\"scope\":{\"mode\":\"servers\",\"servers\":[\"srv-7\",\"srv-8\"]}")]
    [InlineData("\"scope\":{\"mode\":\"tag\",\"tagId\":3}")]
    public async Task ACustomRuleOutsideTheRequestsServer_IsRefused_RuleScope(string scope)
    {
        await RunAsync(async c =>
        {
            var refused = Parse(await CreateRule(c, scope));
            Assert.Equal("rule_scope", (string?)refused["reason"]);
            Assert.Equal("0", await ScalarAsync(c.Connection, "SELECT count(*) FROM config.custom_alert_rules", c.Ct));
        });
    }

    private static async Task<long> SeedRuleAsync(Ctx c, string scope, bool enabled = true)
    {
        var created = await new CustomAlertRuleStore(c.Source, StorageCommandDeadlines.McpReadSeconds).CreateAsync("seed " + Guid.NewGuid().ToString("N"), null, Definition(scope), enabled, "person:erik");
        return ((CustomAlertRuleResult.Ok)created).Rule!.Id;
    }

    [Fact]
    public async Task AnUpdate_IsRefused_RuleScope_WhenTheBeforeOrTheAfterScopeIsNotTheRequestsServer()
    {
        await RunAsync(async c =>
        {
            var other = await SeedRuleAsync(c, Scope8);
            Assert.Equal("rule_scope", (string?)Parse(await DarlingMcpCustomAlertTools.UpdateCustomAlertRule(c.Source, other, 1, "renamed", null, null, null, c.Arg, AgentSeat))["reason"]);
            Assert.Equal("rule_scope", (string?)Parse(await DarlingMcpCustomAlertTools.UpdateCustomAlertRule(c.Source, other, 1, null, Definition(Scope7), null, null, c.Arg, AgentSeat))["reason"]);

            var own = await SeedRuleAsync(c, Scope7);
            Assert.Equal("rule_scope", (string?)Parse(await DarlingMcpCustomAlertTools.UpdateCustomAlertRule(c.Source, own, 1, null, Definition(""), null, null, c.Arg, AgentSeat))["reason"]);
            Assert.Equal("rule_scope", (string?)Parse(await DarlingMcpCustomAlertTools.UpdateCustomAlertRule(c.Source, own, 1, null, Definition(Scope8), null, null, c.Arg, AgentSeat))["reason"]);

            var ok = Parse(await DarlingMcpCustomAlertTools.UpdateCustomAlertRule(c.Source, own, 1, "renamed", null, null, null, c.Arg, AgentSeat));
            Assert.Equal("renamed", (string?)ok["name"]);
            Assert.Equal("agent:agent-a", await ScalarAsync(c.Connection, $"SELECT updated_by FROM config.custom_alert_rules WHERE id = {own}", c.Ct));
            Assert.Equal("seed", (await ScalarAsync(c.Connection, $"SELECT name FROM config.custom_alert_rules WHERE id = {other}", c.Ct))!.Split(' ')[0]);
        });
    }

    [Fact]
    public async Task ADisable_AndADelete_OfARuleOnTheRequestsServer_AreAllowed_AndADeleteOutsideIsNot()
    {
        await RunAsync(async c =>
        {
            var own = await SeedRuleAsync(c, Scope7);
            var disabled = Parse(await DarlingMcpCustomAlertTools.UpdateCustomAlertRule(c.Source, own, 1, null, null, null, false, c.Arg, AgentSeat));
            Assert.Equal(false, (bool?)disabled["enabled"]);
            Assert.Equal("deleted", (string?)Parse(await DarlingMcpCustomAlertTools.DeleteCustomAlertRule(c.Source, own, c.Arg, AgentSeat))["status"]);

            var other = await SeedRuleAsync(c, Scope8);
            Assert.Equal("rule_scope", (string?)Parse(await DarlingMcpCustomAlertTools.DeleteCustomAlertRule(c.Source, other, c.Arg, AgentSeat))["reason"]);
            Assert.Equal("1", await ScalarAsync(c.Connection, "SELECT count(*) FROM config.custom_alert_rules", c.Ct));
        });
    }

    private static async Task<string?> JournalAsync(Ctx c, string select) =>
        await ScalarAsync(c.Connection, $"SELECT {select} FROM config.investigation_journal WHERE request_id = '{c.Request}' AND event LIKE 'action_%'", c.Ct);

    [Fact]
    public async Task ASecondUpdate_NeverStacksTheAttributionSuffix_WhetherTheReasonIsOmittedOrSentBackWhole()
    {
        await RunAsync(async c =>
        {
            var id = await SeedMuteAsync(c, 7, "Blocking Detected");
            async Task<string?> Stored() => await ScalarAsync(c.Connection, $"SELECT reason FROM config_mute_rules WHERE id = '{id}'", c.Ct);
            async Task<string> Update(string changes) => await DarlingMcpAlertTools.UpdateMuteRule(c.Source, id, changes, c.Arg, AgentSeat);
            var suffix = $"[agent:agent-a request {c.Request}]";

            Assert.Equal("updated", (string?)Parse(await Update("{\"reason\":\"known batch\"}"))["status"]);
            Assert.Equal($"known batch {suffix}", await Stored());

            /* No reason in the body: the stored reason is the fallback, and it already ends with the suffix. */
            Assert.Equal("updated", (string?)Parse(await Update("{\"expires_at_utc\":\"2099-11-01T00:00:00Z\"}"))["status"]);
            Assert.Equal($"known batch {suffix}", await Stored());

            /* The read-modify-write an agent does: get_mute_rules shows the suffix, the agent edits a word and sends it all back. */
            Assert.Equal("updated", (string?)Parse(await Update($"{{\"reason\":\"known batch, fixed {suffix}\"}}"))["status"]);
            Assert.Equal($"known batch, fixed {suffix}", await Stored());
        });
    }

    [Fact]
    public async Task AChangeThatSucceeds_IsJournaledActionDone_WithItsAfterImage_AndNeverActionFailed()
    {
        await RunAsync(async c =>
        {
            var created = Parse(await CreateMute(c));
            var muteId = (string)created["mute_rule"]!["id"]!;
            Assert.Equal("updated", (string?)Parse(await DarlingMcpAlertTools.UpdateMuteRule(c.Source, muteId, "{\"reason\":\"r2\"}", c.Arg, AgentSeat))["status"]);
            Assert.Equal("agent rule", (string?)Parse(await CreateRule(c, Scope7))["name"]);

            /* Every called row has its done row, none failed, and each done row carries the after image the tool answered. */
            Assert.Equal("action_called|action_done|action_called|action_done|action_called|action_done",
                await JournalAsync(c, "string_agg(event, '|' ORDER BY journal_id)"));
            Assert.Equal("0", await ScalarAsync(c.Connection,
                $"SELECT count(*) FROM config.investigation_journal WHERE request_id = '{c.Request}' AND event = 'action_failed'", c.Ct));
            Assert.Equal(muteId, await ScalarAsync(c.Connection,
                $"SELECT detail->'after'->'mute_rule'->>'id' FROM config.investigation_journal WHERE request_id = '{c.Request}' AND event = 'action_done' ORDER BY journal_id LIMIT 1", c.Ct));
            Assert.Equal("agent rule", await ScalarAsync(c.Connection,
                $"SELECT detail->'after'->>'name' FROM config.investigation_journal WHERE request_id = '{c.Request}' AND event = 'action_done' ORDER BY journal_id DESC LIMIT 1", c.Ct));
            Assert.Equal("True", await ScalarAsync(c.Connection,
                $"SELECT bool_and(detail ? 'called_id') FROM config.investigation_journal WHERE request_id = '{c.Request}' AND event = 'action_done'", c.Ct));
        });
    }

    [Fact]
    public async Task AWriteThatFails_RollsBackWithTheGatesTransaction_AndJournalsActionFailed_WithTheRealRefusal()
    {
        await RunAsync(async c =>
        {
            Assert.Equal("agent rule", (string?)Parse(await CreateRule(c, Scope7))["name"]);
            /* A duplicate name is the store's conflict, answered inside the write: nothing new is stored, and the failed row
               says why (the answer text), not a constant. */
            var again = Parse(await CreateRule(c, Scope7));
            Assert.Equal("conflict", (string?)again["status"]);
            Assert.Equal("1", await ScalarAsync(c.Connection, "SELECT count(*) FROM config.custom_alert_rules", c.Ct));
            Assert.Equal("1", await ScalarAsync(c.Connection,
                $"SELECT count(*) FROM config.investigation_journal WHERE request_id = '{c.Request}' AND event = 'action_failed' AND detail->>'error' LIKE '%already exists%'", c.Ct));
            Assert.Equal("action_called|action_done|action_called|action_failed",
                await JournalAsync(c, "string_agg(event, '|' ORDER BY journal_id)"));
        });
    }

    [Fact]
    public async Task ACustomRuleDefinitionThatDoesNotParse_IsRefused_AgentActionInvalid_NamingTheField_NotRuleScope()
    {
        await RunAsync(async c =>
        {
            var refused = Parse(await DarlingMcpCustomAlertTools.CreateCustomAlertRule(c.Source, "broken", "{\"metric\":", null, true, c.Arg, AgentSeat));
            Assert.Equal(AgentActionCodes.Invalid, (string?)refused["reason"]);
            Assert.Contains("definition", (string?)refused["message"], StringComparison.Ordinal);
            Assert.DoesNotContain("exactly the request's server", (string?)refused["message"], StringComparison.Ordinal);

            var own = await SeedRuleAsync(c, Scope7);
            var update = Parse(await DarlingMcpCustomAlertTools.UpdateCustomAlertRule(c.Source, own, 1, null, "not json", null, null, c.Arg, AgentSeat));
            Assert.Equal(AgentActionCodes.Invalid, (string?)update["reason"]);
            Assert.Equal("0", await ScalarAsync(c.Connection, "SELECT count(*) FROM config.custom_alert_rules WHERE name = 'broken'", c.Ct));
        });
    }

    [Fact]
    public async Task TheMuteRecheck_InTheGatesTransaction_RefusesARuleRePointedSinceItWasRead()
    {
        await RunAsync(async c =>
        {
            var id = await SeedMuteAsync(c, 7, "Blocking Detected");
            var before = new MuteRule { Id = id, ServerId = 7, MetricName = "Blocking Detected" };
            await using var connection = await c.Source.OpenConnectionAsync(c.Ct);
            await using var transaction = await connection.BeginTransactionAsync(c.Ct);

            await DarlingMcpAgentActions.StillTheSameMuteAsync(connection, transaction, id, before, c.Ct);

            await ExecAsync(connection, $"UPDATE config_mute_rules SET metric_name = 'Collection Stopped' WHERE id = '{id}'", c.Ct);
            var changed = await Assert.ThrowsAsync<AgentToolOutcomeException>(() =>
                DarlingMcpAgentActions.StillTheSameMuteAsync(connection, transaction, id, before, c.Ct));
            Assert.Equal("conflict", (string?)Parse(changed.Outcome)["status"]);

            var gone = await Assert.ThrowsAsync<AgentToolOutcomeException>(() =>
                DarlingMcpAgentActions.StillTheSameMuteAsync(connection, transaction, "no-such-id", before, c.Ct));
            Assert.Equal("not_found", (string?)Parse(gone.Outcome)["status"]);
        });
    }

    [Fact]
    public async Task TheCustomRuleRecheck_InTheGatesTransaction_RefusesARuleRescopedSinceItWasRead()
    {
        await RunAsync(async c =>
        {
            var id = await SeedRuleAsync(c, Scope7);
            await using var connection = await c.Source.OpenConnectionAsync(c.Ct);
            await using var transaction = await connection.BeginTransactionAsync(c.Ct);

            await DarlingMcpAgentActions.StillInScopeAsync(connection, transaction, id, 7, c.Ct);

            await ExecAsync(connection, $"UPDATE config.custom_alert_rules SET definition = '{Definition(Scope8)}'::jsonb WHERE id = {id}", c.Ct);
            var moved = await Assert.ThrowsAsync<AgentToolOutcomeException>(() =>
                DarlingMcpAgentActions.StillInScopeAsync(connection, transaction, id, 7, c.Ct));
            Assert.Equal("rule_scope", (string?)Parse(moved.Outcome)["reason"]);
        });
    }
}
