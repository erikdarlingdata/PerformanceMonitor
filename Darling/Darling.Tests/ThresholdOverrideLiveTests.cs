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
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #5916 against a live store: <c>set_alert_threshold_override</c> under each arm (the writes arm ignores
/// <c>agent_action</c>; the investigations arm requires it, keeps to the request's server and records the agent and the
/// request), every refused key class leaves the table empty, the loader reads what the tool wrote, a role without the grant
/// gets <c>precondition</c> naming <c>provision-roles.sql</c>, and <c>get_alert_settings</c> with <c>server_name</c> shows the
/// effective value and its source.
/// </summary>
public sealed class ThresholdOverrideLiveTests
{
    private static readonly DateTime T0 = new(2026, 10, 16, 12, 0, 0, DateTimeKind.Utc);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly DarlingMcpWritePolicy.Effective WritesArm = new(true, false);
    private static readonly DarlingMcpWritePolicy.Effective AgentArm = new(false, false, Investigations: true);

    private static async Task RunAsync(Func<NpgsqlDataSource, NpgsqlConnection, InvestigationRequestStore, CancellationToken, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the threshold override live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await ExecAsync(connection, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES (7, 'srv-7', 'h7'), (8, 'srv-8', 'h8')", ct);
        await ExecAsync(connection, "INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) "
            + "VALUES (7, 'srv-7', 'srv-7', TRUE, 15, now(), now()), (8, 'srv-8', 'srv-8', TRUE, 15, now(), now())", ct);

        var builder = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath };
        await using var dataSource = NpgsqlDataSource.Create(builder.ConnectionString);
        var store = new InvestigationRequestStore(dataSource, 30, () => T0);
        await body(dataSource, connection, store, ct);
    }

    private static Task<string> Set(NpgsqlDataSource ds, InvestigationRequestStore store, DarlingMcpWritePolicy.Effective policy,
        string server, string key, int? value, string? database = null, bool clear = false, string? setBy = null, string? agentAction = null) =>
        DarlingMcpThresholdOverrideTools.SetAsync(ds, policy, store, NullLogger.Instance, server, key, value, database, clear, setBy, agentAction,
            TestContext.Current.CancellationToken);

    private static string Status(string json) => JsonNode.Parse(json)!["status"]!.GetValue<string>();

    private static string Reason(string json) => JsonNode.Parse(json)!["reason"]!.GetValue<string>();

    [Fact]
    public async Task WritesArm_SetsUpdatesAndClears_IgnoresAgentAction_AndTheLoaderReadsIt()
    {
        await RunAsync(async (ds, connection, store, ct) =>
        {
            /* agent_action is ignored under the writes arm: a garbage value does not stop the write. */
            var set = await Set(ds, store, WritesArm, "srv-7", "cpu.threshold_percent", 95, setBy: "erik", agentAction: "not json");
            Assert.Equal("ok", Status(set));
            Assert.Equal("95", await ScalarAsync(connection, "SELECT value FROM config.config_threshold_overrides WHERE server_id = 7 AND threshold_key = 'cpu.threshold_percent'", ct));
            Assert.Equal("erik", await ScalarAsync(connection, "SELECT set_by FROM config.config_threshold_overrides WHERE server_id = 7", ct));
            Assert.Equal("", await ScalarAsync(connection, "SELECT request_id FROM config.config_threshold_overrides WHERE server_id = 7", ct));

            Assert.Equal("ok", Status(await Set(ds, store, WritesArm, "srv-7", "cpu.threshold_percent", 90)));
            Assert.Equal("1", await ScalarAsync(connection, "SELECT count(*) FROM config.config_threshold_overrides", ct));
            Assert.Equal("90", await ScalarAsync(connection, "SELECT value FROM config.config_threshold_overrides", ct));
            Assert.Equal("mcp", await ScalarAsync(connection, "SELECT set_by FROM config.config_threshold_overrides", ct));

            /* A database override is its own row, and the two PostgreSQL count thresholds are keys too. */
            Assert.Equal("ok", Status(await Set(ds, store, WritesArm, "srv-7", "pvs.threshold_percent", 70, database: "shop")));
            Assert.Equal("ok", Status(await Set(ds, store, WritesArm, "srv-8", "blocking.pg_count_threshold", 4)));
            Assert.Equal("ok", Status(await Set(ds, store, WritesArm, "srv-8", "deadlocks.pg_count_threshold", 6)));

            var loader = new DarlingThresholdOverrides();
            await loader.LoadAsync(ds, ct);
            Assert.Equal(4, loader.Count);
            Assert.Equal(90, loader.Resolve(AlertThresholdKey.CpuThresholdPercent, "7", null, 80));
            Assert.Equal(80, loader.Resolve(AlertThresholdKey.CpuThresholdPercent, "8", null, 80));
            Assert.Equal(70, loader.Resolve(AlertThresholdKey.PvsThresholdPercent, "7", "shop", 40));
            Assert.Equal(40, loader.Resolve(AlertThresholdKey.PvsThresholdPercent, "7", "ledger", 40));
            Assert.Equal(4, loader.ResolveWire(DarlingThresholdKeys.PgBlockingCount, "8", null, 20));
            Assert.Equal(20, loader.ResolveWire(DarlingThresholdKeys.PgBlockingCount, "7", null, 20));
            Assert.Equal(6, loader.ResolveWire(DarlingThresholdKeys.PgDeadlockCount, "8", null, 20));

            var cleared = await Set(ds, store, WritesArm, "srv-7", "cpu.threshold_percent", null, clear: true);
            Assert.Equal("cleared", Status(cleared));
            await loader.LoadAsync(ds, ct);
            Assert.Equal(80, loader.Resolve(AlertThresholdKey.CpuThresholdPercent, "7", null, 80));
        });
    }

    [Theory]
    [InlineData("no.such.key", 5, null, "not_in_registry")]
    [InlineData("self_alerts.collection_stale_minutes", 30, null, "not_tunable")]
    [InlineData("cpu.enabled", 1, null, "not_tunable")]
    [InlineData("cooldown_minutes", 10, null, "not_tunable")]
    [InlineData("route.agent_level", 2, null, "not_tunable")]
    [InlineData("cpu.threshold_percent", 101, null, "out_of_bounds")]
    [InlineData("cpu.threshold_percent", 90, "shop", "database_scope")]
    public async Task ARefusedKeyOrValue_LeavesTheTableEmpty(string key, int value, string? database, string reason)
    {
        await RunAsync(async (ds, connection, store, ct) =>
        {
            var answer = await Set(ds, store, WritesArm, "srv-7", key, value, database);
            Assert.Equal("refused", Status(answer));
            Assert.Equal(reason, Reason(answer));
            Assert.Equal("0", await ScalarAsync(connection, "SELECT count(*) FROM config.config_threshold_overrides", ct));
        });
    }

    [Fact]
    public async Task InvestigationsArm_RequiresAgentAction_KeepsToTheRequestsServer_AndRecordsTheAgentAndTheRequest()
    {
        await RunAsync(async (ds, connection, store, ct) =>
        {
            await ExecAsync(connection, "INSERT INTO config.config_notification_routes (metric_match, agent_level) VALUES ('Blocking', 2)", ct);
            var opened = await store.OpenOrJoinAsync(
                new InvestigationAlert(1, 7, "srv-7", "blocking", "Blocking", T0, Array.Empty<string>(), null, 20, 100,
                    LeaseMinutes: 15, ExpiryMinutes: 240) { AutonomyLevel = 2 }, ct);
            var request = opened.RequestId!.Value;
            var claim = (await store.ClaimAsync(request, "agent-a", ct)).ClaimId!.Value;
            var arg = AgentActionArgumentTests.Arg(request.ToString(), claim.ToString());

            /* Required: no agent_action, no change. */
            Assert.Equal(AgentActionCodes.Required, Reason(await Set(ds, store, AgentArm, "srv-7", "cpu.threshold_percent", 90)));
            Assert.Equal("0", await ScalarAsync(connection, "SELECT count(*) FROM config.config_threshold_overrides", ct));

            /* The request's server: the row names the agent (not the set_by argument) and the request. */
            var ok = await Set(ds, store, AgentArm, "srv-7", "cpu.threshold_percent", 90, setBy: "someone-else", agentAction: arg);
            Assert.Equal("ok", Status(ok));
            Assert.Equal("agent:agent-a", await ScalarAsync(connection, "SELECT set_by FROM config.config_threshold_overrides", ct));
            Assert.Equal(request.ToString(), await ScalarAsync(connection, "SELECT request_id FROM config.config_threshold_overrides", ct));
            Assert.Equal("1", await ScalarAsync(connection, $"SELECT count(*) FROM config.investigation_journal WHERE request_id = '{request}' AND event = 'action_done'", ct));

            /* Another server is refused, and so is a value that would turn the alert off. */
            Assert.Equal(AgentActionCodes.OtherServer, Reason(await Set(ds, store, AgentArm, "srv-8", "cpu.threshold_percent", 90, agentAction: arg)));
            Assert.Equal("threshold_disables_alert", Reason(await Set(ds, store, AgentArm, "srv-7", "cpu.threshold_percent", 100, agentAction: arg)));
            Assert.Equal("1", await ScalarAsync(connection, "SELECT count(*) FROM config.config_threshold_overrides", ct));
            Assert.Equal("90", await ScalarAsync(connection, "SELECT value FROM config.config_threshold_overrides", ct));
        });
    }

    [Fact]
    public async Task WithoutTheGrant_TheToolAnswersPrecondition_AndNamesProvisionRolesSql()
    {
        await RunAsync(async (ds, connection, store, ct) =>
        {
            var role = "ovr_noprov_" + Guid.NewGuid().ToString("N")[..8];
            await ExecAsync(connection, $"CREATE ROLE {role} LOGIN", ct);
            try
            {
                await ExecAsync(connection, $"GRANT USAGE ON SCHEMA collect, config TO {role}; GRANT SELECT ON collect.servers TO {role};", ct);
                var builder = new NpgsqlConnectionStringBuilder(ds.ConnectionString) { Username = role, SearchPath = PgSchemaGenerator.SearchPath };
                await using var restricted = NpgsqlDataSource.Create(builder.ConnectionString);

                var answer = await Set(restricted, store, WritesArm, "srv-7", "cpu.threshold_percent", 90);
                Assert.Equal("precondition", Status(answer));
                Assert.Contains("provision-roles.sql", JsonNode.Parse(answer)!["message"]!.GetValue<string>(), StringComparison.Ordinal);
                Assert.Equal("0", await ScalarAsync(connection, "SELECT count(*) FROM config.config_threshold_overrides", ct));
            }
            finally
            {
                await ExecAsync(connection, $"REVOKE ALL ON ALL TABLES IN SCHEMA collect, config FROM {role}; REVOKE ALL ON SCHEMA collect, config FROM {role}; DROP ROLE {role}", ct);
            }
        });
    }

    [Fact]
    public async Task GetAlertSettings_WithServerName_ShowsTheEffectiveValueAndWhereItComesFrom()
    {
        await RunAsync(async (ds, connection, store, ct) =>
        {
            await ExecAsync(connection, "INSERT INTO config.config_alert_settings (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);
            await ExecAsync(connection, "INSERT INTO config.config_notification (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);
            Assert.Equal("ok", Status(await Set(ds, store, WritesArm, "srv-7", "cpu.threshold_percent", 91)));
            Assert.Equal("ok", Status(await Set(ds, store, WritesArm, "srv-7", "pvs.threshold_percent", 66, database: "shop")));

            var json = JsonNode.Parse(await DarlingMcpAlertTools.GetAlertSettings(ds, "srv-7", ct))!;
            var view = json["server_view"]!;
            JsonNode? Row(string key) => System.Linq.Enumerable.First(view["thresholds"]!.AsArray(), n => n!["key"]!.GetValue<string>() == key);
            Assert.Equal(91, Row("cpu.threshold_percent")!["effective_value"]!.GetValue<int>());
            Assert.Equal("server_override", Row("cpu.threshold_percent")!["source"]!.GetValue<string>());
            Assert.Equal("global", Row("blocking.count_threshold")!["source"]!.GetValue<string>());
            Assert.Equal(66, view["database_overrides"]![0]!["value"]!.GetValue<double>());
            Assert.Equal("shop", view["database_overrides"]![0]!["database"]!.GetValue<string>());

            /* Another server shows no override and the global value. */
            var other = JsonNode.Parse(await DarlingMcpAlertTools.GetAlertSettings(ds, "srv-8", ct))!["server_view"]!;
            Assert.Equal("global", System.Linq.Enumerable.First(other["thresholds"]!.AsArray(), n => n!["key"]!.GetValue<string>() == "cpu.threshold_percent")!["source"]!.GetValue<string>());
            Assert.Empty(other["server_overrides"]!.AsArray());

            /* No server_name: the payload is the one it always was. */
            Assert.Null(JsonNode.Parse(await DarlingMcpAlertTools.GetAlertSettings(ds, null, ct))!["server_view"]);
        });
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture) ?? "";
    }
}
