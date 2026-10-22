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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT
   [Collection("live-postgres")]. */

/// <summary>
/// #5917 against #5915 and #5916: each Level 2 tool makes its change through the real gate, and the undo then reads what the
/// tool journaled (the shared image records in AgentChangeImages.cs) and puts the row back. One test per path: a mute created,
/// updated, deleted and turned off; a custom rule created, disabled and deleted; an override set over a previous one, set with
/// none, and cleared; plus the two refusals that matter (a call that changed nothing, and a row someone changed since).
/// </summary>
public sealed class AgentChangeToolUndoLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly DarlingAgentActionSeat AgentSeat = new(new DarlingMcpWritePolicy.Effective(false, false, Investigations: true));
    private static readonly DarlingMcpWritePolicy.Effective AgentArm = new(false, false, Investigations: true);

    private const string Login = "erik";
    private const string Scope7 = "\"scope\":{\"mode\":\"servers\",\"servers\":[\"srv-7\"]}";

    private static string Definition(int warn) =>
        "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"hours\":1}," +
        "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":" + warn.ToString(CultureInfo.InvariantCulture) + "}," + Scope7 + "}";

    private sealed record Ctx(NpgsqlDataSource Source, NpgsqlConnection Connection, InvestigationRequestStore Store, AgentChangeUndo Undo,
        string Arg, CancellationToken Ct);

    private static async Task RunAsync(Func<Ctx, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the agent change tool undo live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await ExecAsync(connection, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES (7, 'srv-7', 'h7'), (8, 'srv-8', 'h8')", ct);
            await ExecAsync(connection, "INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) "
                + "VALUES (7, 'srv-7', 'srv-7', TRUE, 15, now(), now()), (8, 'srv-8', 'srv-8', TRUE, 15, now(), now())", ct);
            await ExecAsync(connection, "INSERT INTO config.config_notification_routes (metric_match, agent_level) VALUES ('Blocking', 2)", ct);

            var builder = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath };
            await using var source = NpgsqlDataSource.Create(builder.ConnectionString);
            var store = new InvestigationRequestStore(source, 30);
            var alert = new InvestigationAlert(1, 7, "srv-7", "blocking", "Blocking", DateTime.UtcNow, Array.Empty<string>(), null, 20, 100,
                LeaseMinutes: 15, ExpiryMinutes: 240) { AutonomyLevel = 2 };
            var opened = await store.OpenOrJoinAsync(alert, ct);
            var claim = await store.ClaimAsync(opened.RequestId!.Value, "agent-a", ct);
            var arg = AgentActionArgumentTests.Arg(opened.RequestId.Value.ToString(), claim.ClaimId!.Value.ToString());
            await body(new Ctx(source, connection, store, new AgentChangeUndo(source, 30), arg, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string?> ScalarAsync(Ctx c, string sql) => await ScalarAsync(c.Connection, sql, c.Ct);

    private static async Task<string?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static JsonObject Parse(string answer) => JsonNode.Parse(answer)!.AsObject();

    /// <summary>The newest <c>action_called</c> row the named tool wrote.</summary>
    private static async Task<long> CalledAsync(Ctx c, string tool) =>
        long.Parse((await ScalarAsync(c, $"SELECT max(journal_id) FROM config.investigation_journal WHERE event = 'action_called' AND detail->>'tool' = '{tool}'"))!, CultureInfo.InvariantCulture);

    private static async Task<AgentUndoResult> UndoAsync(Ctx c, string tool)
    {
        var called = await CalledAsync(c, tool);
        var result = await c.Undo.UndoAsync(called, Login, c.Ct);
        if (result.Undone)
            Assert.Equal("person:erik", await ScalarAsync(c, $"SELECT actor FROM config.investigation_journal WHERE event = 'action_undone' AND detail->>'called_id' = '{called}'"));
        return result;
    }

    // ---------------------------------------------------------------- mute rules

    private static Task<string> CreateMute(Ctx c) =>
        DarlingMcpAlertTools.CreateMuteRule(c.Source, null, "Blocking Detected", null, null, null, null, "known batch", null, 7, null, null, c.Arg, AgentSeat);

    [Fact]
    public async Task AMuteTheAgentCreated_IsDeletedByTheUndo()
    {
        await RunAsync(async c =>
        {
            var id = (string)Parse(await CreateMute(c))["mute_rule"]!["id"]!;
            Assert.Equal("1", await ScalarAsync(c, $"SELECT count(*) FROM config.config_mute_rules WHERE id = '{id}'"));

            var result = await UndoAsync(c, "create_mute_rule");
            Assert.True(result.Undone, result.Words);
            Assert.Equal("0", await ScalarAsync(c, "SELECT count(*) FROM config.config_mute_rules"));
        });
    }

    [Fact]
    public async Task ACreateThatRepeatedAnExistingMute_ChangedNothing_AndItsUndoRefusesWithoutDeletingTheRule()
    {
        await RunAsync(async c =>
        {
            var id = (string)Parse(await CreateMute(c))["mute_rule"]!["id"]!;
            var again = Parse(await CreateMute(c));
            Assert.Equal("already_exists", (string?)again["status"]);
            Assert.Equal(id, (string?)again["rule_id"]);

            var result = await UndoAsync(c, "create_mute_rule");
            Assert.False(result.Undone);
            Assert.Equal("not_undoable", result.Code);
            Assert.Equal("1", await ScalarAsync(c, $"SELECT count(*) FROM config.config_mute_rules WHERE id = '{id}'"));
        });
    }

    [Fact]
    public async Task AMuteTheAgentUpdated_GetsItsBeforeRowBack()
    {
        await RunAsync(async c =>
        {
            var id = (string)Parse(await CreateMute(c))["mute_rule"]!["id"]!;
            var before = await ScalarAsync(c, $"SELECT reason || '|' || enabled || '|' || coalesce(database_pattern, '-') FROM config.config_mute_rules WHERE id = '{id}'");
            Assert.Equal("updated", (string?)Parse(await DarlingMcpAlertTools.UpdateMuteRule(c.Source, id, "{\"reason\":\"agent edit\",\"database_pattern\":\"shop\"}", c.Arg, AgentSeat))["status"]);
            Assert.NotEqual(before, await ScalarAsync(c, $"SELECT reason || '|' || enabled || '|' || coalesce(database_pattern, '-') FROM config.config_mute_rules WHERE id = '{id}'"));

            var result = await UndoAsync(c, "update_mute_rule");
            Assert.True(result.Undone, result.Words);
            Assert.Equal(before, await ScalarAsync(c, $"SELECT reason || '|' || enabled || '|' || coalesce(database_pattern, '-') FROM config.config_mute_rules WHERE id = '{id}'"));
        });
    }

    [Fact]
    public async Task AMuteTheAgentTurnedOff_IsTurnedBackOn()
    {
        await RunAsync(async c =>
        {
            var id = (string)Parse(await CreateMute(c))["mute_rule"]!["id"]!;
            Assert.Equal("updated", (string?)Parse(await DarlingMcpAlertTools.SetMuteRuleEnabled(c.Source, id, false, c.Arg, AgentSeat))["status"]);
            Assert.Equal("False", await ScalarAsync(c, $"SELECT enabled FROM config.config_mute_rules WHERE id = '{id}'"));

            var result = await UndoAsync(c, "set_mute_rule_enabled");
            Assert.True(result.Undone, result.Words);
            Assert.Equal("True", await ScalarAsync(c, $"SELECT enabled FROM config.config_mute_rules WHERE id = '{id}'"));
        });
    }

    /// <summary>#6403: the Level 2 answer of set_mute_rule_enabled carries previous and an undo that, run through the writes surface, turns the rule back on.</summary>
    [Fact]
    public async Task TheLevel2SetMuteRuleEnabledAnswer_CarriesThePreviousValue_AndAnUndoThatRestoresIt()
    {
        await RunAsync(async c =>
        {
            var id = (string)Parse(await CreateMute(c))["mute_rule"]!["id"]!;
            var answer = Parse(await DarlingMcpAlertTools.SetMuteRuleEnabled(c.Source, id, false, c.Arg, AgentSeat));
            Assert.Equal("updated", (string?)answer["status"]);
            Assert.True((bool)answer["previous"]!["enabled"]!);
            Assert.Equal("set_mute_rule_enabled", (string?)answer["undo"]!["tool"]);

            var arguments = answer["undo"]!["arguments"]!;
            await DarlingMcpAlertTools.SetMuteRuleEnabled(c.Source, (string)arguments["rule_id"]!, (bool)arguments["enabled"]!);
            Assert.Equal("True", await ScalarAsync(c, $"SELECT enabled FROM config.config_mute_rules WHERE id = '{id}'"));
        });
    }

    /// <summary>#6403: the Level 2 answer of create_mute_rule carries previous null and an undo that, run through the writes surface, deletes the rule.</summary>
    [Fact]
    public async Task TheLevel2CreateMuteRuleAnswer_CarriesAnUndoThatDeletesTheRule()
    {
        await RunAsync(async c =>
        {
            var answer = Parse(await CreateMute(c));
            Assert.Equal("created", (string?)answer["status"]);
            Assert.Null(answer["previous"]);
            Assert.Equal("delete_mute_rule", (string?)answer["undo"]!["tool"]);
            Assert.Equal("1", await ScalarAsync(c, "SELECT count(*) FROM config.config_mute_rules"));

            var arguments = answer["undo"]!["arguments"]!;
            Assert.Equal("deleted", (string?)Parse(await DarlingMcpAlertTools.DeleteMuteRule(c.Source, (string)arguments["rule_id"]!))["status"]);
            Assert.Equal("0", await ScalarAsync(c, "SELECT count(*) FROM config.config_mute_rules"));
        });
    }

    /// <summary>#6403: the Level 2 answer of update_mute_rule carries the sent fields as they were and an undo that puts them back.</summary>
    [Fact]
    public async Task TheLevel2UpdateMuteRuleAnswer_CarriesThePreviousFields_AndAnUndoThatRestoresThem()
    {
        await RunAsync(async c =>
        {
            const string read = "SELECT reason || '|' || enabled || '|' || coalesce(database_pattern, '-') FROM config.config_mute_rules WHERE id = '{0}'";
            var id = (string)Parse(await CreateMute(c))["mute_rule"]!["id"]!;
            var before = await ScalarAsync(c, string.Format(CultureInfo.InvariantCulture, read, id));

            var answer = Parse(await DarlingMcpAlertTools.UpdateMuteRule(c.Source, id, "{\"reason\":\"agent edit\",\"database_pattern\":\"shop\"}", c.Arg, AgentSeat));
            Assert.Equal("updated", (string?)answer["status"]);
            Assert.StartsWith("known batch", (string?)answer["previous"]!["reason"], StringComparison.Ordinal);
            Assert.Equal("update_mute_rule", (string?)answer["undo"]!["tool"]);
            Assert.NotEqual(before, await ScalarAsync(c, string.Format(CultureInfo.InvariantCulture, read, id)));

            var arguments = answer["undo"]!["arguments"]!;
            await DarlingMcpAlertTools.UpdateMuteRule(c.Source, (string)arguments["rule_id"]!, (string)arguments["changes_json"]!);
            Assert.Equal(before, await ScalarAsync(c, string.Format(CultureInfo.InvariantCulture, read, id)));
        });
    }

    /// <summary>#6403: the Level 2 answer of delete_mute_rule carries the deleted rule and an undo that creates it again.</summary>
    [Fact]
    public async Task TheLevel2DeleteMuteRuleAnswer_CarriesTheDeletedRule_AndAnUndoThatCreatesItAgain()
    {
        await RunAsync(async c =>
        {
            var id = (string)Parse(await CreateMute(c))["mute_rule"]!["id"]!;
            var answer = Parse(await DarlingMcpAlertTools.DeleteMuteRule(c.Source, id, c.Arg, AgentSeat));
            Assert.Equal("deleted", (string?)answer["status"]);
            Assert.Equal(id, (string?)answer["previous"]!["mute_rule"]!["id"]);
            Assert.Equal("create_mute_rule", (string?)answer["undo"]!["tool"]);
            Assert.Equal("0", await ScalarAsync(c, "SELECT count(*) FROM config.config_mute_rules"));

            var arguments = answer["undo"]!["arguments"]!.AsObject();
            string? Text(string name) => arguments[name]?.ToString();
            var again = Parse(await DarlingMcpAlertTools.CreateMuteRule(
                c.Source, server_name: Text("server_name"), metric_name: Text("metric_name"), database_pattern: Text("database_pattern"),
                query_text_pattern: Text("query_text_pattern"), wait_type_pattern: Text("wait_type_pattern"), job_name_pattern: Text("job_name_pattern"),
                reason: Text("reason"), expires_at: Text("expires_at"), server_id: arguments["server_id"] is { } sid ? sid.GetValue<int>() : null,
                client_app_pattern: Text("client_app_pattern"), external_source: Text("external_source"), starts_at: Text("starts_at")));
            Assert.Equal("created", (string?)again["status"]);
            Assert.Equal("1", await ScalarAsync(c, "SELECT count(*) FROM config.config_mute_rules WHERE reason LIKE 'known batch%' AND server_id = 7 AND metric_name = 'Blocking Detected'"));
        });
    }

    [Fact]
    public async Task AMuteTheAgentDeleted_ComesBackWithItsIdAndItsCreatedTime()
    {
        await RunAsync(async c =>
        {
            var id = (string)Parse(await CreateMute(c))["mute_rule"]!["id"]!;
            var before = await ScalarAsync(c, $"SELECT id || '|' || created_at_utc || '|' || reason || '|' || server_id || '|' || metric_name FROM config.config_mute_rules WHERE id = '{id}'");
            Assert.Equal("deleted", (string?)Parse(await DarlingMcpAlertTools.DeleteMuteRule(c.Source, id, c.Arg, AgentSeat))["status"]);
            Assert.Equal("0", await ScalarAsync(c, "SELECT count(*) FROM config.config_mute_rules"));

            var result = await UndoAsync(c, "delete_mute_rule");
            Assert.True(result.Undone, result.Words);
            Assert.Equal(before, await ScalarAsync(c, $"SELECT id || '|' || created_at_utc || '|' || reason || '|' || server_id || '|' || metric_name FROM config.config_mute_rules WHERE id = '{id}'"));
        });
    }

    [Fact]
    public async Task AMuteAPersonEditedAfterTheAgent_IsKept_AndTheUndoSaysSo()
    {
        await RunAsync(async c =>
        {
            var id = (string)Parse(await CreateMute(c))["mute_rule"]!["id"]!;
            await ExecAsync(c.Connection, $"UPDATE config.config_mute_rules SET reason = 'a person edited this' WHERE id = '{id}'", c.Ct);

            var result = await UndoAsync(c, "create_mute_rule");
            Assert.False(result.Undone);
            Assert.Equal("changed_since", result.Code);
            Assert.Contains("changed since", result.Words);
            Assert.Equal("a person edited this", await ScalarAsync(c, $"SELECT reason FROM config.config_mute_rules WHERE id = '{id}'"));
            Assert.Equal("0", await ScalarAsync(c, "SELECT count(*) FROM config.investigation_journal WHERE event = 'action_undone'"));
        });
    }

    // ---------------------------------------------------------------- custom alert rules

    private static async Task<long> CreateRule(Ctx c, bool enabled = true)
    {
        var created = Parse(await DarlingMcpCustomAlertTools.CreateCustomAlertRule(c.Source, "agent rule", Definition(1000), "first", enabled, c.Arg, AgentSeat));
        return (long)created["id"]!;
    }

    [Fact]
    public async Task ARuleTheAgentCreated_IsDeletedByTheUndo()
    {
        await RunAsync(async c =>
        {
            var id = await CreateRule(c);
            Assert.Equal("1", await ScalarAsync(c, $"SELECT count(*) FROM config.custom_alert_rules WHERE id = {id}"));

            var result = await UndoAsync(c, "create_custom_alert_rule");
            Assert.True(result.Undone, result.Words);
            Assert.Equal("0", await ScalarAsync(c, "SELECT count(*) FROM config.custom_alert_rules"));
        });
    }

    [Fact]
    public async Task ARuleTheAgentDisabled_IsEnabledAgainByTheUndo_AndTheVersionMovesForward()
    {
        await RunAsync(async c =>
        {
            var id = await CreateRule(c);
            Assert.Equal("agent rule", (string?)Parse(await DarlingMcpCustomAlertTools.UpdateCustomAlertRule(c.Source, id, 1, null, null, null, false, c.Arg, AgentSeat))["name"]);
            Assert.Equal("False", await ScalarAsync(c, $"SELECT enabled FROM config.custom_alert_rules WHERE id = {id}"));

            var result = await UndoAsync(c, "update_custom_alert_rule");
            Assert.True(result.Undone, result.Words);
            Assert.Equal("true|3|person:erik", await ScalarAsync(c, $"SELECT enabled || '|' || version || '|' || updated_by FROM config.custom_alert_rules WHERE id = {id}"));
        });
    }

    [Fact]
    public async Task ARuleTheAgentDeleted_ComesBackWithItsIdAndDefinition()
    {
        await RunAsync(async c =>
        {
            var id = await CreateRule(c);
            var before = await ScalarAsync(c, $"SELECT id || '|' || name || '|' || description || '|' || definition::text FROM config.custom_alert_rules WHERE id = {id}");
            Assert.Equal("deleted", (string?)Parse(await DarlingMcpCustomAlertTools.DeleteCustomAlertRule(c.Source, id, c.Arg, AgentSeat))["status"]);
            Assert.Equal("0", await ScalarAsync(c, "SELECT count(*) FROM config.custom_alert_rules"));

            var result = await UndoAsync(c, "delete_custom_alert_rule");
            Assert.True(result.Undone, result.Words);
            Assert.Equal(before, await ScalarAsync(c, $"SELECT id || '|' || name || '|' || description || '|' || definition::text FROM config.custom_alert_rules WHERE id = {id}"));
        });
    }

    // ---------------------------------------------------------------- threshold overrides

    private static Task<string> Set(Ctx c, int? value, bool clear = false) =>
        DarlingMcpThresholdOverrideTools.SetAsync(c.Source, AgentArm, c.Store, NullLogger.Instance, "srv-7", "cpu.threshold_percent", value, null, clear, null, c.Arg, c.Ct);

    private const string OverrideValue = "SELECT value || '|' || set_by FROM config.config_threshold_overrides WHERE server_id = 7 AND threshold_key = 'cpu.threshold_percent'";

    [Fact]
    public async Task AnOverrideTheAgentSetOverAPreviousOne_GetsThePreviousValueBack()
    {
        await RunAsync(async c =>
        {
            await ExecAsync(c.Connection, "INSERT INTO config.config_threshold_overrides (server_id, database_name, threshold_key, value, set_by) VALUES (7, NULL, 'cpu.threshold_percent', 85, 'person:erik')", c.Ct);
            Assert.Equal("ok", (string?)Parse(await Set(c, 90))["status"]);
            Assert.Equal("90|agent:agent-a", await ScalarAsync(c, OverrideValue));

            var result = await UndoAsync(c, DarlingMcpThresholdOverrideTools.ToolName);
            Assert.True(result.Undone, result.Words);
            Assert.Equal("85|person:erik", await ScalarAsync(c, OverrideValue));
        });
    }

    [Fact]
    public async Task AnOverrideTheAgentSetWithNoPreviousOne_IsDeletedByTheUndo()
    {
        await RunAsync(async c =>
        {
            Assert.Equal("ok", (string?)Parse(await Set(c, 90))["status"]);
            Assert.Equal("1", await ScalarAsync(c, "SELECT count(*) FROM config.config_threshold_overrides"));

            var result = await UndoAsync(c, DarlingMcpThresholdOverrideTools.ToolName);
            Assert.True(result.Undone, result.Words);
            Assert.Equal("0", await ScalarAsync(c, "SELECT count(*) FROM config.config_threshold_overrides"));
        });
    }

    [Fact]
    public async Task AnOverrideTheAgentCleared_IsPutBack()
    {
        await RunAsync(async c =>
        {
            await ExecAsync(c.Connection, "INSERT INTO config.config_threshold_overrides (server_id, database_name, threshold_key, value, set_by) VALUES (7, NULL, 'cpu.threshold_percent', 85, 'person:erik')", c.Ct);
            Assert.Equal("cleared", (string?)Parse(await Set(c, null, clear: true))["status"]);
            Assert.Equal("0", await ScalarAsync(c, "SELECT count(*) FROM config.config_threshold_overrides"));

            var result = await UndoAsync(c, DarlingMcpThresholdOverrideTools.ToolName);
            Assert.True(result.Undone, result.Words);
            Assert.Equal("85|person:erik", await ScalarAsync(c, OverrideValue));
        });
    }

    [Fact]
    public async Task AnOverrideAPersonChangedAfterTheAgent_IsKept()
    {
        await RunAsync(async c =>
        {
            Assert.Equal("ok", (string?)Parse(await Set(c, 90))["status"]);
            await ExecAsync(c.Connection, "UPDATE config.config_threshold_overrides SET value = 95 WHERE server_id = 7", c.Ct);

            var result = await UndoAsync(c, DarlingMcpThresholdOverrideTools.ToolName);
            Assert.Equal("changed_since", result.Code);
            Assert.StartsWith("95|", await ScalarAsync(c, OverrideValue));
        });
    }
}
