/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT
   [Collection("live-postgres")]. */

/// <summary>
/// #6403, group A (the alerting write tools), on a real store: each tool answers with <c>previous</c> and an <c>undo</c>
/// <c>{tool, arguments}</c>, and calling that undo puts the state back. One test per tool; every test makes the change, reads
/// the undo out of the answer, runs it through the same tool surface, and compares the stored state with what it was.
/// </summary>
public sealed class ToolUndoAnswersLiveTests
{
    private static readonly DarlingMcpWritePolicy.Effective WritesArm = new(true, false);

    private static async Task RunAsync(Func<NpgsqlDataSource, NpgsqlConnection, CancellationToken, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the tool undo answer live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            /* The route tools write through the store's own functions, which the owner creates as the managed batch does. */
            await ExecAsync(connection, DarlingManagedRoles.BuildNotificationRouteFunctionsSql("config"), ct);
            await ExecAsync(connection, "INSERT INTO config_service (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);
            await ExecAsync(connection, "INSERT INTO config_alert_settings (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);
            await ExecAsync(connection, "INSERT INTO config_notification (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);
            await ExecAsync(connection, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES (7, 'srv-7', 'h7')", ct);
            await ExecAsync(connection, "INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) "
                + "VALUES (7, 'srv-7', 'srv-7', TRUE, 15, now(), now())", ct);

            var builder = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath };
            await using var source = NpgsqlDataSource.Create(builder.ConnectionString);
            await body(source, connection, ct);
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

    private static async Task<string?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static JsonObject Parse(string answer) => JsonNode.Parse(answer)!.AsObject();

    private static string? Text(JsonObject arguments, string name) => arguments[name]?.ToString();

    private static int? Number(JsonObject arguments, string name) => arguments[name] is { } node ? node.GetValue<int>() : null;

    /// <summary>The undo call a tool answered with, checked for the shared <c>{tool, arguments}</c> shape.</summary>
    private static JsonObject UndoOf(JsonObject answer, string tool)
    {
        var undo = answer["undo"]!.AsObject();
        Assert.Equal(tool, (string)undo["tool"]!);
        return undo["arguments"]!.AsObject();
    }

    private static Task<string> CreateMute(NpgsqlDataSource source, string? server = "srv-7", string metric = "Blocking Detected", string? databasePattern = null,
        string? reason = "known batch", string? expiresAt = null) =>
        DarlingMcpAlertTools.CreateMuteRule(source, server_name: server, metric_name: metric, database_pattern: databasePattern, reason: reason, expires_at: expiresAt);

    /// <summary>A mute rule's wire fields without the ones a re-created rule is allowed to differ in (its id and creation date).</summary>
    private static string Identity(JsonObject rule)
    {
        var copy = (JsonObject)rule.DeepClone();
        copy.Remove("id");
        copy.Remove("created_at_utc");
        return copy.ToJsonString();
    }

    [Fact]
    public async Task CreateMuteRule_AnswersWithAnUndoThatDeletesTheRule()
    {
        await RunAsync(async (source, connection, ct) =>
        {
            var created = Parse(await CreateMute(source));
            Assert.Equal("created", (string)created["status"]!);
            Assert.Null(created["previous"]);
            var arguments = UndoOf(created, "delete_mute_rule");
            Assert.Equal("1", await ScalarAsync(connection, "SELECT count(*) FROM config.config_mute_rules", ct));

            Assert.Equal("deleted", (string)Parse(await DarlingMcpAlertTools.DeleteMuteRule(source, Text(arguments, "rule_id")!))["status"]!);
            Assert.Equal("0", await ScalarAsync(connection, "SELECT count(*) FROM config.config_mute_rules", ct));
        });
    }

    [Fact]
    public async Task DeleteMuteRule_AnswersWithTheDeletedRule_AndAnUndoThatCreatesItAgain()
    {
        await RunAsync(async (source, connection, ct) =>
        {
            var created = Parse(await CreateMute(source, databasePattern: "Sales", expiresAt: "2099-01-01T00:00:00Z"));
            var original = created["mute_rule"]!.AsObject();
            var id = (string)original["id"]!;

            var deleted = Parse(await DarlingMcpAlertTools.DeleteMuteRule(source, id));
            Assert.Equal("deleted", (string)deleted["status"]!);
            Assert.Equal(id, (string)deleted["previous"]!["mute_rule"]!["id"]!);
            var arguments = UndoOf(deleted, "create_mute_rule");
            Assert.Equal("0", await ScalarAsync(connection, "SELECT count(*) FROM config.config_mute_rules", ct));

            var again = Parse(await DarlingMcpAlertTools.CreateMuteRule(
                source, server_name: Text(arguments, "server_name"), metric_name: Text(arguments, "metric_name"), database_pattern: Text(arguments, "database_pattern"),
                query_text_pattern: Text(arguments, "query_text_pattern"), wait_type_pattern: Text(arguments, "wait_type_pattern"), job_name_pattern: Text(arguments, "job_name_pattern"),
                reason: Text(arguments, "reason"), expires_at: Text(arguments, "expires_at"), server_id: Number(arguments, "server_id"),
                client_app_pattern: Text(arguments, "client_app_pattern"), external_source: Text(arguments, "external_source"), starts_at: Text(arguments, "starts_at")));
            Assert.Equal("created", (string)again["status"]!);
            Assert.Equal(Identity(original), Identity(again["mute_rule"]!.AsObject()));
        });
    }

    [Theory]
    [InlineData("{\"reason\":\"root cause found\",\"database_pattern\":\"Sales\",\"expires_at_utc\":\"2099-01-01T00:00:00Z\"}", "updated")]
    [InlineData("{\"reason\":\"known batch\"}", "unchanged")]
    public async Task UpdateMuteRule_AnswersWithThePreviousFields_AndAnUndoThatPutsThemBack(string changes, string status)
    {
        await RunAsync(async (source, _, _) =>
        {
            var created = Parse(await CreateMute(source));
            var original = created["mute_rule"]!.AsObject();
            var id = (string)original["id"]!;

            var updated = Parse(await DarlingMcpAlertTools.UpdateMuteRule(source, id, changes));
            Assert.Equal(status, (string)updated["status"]!);
            Assert.Equal("known batch", (string)updated["previous"]!["reason"]!);
            var arguments = UndoOf(updated, "update_mute_rule");

            var restored = Parse(await DarlingMcpAlertTools.UpdateMuteRule(source, Text(arguments, "rule_id")!, Text(arguments, "changes_json")!));
            var after = Parse(await DarlingMcpAlertTools.GetMuteRules(source));
            var stored = after["mute_rules"]!.AsArray().Select(r => r!.AsObject()).Single(r => (string)r["id"]! == id);
            stored.Remove("state");
            Assert.Equal(Identity(original), Identity(stored));
            Assert.Contains((string)restored["status"]!, new[] { "updated", "unchanged" });
        });
    }

    [Fact]
    public async Task UpdateAlertSettings_AnswersWithThePreviousSettings_AndAnUndoThatRestoresThem()
    {
        await RunAsync(async (source, connection, ct) =>
        {
            const string read = "SELECT cpu_threshold_percent || '/' || (SELECT email_cooldown_minutes FROM config_notification WHERE id = 1) FROM config_alert_settings WHERE id = 1";
            var before = await ScalarAsync(connection, read, ct);

            var updated = Parse(await DarlingMcpAlertTools.UpdateAlertSettings(source, "{\"cpu\":{\"threshold_percent\":77},\"delivery\":{\"cooldown_minutes\":33}}"));
            Assert.Equal("updated", (string)updated["status"]!);
            Assert.NotEqual(before, await ScalarAsync(connection, read, ct));
            Assert.Equal("77/33", await ScalarAsync(connection, read, ct));
            Assert.NotEqual(77, (int)updated["previous"]!["cpu"]!["threshold_percent"]!);
            var arguments = UndoOf(updated, "update_alert_settings");

            Assert.Equal("updated", (string)Parse(await DarlingMcpAlertTools.UpdateAlertSettings(source, Text(arguments, "settings_json")!))["status"]!);
            Assert.Equal(before, await ScalarAsync(connection, read, ct));
        });
    }

    /// <summary>
    /// #6403: <c>previous</c> is what the write REPLACED, not what the row held a moment before. Another writer holds the settings row
    /// while update_alert_settings runs; the tool waits on the row, then reads and writes after that writer commits, so the answer
    /// names the other writer's value and its undo restores it. A pre-write read taken before the transaction would name the old one.
    /// </summary>
    [Fact]
    public async Task UpdateAlertSettings_PreviousIsReadUnderTheRowLock_SoAConcurrentWriteIsNotMissed()
    {
        await RunAsync(async (source, connection, ct) =>
        {
            const string read = "SELECT cpu_threshold_percent FROM config_alert_settings WHERE id = 1";
            var original = await ScalarAsync(connection, read, ct);
            Assert.NotEqual("61", original);

            await using var other = await connection.BeginTransactionAsync(ct);
            await using (var hold = new NpgsqlCommand("UPDATE config_alert_settings SET cpu_threshold_percent = 61 WHERE id = 1", connection, other))
            {
                await hold.ExecuteNonQueryAsync(ct);
            }

            var pending = DarlingMcpAlertTools.UpdateAlertSettings(source, "{\"cpu\":{\"threshold_percent\":77}}");
            await Task.Delay(1500, ct);
            Assert.False(pending.IsCompleted, "the update should wait for the row the other writer holds");
            await other.CommitAsync(ct);

            var updated = Parse(await pending);
            Assert.Equal("updated", (string)updated["status"]!);
            Assert.Equal(61, (int)updated["previous"]!["cpu"]!["threshold_percent"]!);
            var arguments = UndoOf(updated, "update_alert_settings");

            await DarlingMcpAlertTools.UpdateAlertSettings(source, Text(arguments, "settings_json")!);
            Assert.Equal("61", await ScalarAsync(connection, read, ct));
        });
    }

    [Fact]
    public async Task SetAlertThresholdOverride_AnswersWithAnUndo_ThatClearsANewOverride_AndRestoresAPreviousOne()
    {
        await RunAsync(async (source, connection, ct) =>
        {
            var store = new InvestigationRequestStore(source, 30);
            Task<string> Set(string key, int? value, bool clear = false) =>
                DarlingMcpThresholdOverrideTools.SetAsync(source, WritesArm, store, NullLogger.Instance, "srv-7", key, value, null, clear, null, null, ct);
            const string read = "SELECT value FROM config.config_threshold_overrides WHERE server_id = 7 AND threshold_key = 'cpu.threshold_percent'";

            /* No override before: the undo clears. */
            var first = Parse(await Set("cpu.threshold_percent", 90));
            Assert.Equal("90", await ScalarAsync(connection, read, ct));
            var clearArguments = UndoOf(first, "set_alert_threshold_override");
            Assert.True((bool)clearArguments["clear"]!);
            await Set(Text(clearArguments, "key")!, null, clear: true);
            Assert.Equal("", await ScalarAsync(connection, read, ct));

            /* An override before: the undo sets the old value again. */
            await Set("cpu.threshold_percent", 90);
            var second = Parse(await Set("cpu.threshold_percent", 95));
            Assert.Equal("95", await ScalarAsync(connection, read, ct));
            var setArguments = UndoOf(second, "set_alert_threshold_override");
            Assert.Equal(90, Number(setArguments, "value"));
            await Set(Text(setArguments, "key")!, Number(setArguments, "value"));
            Assert.Equal("90", await ScalarAsync(connection, read, ct));

            /* A clear carries the removed value in its undo. */
            var cleared = Parse(await Set("cpu.threshold_percent", null, clear: true));
            Assert.Equal(90, Number(UndoOf(cleared, "set_alert_threshold_override"), "value"));
        });
    }

    [Fact]
    public async Task MuteAnalysisFinding_AnswersWithAnUndoThatUnmutesIt()
    {
        await RunAsync(async (source, connection, ct) =>
        {
            var service = new DarlingAnalysisService(source);
            const string count = "SELECT count(*) FROM analysis_muted WHERE story_path_hash = 'n6403-hash'";

            var muted = Parse(await DarlingMcpTools.MuteAnalysisFinding(service, source, "n6403-hash", null, "known"));
            Assert.True((bool)muted["registered"]!);
            Assert.Null(muted["previous"]);
            var arguments = UndoOf(muted, "unmute_analysis_finding");
            Assert.Equal("1", await ScalarAsync(connection, count, ct));

            var unmuted = Parse(await DarlingMcpTools.UnmuteAnalysisFinding(service, source, Text(arguments, "story_path_hash")!));
            Assert.Equal("unmuted", (string)unmuted["status"]!);
            Assert.Equal("0", await ScalarAsync(connection, count, ct));

            /* A mute that was already there changes nothing, so there is nothing to undo. */
            await DarlingMcpTools.MuteAnalysisFinding(service, source, "n6403-hash", null, "known");
            var again = Parse(await DarlingMcpTools.MuteAnalysisFinding(service, source, "n6403-hash", null, "known"));
            Assert.True((bool)again["already_muted"]!);
            Assert.StartsWith("None needed", (string)again["undo"]!, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task DeleteNotificationRoute_AnswersWithTheRoute_AndAnUndoThatCreatesItAgain_AndAnUnchangedUpdateCarriesAnUndo()
    {
        await RunAsync(async (source, connection, ct) =>
        {
            var created = Parse(await DarlingMcpAlertTools.CreateNotificationRoute(
                source, "Blocking Detected", enabled: false, send_resolutions: "never", email_recipients: "ops@example.com", cancellationToken: ct));
            Assert.True((string)created["status"]! == "created", created.ToJsonString());
            Assert.Null(created["previous"]);
            var routeId = (int)created["route"]!["route_id"]!;

            /* An update that changes nothing still says what it held and how to put it back. */
            var unchanged = Parse(await DarlingMcpAlertTools.UpdateNotificationRoute(source, routeId, cancellationToken: ct));
            Assert.Equal("unchanged", (string)unchanged["status"]!);
            Assert.Equal(routeId, (int)unchanged["previous"]!["route_id"]!);
            Assert.Equal(routeId, Number(UndoOf(unchanged, "update_notification_route"), "route_id"));

            var deleted = Parse(await DarlingMcpAlertTools.DeleteNotificationRoute(source, routeId));
            Assert.Equal("deleted", (string)deleted["status"]!);
            Assert.Equal(routeId, (int)deleted["previous"]!["route_id"]!);
            var arguments = UndoOf(deleted, "create_notification_route");
            Assert.Equal("0", await ScalarAsync(connection, "SELECT count(*) FROM config.config_notification_routes", ct));

            var again = Parse(await DarlingMcpAlertTools.CreateNotificationRoute(
                source, Text(arguments, "metric_match")!, enabled: arguments["enabled"]!.GetValue<bool>(), send_resolutions: Text(arguments, "send_resolutions"),
                email_recipients: Text(arguments, "email_recipients"), cancellationToken: ct));
            Assert.Equal("created", (string)again["status"]!);
            var route = again["route"]!.AsObject();
            Assert.Equal((string)created["route"]!["metric_match"]!, (string)route["metric_match"]!);
            Assert.Equal((bool)created["route"]!["enabled"]!, (bool)route["enabled"]!);
            Assert.Equal((string?)created["route"]!["smtp_recipients"], (string?)route["smtp_recipients"]);
            Assert.NotEqual(routeId, (int)route["route_id"]!);
        });
    }
}
