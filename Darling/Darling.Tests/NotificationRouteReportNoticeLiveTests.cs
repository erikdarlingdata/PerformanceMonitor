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
using System.Security.Cryptography;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database (and the MCP facts a role with a per-run name), so the class is deliberately
   NOT [Collection("live-postgres")]. */

/// <summary>
/// #6979: the route report notice setting against a live store. V233 on a store that already holds routes (they read <c>off</c>, the
/// constraint holds the three words, and applying the rung again changes nothing), the writer's refusal of an unknown word and its round
/// trip of each valid one, the audit row of a change to an agent route, and the MCP create, update and get round trip AS the <c>mcp</c>
/// role with the grants taken from the managed batch.
/// </summary>
public sealed class NotificationRouteReportNoticeLiveTests
{
    private static readonly string RolePassword = Convert.ToHexString(RandomNumberGenerator.GetBytes(16));

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static DarlingMcpServedPolicy OperatorPolicy() => new(new DarlingMcpWritePolicy.Effective(true, false, false, "ops-lead"));

    private static string Status(string answer) => JsonDocument.Parse(answer).RootElement.GetProperty("status").GetString()!;

    private static int RouteIdOf(string answer) =>
        JsonDocument.Parse(answer).RootElement.GetProperty("route").GetProperty("route_id").GetInt32();

    [Theory]
    [InlineData(null, "off")]
    [InlineData("", "off")]
    [InlineData("  ALL ", "all")]
    [InlineData("Needs_Person_Or_Change", "needs_person_or_change")]
    [InlineData("sometimes", "off")]
    public void NormalizeReportNotice_ReadsAnUnknownWordAsOff(string? stored, string expected) =>
        Assert.Equal(expected, NotificationRouteKinds.NormalizeReportNotice(stored));

    [Theory]
    [InlineData("off")]
    [InlineData("needs_person_or_change")]
    [InlineData("ALL")]
    [InlineData("")]
    public void TheWriter_AcceptsEachValidWord(string word) =>
        Assert.Null(NotificationRouteWriter.Validate(new NotificationRouteDraft { MetricMatch = "Blocking", SmtpRecipients = "ops@example.com", ReportNotice = word }));

    [Fact]
    public void TheWriter_RefusesAnUnknownWord_WithItsSentence() =>
        Assert.Equal("report_notice is off, needs_person_or_change or all.", NotificationRouteWriter.Validate(
            new NotificationRouteDraft { MetricMatch = "Blocking", SmtpRecipients = "ops@example.com", ReportNotice = "sometimes" }));

    [Fact]
    public async Task V233_OnAStoreWithExistingRoutes_ReadsOff_HoldsTheThreeWords_AndAppliesTwiceWithNoChange()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V233 live pin (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var bodySucceeded = false;
        try
        {
            /* A store one rung behind: V233's constraint and column gone, two routes stored (one scoped, one not). */
            await ExecAsync(connection,
                "ALTER TABLE config.config_notification_routes DROP CONSTRAINT ck_config_notification_routes_report_notice, DROP COLUMN report_notice", ct);
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 233", ct);
            await ExecAsync(connection,
                "INSERT INTO config.config_notification_routes (metric_match, slack_url) VALUES ('Deadlocks Detected', 'sealed-a')", ct);
            await ExecAsync(connection,
                "INSERT INTO config.config_notification_routes (metric_match, slack_url, scope_server_id) VALUES ('High CPU', 'sealed-b', 11)", ct);

            await PgMigrations.MigrateAsync(connection, ct);

            Assert.Equal("off|off", await ScalarAsync(connection,
                "SELECT string_agg(report_notice, '|' ORDER BY route_id) FROM config.config_notification_routes", ct));

            /* The three words are accepted and anything else is refused by the CHECK. */
            foreach (var word in new[] { "needs_person_or_change", "all", "off" })
            {
                await ExecAsync(connection, $"UPDATE config.config_notification_routes SET report_notice = '{word}' WHERE metric_match = 'Deadlocks Detected'", ct);
            }

            var refused = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection,
                "UPDATE config.config_notification_routes SET report_notice = 'sometimes' WHERE metric_match = 'Deadlocks Detected'", ct));
            Assert.Equal("23514", refused.SqlState);
            var nullRefused = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection,
                "UPDATE config.config_notification_routes SET report_notice = NULL WHERE metric_match = 'Deadlocks Detected'", ct));
            Assert.Equal("23502", nullRefused.SqlState);

            /* The rung applied again changes nothing: the stored word survives and the constraint is not added twice. */
            await ExecAsync(connection, "UPDATE config.config_notification_routes SET report_notice = 'all' WHERE metric_match = 'High CPU'", ct);
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 233", ct);
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal("off|all", await ScalarAsync(connection,
                "SELECT string_agg(report_notice, '|' ORDER BY route_id) FROM config.config_notification_routes", ct));
            Assert.Equal("1", await ScalarAsync(connection,
                "SELECT count(*) FROM pg_constraint WHERE conname = 'ck_config_notification_routes_report_notice'", ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>The viewer and web writer: each valid word is stored and read back, an unknown word is refused before the store is touched,
    /// and changing the setting of an agent route writes a route_agent_changed row naming it with the words before and after. The same change
    /// to a route with no agent level writes nothing.</summary>
    [Fact]
    public async Task Writer_StoresEachWord_RefusesAnUnknownOne_AndJournalsAChangeToAnAgentRouteOnly()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the route report notice writer pin (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        await using var owner = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            var proxies = new WebhookProxies(null, null, null, null);
            var agentRoute = await NotificationRouteWriter.SaveAsync(
                owner, new NotificationRouteDraft { MetricMatch = "Blocking", SmtpRecipients = "ops@example.com", AgentLevel = 2 }, null, proxies, null, null, 30, ct);
            var plainRoute = await NotificationRouteWriter.SaveAsync(
                owner, new NotificationRouteDraft { MetricMatch = "Deadlocks", SmtpRecipients = "ops@example.com", ReportNotice = "all" }, null, proxies, null, null, 30, ct);
            const string Journal = "SELECT count(*) FROM config.investigation_journal WHERE event = 'route_agent_changed'";

            Assert.Equal("off", await ScalarAsync(owner, $"SELECT report_notice FROM config.config_notification_routes WHERE route_id = {agentRoute}", ct));
            Assert.Equal("all", await ScalarAsync(owner, $"SELECT report_notice FROM config.config_notification_routes WHERE route_id = {plainRoute}", ct));
            Assert.Equal("0", await ScalarAsync(owner, Journal, ct));

            /* An unknown word is refused with its sentence, and nothing is stored. */
            var stored = (await NotificationRouteWriter.ReadStoredAsync(owner, agentRoute, 30, ct))!;
            Assert.Equal("off", stored.ReportNotice);
            var bad = stored.Clone();
            bad.ReportNotice = "sometimes";
            var refusal = await Assert.ThrowsAsync<NotificationRouteRefusedException>(() =>
                NotificationRouteWriter.SaveAsync(owner, bad, stored, proxies, null, null, 30, ct, "bob"));
            Assert.Contains("report_notice is off, needs_person_or_change or all.", refusal.Message, StringComparison.Ordinal);
            Assert.Equal("off", await ScalarAsync(owner, $"SELECT report_notice FROM config.config_notification_routes WHERE route_id = {agentRoute}", ct));

            /* Off to needs_person_or_change on an agent route: stored, read back, and journaled with the words before and after. */
            var typed = stored.Clone();
            typed.ReportNotice = "needs_person_or_change";
            await NotificationRouteWriter.SaveAsync(owner, typed, stored, proxies, null, null, 30, ct, "bob");
            Assert.Equal("needs_person_or_change", (await NotificationRouteWriter.ReadStoredAsync(owner, agentRoute, 30, ct))!.ReportNotice);
            Assert.Equal("1", await ScalarAsync(owner, Journal, ct));
            Assert.Equal("person:bob|changed report_notice|off|needs_person_or_change", await ScalarAsync(owner,
                "SELECT actor || '|' || reason || '|' || (detail->'before'->>'report_notice') || '|' || (detail->'after'->>'report_notice') FROM config.investigation_journal WHERE event = 'route_agent_changed' ORDER BY journal_id DESC LIMIT 1", ct));

            /* The last word, and a route with no agent level is stored but not journaled. */
            var stored2 = (await NotificationRouteWriter.ReadStoredAsync(owner, agentRoute, 30, ct))!;
            var typed2 = stored2.Clone();
            typed2.ReportNotice = "all";
            await NotificationRouteWriter.SaveAsync(owner, typed2, stored2, proxies, null, null, 30, ct, "bob");
            Assert.Equal("all", await ScalarAsync(owner, $"SELECT report_notice FROM config.config_notification_routes WHERE route_id = {agentRoute}", ct));
            Assert.Equal("2", await ScalarAsync(owner, Journal, ct));

            var plainStored = (await NotificationRouteWriter.ReadStoredAsync(owner, plainRoute, 30, ct))!;
            var plainTyped = plainStored.Clone();
            plainTyped.ReportNotice = "off";
            await NotificationRouteWriter.SaveAsync(owner, plainTyped, plainStored, proxies, null, null, 30, ct, "bob");
            Assert.Equal("off", await ScalarAsync(owner, $"SELECT report_notice FROM config.config_notification_routes WHERE route_id = {plainRoute}", ct));
            Assert.Equal("2", await ScalarAsync(owner, Journal, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task Mcp_CreateUpdateAndGet_RoundTripTheReportNotice_AndABadWordIsInvalid()
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            /* Left out, a new route is off. */
            var plain = await DarlingMcpAlertTools.CreateNotificationRoute(asMcp, "Blocking", served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal("created", Status(plain));
            Assert.Equal("off", await ScalarAsync(owner, $"SELECT report_notice FROM config.config_notification_routes WHERE route_id = {RouteIdOf(plain)}", ct));

            /* Any case is accepted and stored lower. */
            var created = await DarlingMcpAlertTools.CreateNotificationRoute(
                asMcp, "Deadlocks", report_notice: "Needs_Person_Or_Change", served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal("created", Status(created));
            var routeId = RouteIdOf(created);
            Assert.Equal("needs_person_or_change", await ScalarAsync(owner, $"SELECT report_notice FROM config.config_notification_routes WHERE route_id = {routeId}", ct));
            Assert.Equal("needs_person_or_change", await ReadReportNoticeAsync(asMcp, routeId, ct));

            /* An update changes it, a field left out keeps it, and the undo carries it. */
            var updated = await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, report_notice: "all", served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal("updated", Status(updated));
            Assert.Equal("needs_person_or_change", JsonDocument.Parse(updated).RootElement.GetProperty("undo").GetProperty("arguments").GetProperty("report_notice").GetString());
            Assert.Equal("all", await ReadReportNoticeAsync(asMcp, routeId, ct));
            Assert.Equal("updated", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, enabled: false, served: OperatorPolicy(), cancellationToken: ct)));
            Assert.Equal("all", await ReadReportNoticeAsync(asMcp, routeId, ct));

            /* A bad word is invalid on both tools, and nothing changes or is stored. */
            var badCreate = await DarlingMcpAlertTools.CreateNotificationRoute(
                asMcp, "Blocking Wait", report_notice: "sometimes", served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal("invalid", Status(badCreate));
            Assert.Contains("report_notice is off, needs_person_or_change or all.", badCreate, StringComparison.Ordinal);
            Assert.Equal("2", await ScalarAsync(owner, "SELECT count(*) FROM config.config_notification_routes", ct));
            Assert.Equal("invalid", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, report_notice: "sometimes", served: OperatorPolicy(), cancellationToken: ct)));
            Assert.Equal("all", await ScalarAsync(owner, $"SELECT report_notice FROM config.config_notification_routes WHERE route_id = {routeId}", ct));
        });
    }

    /// <summary>The MCP update tool: an operator who changes the setting of an agent route gets a route_agent_changed row (through the update
    /// function's before row), and the same change to a route with no agent level writes none.</summary>
    [Fact]
    public async Task Mcp_ChangingTheReportNoticeOfAnAgentRoute_IsJournaled_AndOfARouteWithNoAgentLevelIsNot()
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            var agentRoute = RouteIdOf(await DarlingMcpAlertTools.CreateNotificationRoute(
                asMcp, "Blocking", agent_level: 2, served: OperatorPolicy(), cancellationToken: ct));
            var plainRoute = RouteIdOf(await DarlingMcpAlertTools.CreateNotificationRoute(
                asMcp, "Deadlocks", served: OperatorPolicy(), cancellationToken: ct));
            const string Journal = "SELECT count(*) FROM config.investigation_journal WHERE event = 'route_agent_changed'";
            Assert.Equal("1", await ScalarAsync(owner, Journal, ct));

            Assert.Equal("updated", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, agentRoute, report_notice: "all", served: OperatorPolicy(), cancellationToken: ct)));
            Assert.Equal("2", await ScalarAsync(owner, Journal, ct));
            Assert.Equal("operator:ops-lead|changed report_notice|off|all", await ScalarAsync(owner,
                $"SELECT actor || '|' || reason || '|' || (detail->'before'->>'report_notice') || '|' || (detail->'after'->>'report_notice') FROM config.investigation_journal WHERE route_id = {agentRoute} AND event = 'route_agent_changed' ORDER BY journal_id DESC LIMIT 1", ct));

            /* The same word again changes nothing, and a route with no agent level writes no row. */
            await DarlingMcpAlertTools.UpdateNotificationRoute(asMcp, agentRoute, report_notice: "all", served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal("updated", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, plainRoute, report_notice: "needs_person_or_change", served: OperatorPolicy(), cancellationToken: ct)));
            Assert.Equal("2", await ScalarAsync(owner, Journal, ct));
        });
    }

    private static async Task<string> ReadReportNoticeAsync(NpgsqlDataSource source, int routeId, CancellationToken ct) =>
        JsonDocument.Parse(await DarlingMcpAlertTools.GetNotificationRoutes(source, ct)).RootElement
            .GetProperty("routes").EnumerateArray().Single(r => r.GetProperty("route_id").GetInt32() == routeId)
            .GetProperty("report_notice").GetString()!;

    private static async Task WithMcpRoleAsync(Func<NpgsqlDataSource, NpgsqlDataSource, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the route report notice live tests.");

        var ct = TestContext.Current.CancellationToken;
        var roleName = "route_rn_" + Guid.NewGuid().ToString("N")[..8];
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        await using var owner = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            /* The functions and the journal trigger are created by the owner, as the managed batch does; the batch's own grants, retargeted. */
            await ExecAsync(owner, DarlingManagedRoles.BuildNotificationRouteFunctionsSql("config"), ct);
            await ExecAsync(owner, DarlingManagedRoles.BuildJournalEventRulesSql("config"), ct);
            await ExecAsync(owner, $"CREATE ROLE {roleName} LOGIN NOSUPERUSER PASSWORD '{RolePassword}'", ct);
            foreach (var statement in ViewerGrantReplay.StatementsFor(roleName, scratch.DatabaseName, "mcp"))
            {
                await ExecAsync(owner, statement, ct);
            }

            var executes = ViewerGrantReplay.BatchStatements().Where(s =>
                s.StartsWith("GRANT EXECUTE ON FUNCTION config.create_notification_route", StringComparison.Ordinal)
                || s.StartsWith("GRANT EXECUTE ON FUNCTION config.update_notification_route", StringComparison.Ordinal)
                || s.StartsWith("GRANT EXECUTE ON FUNCTION config.set_notification_route_agent", StringComparison.Ordinal)).ToList();
            Assert.Equal(3, executes.Count);
            foreach (var execute in executes)
            {
                var at = execute.LastIndexOf(" TO ", StringComparison.Ordinal);
                Assert.Equal("mcp", execute[(at + 4)..].Trim());
                await ExecAsync(owner, execute[..at] + " TO " + roleName, ct);
            }

            await using var asMcp = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = roleName,
                Password = RolePassword,
                Pooling = false,
            }.ConnectionString);

            await body(owner, asMcp, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, () => ExecAsync(owner, $"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName};", CancellationToken.None));
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static async Task<string?> ScalarAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }
}
