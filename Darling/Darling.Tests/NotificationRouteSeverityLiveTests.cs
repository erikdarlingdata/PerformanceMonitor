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
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database (and the MCP facts a role with a per-run name), so the class is deliberately
   NOT [Collection("live-postgres")]. */

/// <summary>
/// #6928: the route severity filter against a live store. V229 on a store that already holds routes (they read <c>both</c>, the
/// constraint holds the three words, and applying the rung again changes nothing), and the MCP create, update and get round trip
/// AS the <c>mcp</c> role with the grants taken from the managed batch, including the <c>invalid</c> answer for a word that is
/// not critical, warning or both.
/// </summary>
public sealed class NotificationRouteSeverityLiveTests
{
    private static readonly string RolePassword = Convert.ToHexString(RandomNumberGenerator.GetBytes(16));

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static DarlingMcpServedPolicy OperatorPolicy() => new(new DarlingMcpWritePolicy.Effective(true, false, false, "ops-lead"));

    private static string Status(string answer) => JsonDocument.Parse(answer).RootElement.GetProperty("status").GetString()!;

    private static int RouteIdOf(string answer) =>
        JsonDocument.Parse(answer).RootElement.GetProperty("route").GetProperty("route_id").GetInt32();

    [Fact]
    public async Task V229_OnAStoreWithExistingRoutes_ReadsBoth_HoldsTheThreeWords_AndAppliesTwiceWithNoChange()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V229 live pin (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var bodySucceeded = false;
        try
        {
            /* A store one rung behind: V229's constraint and column gone, two routes stored (one scoped, one not). */
            await ExecAsync(connection,
                "ALTER TABLE config.config_notification_routes DROP CONSTRAINT ck_config_notification_routes_severities, DROP COLUMN severities", ct);
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 229", ct);
            await ExecAsync(connection,
                "INSERT INTO config.config_notification_routes (metric_match, slack_url) VALUES ('Deadlocks Detected', 'sealed-a')", ct);
            await ExecAsync(connection,
                "INSERT INTO config.config_notification_routes (metric_match, slack_url, scope_server_id) VALUES ('High CPU', 'sealed-b', 11)", ct);

            await PgMigrations.MigrateAsync(connection, ct);

            Assert.Equal("both|both", await ScalarAsync(connection,
                "SELECT string_agg(severities, '|' ORDER BY route_id) FROM config.config_notification_routes", ct));

            /* The three words are accepted and anything else is refused by the CHECK. */
            foreach (var word in new[] { "critical", "warning", "both" })
            {
                await ExecAsync(connection, $"UPDATE config.config_notification_routes SET severities = '{word}' WHERE metric_match = 'Deadlocks Detected'", ct);
            }

            var refused = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection,
                "UPDATE config.config_notification_routes SET severities = 'urgent' WHERE metric_match = 'Deadlocks Detected'", ct));
            Assert.Equal("23514", refused.SqlState);
            var nullRefused = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection,
                "UPDATE config.config_notification_routes SET severities = NULL WHERE metric_match = 'Deadlocks Detected'", ct));
            Assert.Equal("23502", nullRefused.SqlState);

            /* The rung applied again changes nothing: the stored word survives and the constraint is not added twice. */
            await ExecAsync(connection, "UPDATE config.config_notification_routes SET severities = 'critical' WHERE metric_match = 'High CPU'", ct);
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 229", ct);
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal("both|critical", await ScalarAsync(connection,
                "SELECT string_agg(severities, '|' ORDER BY route_id) FROM config.config_notification_routes", ct));
            Assert.Equal("1", await ScalarAsync(connection,
                "SELECT count(*) FROM pg_constraint WHERE conname = 'ck_config_notification_routes_severities'", ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task Mcp_CreateUpdateAndGet_RoundTripTheSeverityFilter_AndABadWordIsInvalid()
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            /* Left out, a new route takes both. */
            var plain = await DarlingMcpAlertTools.CreateNotificationRoute(asMcp, "Blocking", served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal("created", Status(plain));
            Assert.Equal("both", await ScalarAsync(owner, $"SELECT severities FROM config.config_notification_routes WHERE route_id = {RouteIdOf(plain)}", ct));

            /* Any case is accepted and stored lower. */
            var created = await DarlingMcpAlertTools.CreateNotificationRoute(
                asMcp, "Deadlocks", severities: "Critical", served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal("created", Status(created));
            var routeId = RouteIdOf(created);
            Assert.Equal("critical", await ScalarAsync(owner, $"SELECT severities FROM config.config_notification_routes WHERE route_id = {routeId}", ct));
            Assert.Equal("critical", await ReadSeveritiesAsync(asMcp, routeId, ct));

            /* An update changes it, and a field left out keeps it. */
            Assert.Equal("updated", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, severities: "warning", served: OperatorPolicy(), cancellationToken: ct)));
            Assert.Equal("warning", await ReadSeveritiesAsync(asMcp, routeId, ct));
            Assert.Equal("updated", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, enabled: false, served: OperatorPolicy(), cancellationToken: ct)));
            Assert.Equal("warning", await ReadSeveritiesAsync(asMcp, routeId, ct));

            /* A bad word is invalid on both tools, and nothing changes or is stored. */
            var badCreate = await DarlingMcpAlertTools.CreateNotificationRoute(
                asMcp, "Blocking Wait", severities: "urgent", served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal("invalid", Status(badCreate));
            Assert.Contains("severities is critical, warning or both.", badCreate, StringComparison.Ordinal);
            Assert.Equal("2", await ScalarAsync(owner, "SELECT count(*) FROM config.config_notification_routes", ct));
            Assert.Equal("invalid", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, severities: "urgent", served: OperatorPolicy(), cancellationToken: ct)));
            Assert.Equal("warning", await ScalarAsync(owner, $"SELECT severities FROM config.config_notification_routes WHERE route_id = {routeId}", ct));
        });
    }

    private static async Task<string> ReadSeveritiesAsync(NpgsqlDataSource source, int routeId, CancellationToken ct) =>
        JsonDocument.Parse(await DarlingMcpAlertTools.GetNotificationRoutes(source, ct)).RootElement
            .GetProperty("routes").EnumerateArray().Single(r => r.GetProperty("route_id").GetInt32() == routeId)
            .GetProperty("severities").GetString()!;

    private static async Task WithMcpRoleAsync(Func<NpgsqlDataSource, NpgsqlDataSource, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the route severity live tests.");

        var ct = TestContext.Current.CancellationToken;
        var roleName = "route_sev_" + Guid.NewGuid().ToString("N")[..8];
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
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
