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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live fact mints its own scratch database through ScratchPostgres and never touches another test's
   rows, so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// Pins the rung that adds the Level 1 investigation tables and the agent route columns (V184, #5632 Phase A): its place at
/// the top of the ladder, the shape of its script, the probe's sentinel, and on a live store: a store at V183 holding a route
/// migrates with the route kept (no agent level, the default limits), the agent CHECK refuses an out-of-range limit, the
/// journal's once key is unique, and a second run changes nothing.
/// </summary>
public sealed class InvestigationRungTests
{
    private const int RungVersion = 184;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static PgMigrations.Migration Rung => PgMigrations.Scripts.Single(m => m.Version == RungVersion);

    [Fact]
    public void TheRungIsRegistered_OnceJustBelowTheTopOfTheLadder()
    {
        Assert.Equal("agent-investigations", Rung.Name);
        Assert.Single(PgMigrations.Scripts, m => m.Version == RungVersion);
        Assert.Equal(InvestigationTables.RungVersion, RungVersion);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        /* V185 (#5812) took the top of the ladder; this rung stays registered just below it. */
        Assert.Single(PgMigrations.Scripts, m => m.Version == RungVersion);
        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);
        Assert.Equal(InvestigationTables.CreateSql, Rung.Sql);
    }

    [Fact]
    public void TheScript_IsGuarded_MovesNoRow_AndTakesTheWritesAway()
    {
        var sql = Rung.Sql;
        Assert.Contains("ADD COLUMN IF NOT EXISTS agent_max_per_hour integer NOT NULL DEFAULT 20,", sql, StringComparison.Ordinal);
        Assert.Contains("ADD COLUMN IF NOT EXISTS agent_max_per_day integer NOT NULL DEFAULT 100,", sql, StringComparison.Ordinal);
        Assert.Contains("agent_max_per_hour BETWEEN 1 AND 60", sql, StringComparison.Ordinal);
        Assert.Contains("agent_max_per_day BETWEEN 1 AND 500", sql, StringComparison.Ordinal);
        Assert.Contains("ON config.investigation_journal (once_key) WHERE once_key IS NOT NULL", sql, StringComparison.Ordinal);
        foreach (var verb in new[] { "INSERT INTO", "UPDATE config", "DELETE FROM", "DROP ", "GRANT " })
        {
            Assert.DoesNotContain(verb, sql, StringComparison.Ordinal);
        }

        /* Every CREATE is guarded, and the last statement takes the default write privileges away. */
        foreach (var line in sql.Split('\n').Where(l => l.StartsWith("CREATE ", StringComparison.Ordinal)))
        {
            Assert.Contains("IF NOT EXISTS", line, StringComparison.Ordinal);
        }

        Assert.True(sql.LastIndexOf("REVOKE INSERT, UPDATE, DELETE", StringComparison.Ordinal) > sql.LastIndexOf("CREATE ", StringComparison.Ordinal));
    }

    /// <summary>A store at V183 that holds a route takes V184: the route keeps its effect (no agent level, the default
    /// limits), the agent CHECK refuses an hourly limit of 61, the journal refuses a second row with the same once key, and a
    /// second run of the rung changes nothing.</summary>
    [Fact]
    public async Task AStoreAtV183WithARoute_TakesV184_AndKeepsTheRoute()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V184 migration pin.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await RungRewindArtifacts.DropSentinelsAboveAsync(connection, RungVersion - 1, ct);
        await ExecAsync(connection, @"
DELETE FROM darling_schema_version WHERE version >= 184;
INSERT INTO config.config_notification_routes (metric_match, teams_url) VALUES ('Blocking', 'https://example.invalid/hook');", ct);
        Assert.Equal(183, await ProbedVersionAsync(connection, ct));

        /* V184 and the rung above it (V185, #5812) both apply. */
        Assert.Equal(StorageVersion.SchemaVersion - 183, await PgMigrations.MigrateAsync(connection, ct));
        await AssertMigratedAsync(connection, ct);

        await ExecAsync(connection, Rung.Sql, ct);
        await AssertMigratedAsync(connection, ct);

        var tooMany = await Assert.ThrowsAsync<PostgresException>(() =>
            ExecAsync(connection, "UPDATE config.config_notification_routes SET agent_max_per_hour = 61;", ct));
        Assert.Equal("ck_config_notification_routes_agent", tooMany.ConstraintName);

        await ExecAsync(connection, @"
INSERT INTO config.investigation_journal (event, actor, once_key) VALUES ('limit_reached', 'service', 'limit:1:2026-10-14T19');", ct);
        var twice = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection, @"
INSERT INTO config.investigation_journal (event, actor, once_key) VALUES ('limit_reached', 'service', 'limit:1:2026-10-14T19');", ct));
        Assert.Equal(PostgresErrorCodes.UniqueViolation, twice.SqlState);
    }

    private static async Task AssertMigratedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        Assert.Equal(StorageVersion.SchemaVersion, await ProbedVersionAsync(connection, ct));
        Assert.Equal("1|1|20|100|15|240", await ScalarAsync(connection, @"
SELECT count(*) || '|' || count(*) FILTER (WHERE agent_level IS NULL) || '|' || max(agent_max_per_hour) || '|' || max(agent_max_per_day)
       || '|' || max(agent_lease_minutes) || '|' || max(agent_expiry_minutes)
FROM config.config_notification_routes", ct));
        Assert.Equal("6", await ScalarAsync(connection, @"
SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'config' AND c.relkind = 'r'
  AND c.relname IN ('investigation_requests', 'investigation_request_alerts', 'investigation_reports',
                    'investigation_journal', 'investigation_notes', 'agent_environment_profiles')", ct));
    }

    private static async Task<int> ProbedVersionAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var count = method.GetParameters().Length;
        await using var command = new NpgsqlCommand(ViewerDataService.StoreSchemaProbeSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        var args = Enumerable.Range(0, count).Select(i => (object)reader.GetBoolean(i)).ToArray();
        return (int)method.Invoke(null, args)!;
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
}
