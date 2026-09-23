/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3974: the three TOOL-side reads of the newest <c>pg_server_config</c> snapshot —
/// <see cref="DarlingPgServerConfigReader.CurrentConfigSql"/> (<c>get_pg_server_config</c> and the WPF
/// viewer's PostgreSQL config panel), <see cref="DarlingPgServerConfigReader.OverrideSql"/>
/// (<c>get_pg_server_config</c>'s <c>database_overrides</c> section) and
/// <see cref="DarlingPgLoggingAuditReader.NewestSnapshotSql"/> (<c>get_pg_logging_audit</c>) — carry the same
/// #3928 shape the analysis reads already had: a day's lower bound on both the anchor's
/// <c>MAX(collection_time)</c> and the outer scan, and a fallback to every retained snapshot only when the
/// day found nothing. Unbounded, 366 one-day chunks cost 4.0-8.3 s of cold planning per read; bounded, 0.6 ms
/// warm to 21 ms cold (the issue's measurement).
///
/// <para><b><c>OverrideSql</c> cannot use "the read returned rows" as its retry signal</b> — most servers
/// have zero overrides, so that would run the unbounded fallback on nearly every call, which is the cost this
/// fix removes. Its shape instead threads the anchor's <c>collection_time</c> through a LEFT JOIN so it rides
/// on every call, even one with zero override rows; the retry loop keys on THAT column, never on row count.</para>
/// </summary>
public sealed class PgServerConfigToolBoundTests
{
    private static string Strip(string sql) => Regex.Replace(sql, @"/\*.*?\*/", " ", RegexOptions.Singleline);

    /// <summary><c>CurrentConfigSql</c> and <c>NewestSnapshotSql</c> share the same anchor-and-outer-scan
    /// shape, both keyed on $4/$2 respectively — the two-scan bound <see cref="PgTargetConfigSnapshotBoundTests"/>
    /// pins for the analysis family.</summary>
    public static TheoryData<string, string> AnchoredReads()
    {
        var data = new TheoryData<string, string>();
        data.Add(nameof(DarlingPgServerConfigReader.CurrentConfigSql), "$4");
        data.Add(nameof(DarlingPgLoggingAuditReader.NewestSnapshotSql), "$2");
        return data;
    }

    private static string SqlFor(string name) => name switch
    {
        nameof(DarlingPgServerConfigReader.CurrentConfigSql) => DarlingPgServerConfigReader.CurrentConfigSql,
        nameof(DarlingPgLoggingAuditReader.NewestSnapshotSql) => DarlingPgLoggingAuditReader.NewestSnapshotSql,
        _ => throw new ArgumentOutOfRangeException(nameof(name)),
    };

    [Theory]
    [MemberData(nameof(AnchoredReads))]
    public void EachAnchoredRead_BoundsTheAnchorAndTheRowScan_FromBelow(string name, string bound)
    {
        var sql = Strip(SqlFor(name));
        var escaped = Regex.Escape(bound);

        Assert.Equal(2, Regex.Matches(sql, @"collection_time >= " + escaped + @"(?!\d)").Count);
        Assert.Matches(new Regex(@"\bc\.collection_time >= " + escaped + @"(?!\d)"), sql);
        Assert.Matches(
            new Regex(@"SELECT MAX\(collection_time\)\s+FROM pg_server_config\s+WHERE server_id = \$1\s+AND\s+collection_time >= " + escaped + @"(?!\d)"),
            sql);
    }

    /// <summary><c>OverrideSql</c>'s own shape: a one-row anchor CTE bound on $2, LEFT JOINed so the anchor's
    /// <c>collection_time</c> rides on every call — the column <see cref="DarlingPgServerConfigReader.GetOverridesAsync"/>
    /// reads to decide whether to retry, which must never be "did any override row come back".</summary>
    [Fact]
    public void OverrideSql_BoundsTheAnchorCte_AndLeftJoinsSoTheAnchorRidesWithZeroOverrides()
    {
        var sql = Strip(DarlingPgServerConfigReader.OverrideSql);

        Assert.Contains("WITH anchor AS", sql, StringComparison.Ordinal);
        Assert.Matches(new Regex(@"SELECT MAX\(collection_time\) AS collection_time\s+FROM pg_server_config\s+WHERE server_id = \$1\s+AND\s+collection_time >= \$2"), sql);
        Assert.Contains("LEFT JOIN pg_server_config AS c", sql, StringComparison.Ordinal);
        Assert.Contains("a.collection_time AS anchor_time", sql, StringComparison.Ordinal);
        Assert.Contains("c.collection_time = a.collection_time", sql, StringComparison.Ordinal);
        Assert.Contains("c.collection_time >= $2", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every call site runs the day first and the fallback second. For the two anchored reads the retry
    /// signal is "no rows"; for the override read it must be the anchor column specifically, never the
    /// presence of an override row, or the fallback would run on nearly every call.
    /// </summary>
    [Fact]
    public void EveryCallSite_RunsTheDayFirst_AndFallsBackOnlyWhenNoSnapshotWasFound()
    {
        var configReader = CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "DarlingPgServerConfigReader.cs"));
        var loggingReader = CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "DarlingPgLoggingAuditReader.cs"));

        /* CurrentConfigSql: retry while rows.Count == 0 (an always-nonzero population once include_defaults
           is honoured, so "no rows" only ever means "no snapshot in the bound"). */
        Assert.Contains("foreach (var lowerBound in ConfigSnapshotLowerBounds(DateTime.UtcNow))", configReader, StringComparison.Ordinal);
        Assert.Contains("if (rows.Count > 0) break;", configReader, StringComparison.Ordinal);

        /* OverrideSql: retry keyed on the anchor flag, never on rows.Count — the whole point of the LEFT JOIN. */
        Assert.Contains("var anchorFound = false;", configReader, StringComparison.Ordinal);
        Assert.Contains("if (anchorFound) break;", configReader, StringComparison.Ordinal);

        /* NewestSnapshotSql: the logging reader shares the config reader's bounds helper rather than its own. */
        Assert.Contains("foreach (var lowerBound in DarlingPgServerConfigReader.ConfigSnapshotLowerBounds(DateTime.UtcNow))", loggingReader, StringComparison.Ordinal);
        Assert.Contains("if (rows.Count > 0) break;", loggingReader, StringComparison.Ordinal);
    }

    /// <summary>The day before now, then no bound at all — the same two-rung ladder #3928 uses, just counted
    /// back from the call's own instant rather than an analysis window's end (these reads have no window).</summary>
    [Fact]
    public void TheBounds_AreTheDayBeforeNow_ThenNone()
    {
        var now = new DateTime(2026, 9, 23, 12, 0, 0, DateTimeKind.Utc);
        var bounds = DarlingPgServerConfigReader.ConfigSnapshotLowerBounds(now);

        Assert.Equal(2, bounds.Length);
        Assert.Equal(new DateTime(2026, 9, 22, 12, 0, 0), bounds[0]);
        Assert.Equal(DateTimeKind.Unspecified, bounds[0].Kind);
        Assert.Equal(DateTime.MinValue, bounds[1]);
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) proof for <see cref="PgServerConfigToolBoundTests"/>: the planner prunes to the
/// day, the answers do not move for a dark target, and <c>OverrideSql</c> does not fall back to the unbounded
/// scan merely because a snapshot has zero overrides. Sentinel server ids, cleaned up through
/// <see cref="LiveStoreCleanup"/>.
/// </summary>
[Collection("live-postgres")]
public sealed class PgServerConfigToolBoundLiveTests
{
    private const int PlanShapeServerId = -397_401;
    private const int DarkServerId = -397_402;
    private const int NoOverridesServerId = -397_403;
    private const string ServerName = "pg-server-config-tool-bound-e2e";

    private static readonly int[] s_serverIds = { PlanShapeServerId, DarkServerId, NoOverridesServerId };

    /// <summary>
    /// Seeds one target's hourly snapshot across ten days (eleven one-day chunks) and EXPLAINs the three
    /// shipped reads with each bound they run with: the day plans at most a couple of chunks, the fallback
    /// plans every one of them, and the answer (the newest snapshot) is identical either way.
    /// </summary>
    [Fact]
    public async Task TheDayPlansOnlyItsOwnChunks_AndTheNewestSnapshotStillWins_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the tool-read config plan-shape test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await LiveTimescaleProbe.TryEnableAsync(connectionString!, ct);
        if (timescaleEnabled)
        {
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        }

        var bodySucceeded = false;
        try
        {
            await DeleteSentinelsAsync(connection, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, PlanShapeServerId, ServerName, ct);

            var end = TruncateToSeconds(DateTime.UtcNow);
            using (var seed = new NpgsqlCommand(@"
INSERT INTO pg_server_config (collection_id, collection_time, server_id, server_name, name, setting, unit, source)
SELECT 9_397_401_000 + (EXTRACT(EPOCH FROM t)::bigint % 1_000_000) * 10 + s.k, t, $1, $2, s.name,
       CASE WHEN s.name = 'shared_buffers' AND t = $3 THEN '32768' ELSE s.setting END, s.unit, 'configuration file'
FROM generate_series($3::timestamp - interval '10 days', $3::timestamp, interval '1 hour') AS t
CROSS JOIN (VALUES (1, 'shared_buffers', '16384', '8kB'), (2, 'log_min_duration_statement', '-1', 'ms')) AS s(k, name, setting, unit)", connection))
            {
                seed.Parameters.AddWithValue(PlanShapeServerId);
                seed.Parameters.AddWithValue(ServerName);
                seed.Parameters.AddWithValue(end);
                await seed.ExecuteNonQueryAsync(ct);
            }

            using (var analyze = new NpgsqlCommand("ANALYZE collect.pg_server_config", connection))
            {
                await analyze.ExecuteNonQueryAsync(ct);
            }

            var bounds = DarlingPgServerConfigReader.ConfigSnapshotLowerBounds(end);

            /* CurrentConfigSql: $1 server_id, $2 include_defaults, $3 limit, $4 lower bound. */
            var day = await ExplainAsync(connection, DarlingPgServerConfigReader.CurrentConfigSql, [PlanShapeServerId, true, 50, bounds[0]], ct);
            var fallback = await ExplainAsync(connection, DarlingPgServerConfigReader.CurrentConfigSql, [PlanShapeServerId, true, 50, bounds[1]], ct);

            if (timescaleEnabled)
            {
                var dayChunks = PlanChunkScans.Count(day);
                var fallbackChunks = PlanChunkScans.Count(fallback);
                Assert.True(fallbackChunks >= 11,
                    $"the fallback should plan every seeded chunk (eleven days of snapshots), planned {fallbackChunks}:\n{fallback}");
                Assert.True(dayChunks is >= 1 and <= 4,
                    $"the day's read planned {dayChunks} chunk scans; the day spans at most two chunks, each scanned once by the anchor and once by the row scan:\n{day}");
            }

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var page = await DarlingPgServerConfigReader.GetCurrentConfigPageAsync(postgres, PlanShapeServerId, 50, includeDefaults: true, ct);
            var sharedBuffers = Assert.Single(page.Rows, r => r.Name == "shared_buffers");
            Assert.Equal("32768", sharedBuffers.Setting);

            var logging = await DarlingPgLoggingAuditReader.GetNewestSnapshotAsync(postgres, PlanShapeServerId, ct);
            Assert.Contains(logging, r => r.Name == "shared_buffers" && r.Setting == "32768");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSentinelsAsync);
        }
    }

    /// <summary>
    /// A target whose config collector has been dark for three days still answers with its newest snapshot,
    /// however old, on all three reads — the fallback rung's whole reason to exist.
    /// </summary>
    [Fact]
    public async Task ADarkTarget_StillAnswersWithItsNewestSnapshot_OnAllThreeReads_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the tool-read dark-target test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DeleteSentinelsAsync(connection, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, DarkServerId, ServerName, ct);
            await using var postgres = NpgsqlDataSource.Create(connectionString!);

            var now = TruncateToSeconds(DateTime.UtcNow);
            var staleAt = now.AddHours(-72);
            await SeedSnapshotAsync(connection, DarkServerId, staleAt, ct);

            var page = await DarlingPgServerConfigReader.GetCurrentConfigPageAsync(postgres, DarkServerId, 50, includeDefaults: true, ct);
            Assert.Contains(page.Rows, r => r.Name == "shared_buffers");
            Assert.Equal(DarlingMcpTestData.Naive(staleAt), page.Rows[0].CollectionTimeUtc);

            var overrides = await DarlingPgServerConfigReader.GetOverridesAsync(postgres, DarkServerId, ct);
            Assert.Empty(overrides);

            var logging = await DarlingPgLoggingAuditReader.GetNewestSnapshotAsync(postgres, DarkServerId, ct);
            Assert.Contains(logging, r => r.Name == "shared_buffers");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSentinelsAsync);
        }
    }

    /// <summary>
    /// The case #3974's design question was about: a snapshot inside the day bound with ZERO overrides must
    /// return an empty list WITHOUT running the unbounded fallback — proven here by planting an OLDER
    /// snapshot that DOES carry an override outside the day bound, which must NOT appear. If the loop's retry
    /// signal were "no override rows" instead of "no anchor", this older override would leak into the answer.
    /// </summary>
    [Fact]
    public async Task ASnapshotWithZeroOverrides_DoesNotFallBackToTheOlderSnapshotsOverrides_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the #3974 override no-fallback test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DeleteSentinelsAsync(connection, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, NoOverridesServerId, ServerName, ct);
            await using var postgres = NpgsqlDataSource.Create(connectionString!);

            var now = TruncateToSeconds(DateTime.UtcNow);

            /* An override three days back (outside the day bound) ... */
            await SeedSnapshotAsync(connection, NoOverridesServerId, now.AddDays(-3), ct);
            await SeedOverrideAsync(connection, NoOverridesServerId, now.AddDays(-3), "tenant_db", "work_mem", "262144", ct);

            /* ... and the NEWEST snapshot, inside the day bound, with NO overrides at all. */
            await SeedSnapshotAsync(connection, NoOverridesServerId, now.AddMinutes(-5), ct);

            var overrides = await DarlingPgServerConfigReader.GetOverridesAsync(postgres, NoOverridesServerId, ct);
            Assert.Empty(overrides);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSentinelsAsync);
        }
    }

    private static async Task SeedSnapshotAsync(NpgsqlConnection connection, int serverId, DateTime at, CancellationToken ct)
    {
        foreach (var (name, setting, unit) in new[] { ("shared_buffers", "16384", "8kB"), ("log_min_duration_statement", "-1", "ms") })
        {
            using var command = new NpgsqlCommand(@"
INSERT INTO pg_server_config (collection_id, collection_time, server_id, server_name, name, setting, unit, source)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'configuration file')", connection);
            command.Parameters.AddWithValue(CollectionIdGenerator.Next());
            command.Parameters.AddWithValue(DarlingMcpTestData.Naive(at));
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(ServerName);
            command.Parameters.AddWithValue(name);
            command.Parameters.AddWithValue(setting);
            command.Parameters.Add(new NpgsqlParameter { Value = (object?)unit ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task SeedOverrideAsync(
        NpgsqlConnection connection, int serverId, DateTime at, string databaseName, string name, string setting, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_server_config (collection_id, collection_time, server_id, server_name, name, setting, source, database_name)
VALUES ($1, $2, $3, $4, $5, $6, 'database', $7)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DarlingMcpTestData.Naive(at));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(setting);
        command.Parameters.AddWithValue(databaseName);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string> ExplainAsync(NpgsqlConnection connection, string sql, object[] parameters, CancellationToken ct)
    {
        using var explain = new NpgsqlCommand("EXPLAIN (COSTS OFF) " + sql, connection);
        foreach (var parameter in parameters)
        {
            explain.Parameters.AddWithValue(parameter is DateTime dt ? DateTime.SpecifyKind(dt, DateTimeKind.Unspecified) : parameter);
        }

        var plan = new StringBuilder();
        using var reader = await explain.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            plan.AppendLine(reader.GetString(0));
        }

        return plan.ToString();
    }

    private static async Task DeleteSentinelsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var ids = string.Join(", ", Array.ConvertAll(s_serverIds, id => id.ToString(CultureInfo.InvariantCulture)));
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_server_config WHERE server_id IN ({ids}); " +
            $"DELETE FROM servers WHERE server_id IN ({ids});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);
}
