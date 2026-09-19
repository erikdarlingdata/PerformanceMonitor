/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3691 (closing #3749's out-of-lane note): a PostgreSQL target's hour-of-week buckets key on ITS clock — the
/// <c>TimeZone</c> setting in its latest <c>pg_server_config</c> snapshot — not on UTC. Q6 (#3749) re-keyed every
/// bucket to the target's local clock but read that clock from <c>server_properties</c>, a table only the SQL Server
/// collectors write, so a PostgreSQL target landed on the resolver's (NULL, NULL) → UTC arm by construction. This
/// lane makes the clock READ a seam (<c>PgBaselineProvider.ReadServerClockAsync</c>, <c>protected virtual</c>) and
/// overrides it once, handing the same resolver the target's zone name; the bind, the cache, the lookup and the
/// local-clock census are inherited unchanged.
///
/// <para>Two layers. (1) Source and reflection pins: the seam's shape, the override's SQL (the config facts'
/// "latest snapshot at or before the window end, session-scoped sources excluded" rule, copied), the SQL Server
/// read byte-for-byte where #3749 left it, and neither engine's clock read naming the other's table. (2) The live
/// proof (<c>DARLING_TEST_PG</c>): two planted twins with identical 32-day <c>pg_database_stats</c> series — 100 tps
/// from 09:00 to 09:59 New York time every weekday, 10 tps otherwise, across the 2026-03-08 spring-forward — one with
/// a <c>TimeZone = America/New_York</c> snapshot and one with no snapshot. Through the real
/// <see cref="PgTargetBaselineProvider"/> the New York twin's bucket for Tuesday 09:30 local is (9, Tue), 270 samples
/// of exactly 100 tps; the UTC twin's bucket for the same instant is (13, Tue) and holds the four EST Tuesdays'
/// 08:xx idle rows beside the one EDT Tuesday's 09:xx load rows — median 10, mean 20 — the smear the ruling names.
/// The same twin then shows the two degradations: a snapshot after the window end is ignored, and a snapshot whose
/// <c>TimeZone</c> is session-scoped yields UTC keying for that compute.</para>
/// </summary>
public sealed class PgTargetClockTests
{
    [Fact]
    public void TheClockRead_IsAProtectedVirtualSeam_OverriddenOnceByThePostgresProvider()
    {
        var seam = typeof(PgBaselineProvider).GetMethod("ReadServerClockAsync", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(seam);
        Assert.True(seam!.IsVirtual && seam.IsFamily, "ReadServerClockAsync must be protected virtual — the PostgreSQL provider overrides it");
        Assert.Equal(typeof(Task<(int? UtcOffsetMinutes, string? TimeZoneId)>), seam.ReturnType);
        Assert.Equal(
            new[] { typeof(NpgsqlConnection), typeof(int), typeof(DateTime), typeof(CancellationToken) },
            Array.ConvertAll(seam.GetParameters(), p => p.ParameterType));

        var overridden = typeof(PgTargetBaselineProvider).GetMethod("ReadServerClockAsync", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.DeclaredOnly);
        Assert.NotNull(overridden);
        Assert.Equal(seam, overridden!.GetBaseDefinition());

        /* The compute calls the SEAM, once, with the analysis time as the window end — so an override that anchors
           on the window sees the window an anchored pass (#2506) asked for, not the wall clock. */
        var baseSource = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgBaselineProvider.cs");
        var baseCode = CSharpSourceWalker.StripCommentsAndStrings(baseSource);
        Assert.Single(Regex.Matches(baseCode, @"await ReadServerClockAsync\("));
        Assert.Contains("await ReadServerClockAsync(connection, serverId, AsNaive(analysisTime), cancellationToken)", baseCode, StringComparison.Ordinal);
        Assert.DoesNotContain("DateTime.UtcNow", CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetBaselineProvider.Clock.cs")), StringComparison.Ordinal);
    }

    /// <summary>
    /// The SQL Server read is where #3749 left it: the same statement (<c>LocalClockBucketKeyTests</c> pins its text
    /// and Lite's twin), still executed by the base body, and never a PostgreSQL table — while the override never
    /// names the SQL Server one. The two engines share how a clock becomes a key; they must not share where it is read.
    /// </summary>
    [Fact]
    public void EachEnginesClockRead_NamesOnlyItsOwnCollectorsTable()
    {
        var baseSource = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgBaselineProvider.cs");
        Assert.Contains("new NpgsqlCommand(ServerClockSql, connection)", baseSource, StringComparison.Ordinal);
        Assert.Contains("FROM server_properties", PgBaselineProvider.ServerClockSql, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_server_config", PgBaselineProvider.ServerClockSql, StringComparison.Ordinal);

        Assert.Contains("new NpgsqlCommand(PgTargetClockSql, connection)",
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetBaselineProvider.Clock.cs"), StringComparison.Ordinal);
        Assert.Contains("FROM pg_server_config", PgTargetBaselineProvider.PgTargetClockSql, StringComparison.Ordinal);
        Assert.DoesNotContain("server_properties", PgTargetBaselineProvider.PgTargetClockSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The override's read is the config facts' rule, copied: the snapshot that applied AT OR BEFORE the window end
    /// (a historical window keys on the clock it had then), the reader's three-value session-scope exclusion spelled
    /// inline, the one setting by name, an unqualified collector table (no <c>v_</c> view), bound bounds (no bare
    /// <c>now()</c>), and one row at most.
    /// </summary>
    [Fact]
    public void TheOverridesRead_IsTheConfigFactsRule_LatestSnapshotAtOrBeforeTheWindowEnd_SessionScopesExcluded()
    {
        var sql = PgTargetBaselineProvider.PgTargetClockSql;
        Assert.Contains("FROM pg_server_config", sql, StringComparison.Ordinal);
        Assert.Contains("c.name = 'TimeZone'", sql, StringComparison.Ordinal);
        Assert.Contains("NOT IN (" + DarlingPgServerConfigReader.SessionScopedSources + ")", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $2", sql, StringComparison.Ordinal);
        Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("now(", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("CURRENT_TIMESTAMP", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotMatch(new Regex(@"\bFROM\s+v_"), sql);
        Assert.DoesNotContain("$3", sql, StringComparison.Ordinal);

        /* The anchor and the exclusion are the collector's, token for token. */
        var config = PgTargetFactCollector.PgTargetConfigSnapshotSql;
        foreach (var shared in new[]
                 {
                     "AND   c.collection_time = (\r\n          SELECT MAX(collection_time)\r\n          FROM pg_server_config\r\n          WHERE server_id = $1\r\n          AND   collection_time <= $2)",
                     "AND   coalesce(c.source, '') NOT IN ('client', 'session', 'override')",
                 })
        {
            var anchor = shared.Replace("\r\n", "\n", StringComparison.Ordinal);
            Assert.Contains(anchor, sql.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);
            Assert.Contains(anchor, config.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The seam count the anomaly tests' "one protected override" pin grew by one, and nothing else of the base
    /// machinery is redeclared: no instance state (the resolved clock lives in the base's cache entry), no second
    /// resolver, no second bind. The override is a read and a tuple, and the base does the rest.
    /// </summary>
    [Fact]
    public void ThePostgresProvider_RedeclaresExactlyTheTwoSeams_AndNoState()
    {
        var declared = typeof(PgTargetBaselineProvider)
            .GetMethods(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.DeclaredOnly);
        var overrides = Array.FindAll(declared, m => m.GetBaseDefinition().DeclaringType == typeof(PgBaselineProvider));
        Assert.Equal(2, overrides.Length);
        Assert.Contains(overrides, m => m.Name == "ResolveBaselineQuery");
        Assert.Contains(overrides, m => m.Name == "ReadServerClockAsync");
        Assert.Empty(typeof(PgTargetBaselineProvider).GetFields(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.DeclaredOnly));

        var clock = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetBaselineProvider.Clock.cs"));
        Assert.DoesNotContain("BaselineLocalClock", clock, StringComparison.Ordinal);
        Assert.DoesNotContain("LocalClockWindow", clock, StringComparison.Ordinal);
        Assert.Contains("CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds", clock, StringComparison.Ordinal);
    }

    /// <summary>The root doc says what the clock is now: the setting is the rule and UTC the fallback, not the reverse.</summary>
    [Fact]
    public void TheRootDoc_StatesTheSettingAsTheRuleAndUtcAsTheFallback()
    {
        var root = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetBaselineProvider.cs");
        Assert.Contains("PostgreSQL targets resolve their clock from the <c>TimeZone</c> setting", root, StringComparison.Ordinal);
        Assert.Contains("UTC only when no snapshot carries a", root, StringComparison.Ordinal);
        Assert.Contains("$4..$6", root, StringComparison.Ordinal);
        Assert.DoesNotContain("has no clock source", root, StringComparison.Ordinal);

        /* And the base no longer describes the row-less arm as the PostgreSQL target's fate. */
        var baseDoc = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgBaselineProvider.cs");
        Assert.DoesNotContain("when the server has no row (a", baseDoc, StringComparison.Ordinal);
        Assert.Contains("overrides <see cref=\"ReadServerClockAsync\"/> to read the target's own <c>TimeZone</c> setting", baseDoc, StringComparison.Ordinal);
    }
}

/// <summary>
/// The live proof for <see cref="PgTargetClockTests"/>, in the <c>live-postgres</c> collection so the shared store is
/// established before it runs and the residue check runs after it (#1862, #1873).
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetClockLiveTests
{
    private const int NewYorkServerId = -3691_20;
    private const int UtcServerId = -3691_21;
    private const string NewYorkServerName = "pg-target-clock-new-york-e2e";
    private const string UtcServerName = "pg-target-clock-utc-e2e";
    private const int Tuesday = (int)DayOfWeek.Tuesday;

    /* The series: 2026-02-07 00:00Z through 2026-03-11 00:00Z, one row a minute, across the 2026-03-08 07:00Z
       spring-forward (−05:00 → −04:00). The analysis instant is Tuesday 2026-03-10 13:30Z = 09:30 EDT; its 30-day
       window opens 2026-02-08 13:30Z, so it holds Tuesdays Feb 10, 17, 24 and Mar 3 on the EST side and Mar 10 on the
       EDT side. */
    private static readonly DateTime SeriesStart = new(2026, 2, 7, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime AnalysisTime = new(2026, 3, 10, 13, 30, 0, DateTimeKind.Unspecified);
    private const int SeriesMinutes = 32 * 24 * 60;

    [Fact]
    public async Task Live_ANewYorkTargetKeysOnNewYorkHours_AUtcTwinOnUtc_AndTheSnapshotRuleHolds()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the PostgreSQL-target clock e2e.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            /* Identical series on both twins: 100 tps (6000 commits a minute) in the 09:00 hour NEW YORK time on
               weekdays, 10 tps otherwise — the local hour computed in SQL with the same one-step function the
               provider binds, so the planted pattern is the pattern a New York server would actually produce. */
            foreach (var (serverId, serverName) in new[] { (NewYorkServerId, NewYorkServerName), (UtcServerId, UtcServerName) })
            {
                await PlantSeriesAsync(connection, serverId, serverName, ct);
            }

            /* The New York twin's clock: an hourly snapshot from the EST side carrying TimeZone from the server's
               configuration file, and a LATER snapshot — after the window end — saying Tokyo, which the read must
               ignore (the clock that applied to the window is the one its buckets key on). The UTC twin has no
               pg_server_config rows at all: the base read's (NULL, NULL), UTC keying. */
            await PlantSnapshotAsync(connection, NewYorkServerId, NewYorkServerName, SeriesStart.AddHours(1), "America/New_York", "configuration file", ct);
            await PlantSnapshotAsync(connection, NewYorkServerId, NewYorkServerName, AnalysisTime.AddHours(1), "Asia/Tokyo", "configuration file", ct);

            var provider = new PgTargetBaselineProvider(postgres);

            /* New York: the bucket for 09:30 local is the 09:00 local hour on a Tuesday — 4 EST Tuesdays × 60 rows
               (14:xxZ) + the EDT Tuesday's 30 rows (13:00–13:29Z, the window end is exclusive) = 270 samples over 5
               local days, every one of them exactly 100 tps. */
            var newYork = await provider.GetBaselineAsync(NewYorkServerId, MetricNames.PgTps, AnalysisTime, ct);
            Assert.Equal(BaselineTier.Full, newYork.Tier);
            Assert.Equal((9, Tuesday), (newYork.HourOfDay, newYork.DayOfWeek));
            Assert.Equal(270L, newYork.SampleCount);
            Assert.Equal(5L, newYork.DistinctDays);
            Assert.Equal(100.0, newYork.Median, 0.001);
            Assert.Equal(100.0, newYork.Mean, 0.001);
            Assert.Equal(0.0, newYork.Mad, 0.001);

            /* UTC: the same instant is 13:30Z, and the 13Z Tuesday bucket pools the four EST Tuesdays' 08:xx local
               rows (idle, 10 tps) with the EDT Tuesday's 09:xx local rows (load, 100 tps) — 240 at 10 and 30 at 100:
               median 10, mean 20. A detector judging 100 tps at 09:30 New York time against THIS bucket fires; against
               the New York twin's it does not. That is the smear, on the PostgreSQL engine, and this is its end. */
            var utc = await provider.GetBaselineAsync(UtcServerId, MetricNames.PgTps, AnalysisTime, ct);
            Assert.Equal(BaselineTier.Full, utc.Tier);
            Assert.Equal((13, Tuesday), (utc.HourOfDay, utc.DayOfWeek));
            Assert.Equal(270L, utc.SampleCount);
            Assert.Equal(10.0, utc.Median, 0.001);
            Assert.Equal(20.0, utc.Mean, 0.001);

            /* The snapshot rule's other half: a newer snapshot INSIDE the window whose TimeZone is session-scoped
               (the collector's own connection set it) describes the connection, not the server — that compute keys
               on UTC rather than on Tokyo, and rather than on the older snapshot's New York. */
            await PlantSnapshotAsync(connection, NewYorkServerId, NewYorkServerName, AnalysisTime.AddHours(-1), "Asia/Tokyo", "session", ct);
            provider.ClearCache();
            var sessionScoped = await provider.GetBaselineAsync(NewYorkServerId, MetricNames.PgTps, AnalysisTime, ct);
            Assert.Equal((13, Tuesday), (sessionScoped.HourOfDay, sessionScoped.DayOfWeek));
            Assert.Equal(10.0, sessionScoped.Median, 0.001);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task PlantSeriesAsync(NpgsqlConnection connection, int serverId, string serverName, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
WITH s AS (
    SELECT n, $2 + (n * interval '1 minute') AS at
    FROM generate_series(0, $5) AS n
),
l AS (
    SELECT n, at,
           at + (CASE WHEN at < TIMESTAMP '2026-03-08 07:00:00' THEN -300 ELSE -240 END) * INTERVAL '1' MINUTE AS local_at
    FROM s
)
INSERT INTO pg_database_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset)
SELECT $1 + n, at, $3, $4, 'appdb',
       SUM(CASE WHEN EXTRACT(HOUR FROM local_at) = 9 AND EXTRACT(DOW FROM local_at) BETWEEN 1 AND 5 THEN 6000 ELSE 600 END) OVER (ORDER BY n),
       0, 100, 9000, 0, 0, 0, NULL
FROM l", connection) { CommandTimeout = 120 };
        command.Parameters.AddWithValue(CollectionIdGenerator.Next() + 1_000_000L);
        command.Parameters.AddWithValue(SeriesStart);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(SeriesMinutes);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantSnapshotAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime at, string timeZone, string source, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO pg_server_config (collection_id, collection_time, server_id, server_name, name, setting, source) VALUES ($1, $2, $3, $4, 'TimeZone', $5, $6)",
            connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(timeZone);
        command.Parameters.AddWithValue(source);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_database_stats WHERE server_id IN ({NewYorkServerId}, {UtcServerId}); " +
            $"DELETE FROM pg_server_config WHERE server_id IN ({NewYorkServerId}, {UtcServerId});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
