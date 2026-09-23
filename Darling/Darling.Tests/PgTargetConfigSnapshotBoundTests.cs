/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3928: the PostgreSQL target's five reads of the newest <c>pg_server_config</c> snapshot (the config, posture,
/// blocking-settings and memory families' and the baseline clock's) bound both the snapshot's
/// <c>MAX(collection_time)</c> and the row scan from below by the day before the window's end, and fall back to every
/// retained snapshot only when the day found nothing (<see cref="PgTargetFactCollector.ConfigSnapshotLowerBounds"/>).
/// The table keeps a year of hourly snapshots in one-day chunks. Unbounded, TimescaleDB planned every chunk on each
/// read: 2.5-4.9 s a read at 366 chunks on the CI-sized rig, against about a millisecond bounded.
///
/// <para>The answer is pinned not to move. The fallback keeps "the newest snapshot however old", so a target whose
/// config collector went dark still states its config, and a historical window still reads the snapshot that applied
/// at its own end.</para>
/// </summary>
public sealed class PgTargetConfigSnapshotBoundTests
{
    /// <summary>The five statements: the const, the file that executes it, and the parameter its lower bound takes.</summary>
    private static readonly (string Name, string File, string LowerBound)[] s_reads =
    {
        (nameof(PgTargetFactCollector.PgTargetConfigSnapshotSql), "PgTargetFactCollector.Config.cs", "$3"),
        (nameof(PgTargetFactCollector.PgTargetPostureSql), "PgTargetFactCollector.Posture.cs", "$2"),
        (nameof(PgTargetFactCollector.PgTargetBlockingSettingsSql), "PgTargetFactCollector.Blocking.cs", "$3"),
        (nameof(PgTargetFactCollector.PgTargetMemoryConfigSql), "PgTargetFactCollector.Memory.cs", "$3"),
        (nameof(PgTargetBaselineProvider.PgTargetClockSql), "PgTargetBaselineProvider.Clock.cs", "$3"),
    };

    public static TheoryData<string, string> Statements()
    {
        var data = new TheoryData<string, string>();
        foreach (var (name, _, lowerBound) in s_reads) data.Add(name, lowerBound);
        return data;
    }

    public static TheoryData<string, string> CallSites()
    {
        var data = new TheoryData<string, string>();
        foreach (var (name, file, _) in s_reads) data.Add(name, file);
        return data;
    }

    /// <summary>
    /// Both scans carry the bound. The anchor's <c>MAX</c> alone is not enough: the outer
    /// <c>c.collection_time = (…)</c> can exclude chunks only at run time, so on the rig a read bounded only inside
    /// still planned all 366 chunks.
    /// </summary>
    [Theory]
    [MemberData(nameof(Statements))]
    public void EachRead_BoundsTheAnchorAndTheRowScan_FromBelow(string name, string lowerBound)
    {
        var sql = StripComments(SqlFor(name));
        var bound = Regex.Escape(lowerBound);

        Assert.Equal(2, Regex.Matches(sql, @"collection_time >= " + bound + @"(?!\d)").Count);
        Assert.Matches(new Regex(@"\bc\.collection_time >= " + bound + @"(?!\d)"), sql);
        Assert.Matches(
            new Regex(@"SELECT MAX\(collection_time\)\s+FROM pg_server_config\s+WHERE server_id = \$1\s+AND\s+collection_time >= " + bound + @"(?!\d)"),
            sql);
    }

    /// <summary>
    /// Every call site runs the day first and the fallback second, binding the bound it is on. The loop is the whole
    /// of the semantics: without it the bound would drop a dark target's config instead of only planning less.
    /// </summary>
    [Theory]
    [MemberData(nameof(CallSites))]
    public void EachCallSite_RunsTheDayFirst_AndEveryRetainedSnapshotOnlyWhenThatFoundNothing(string name, string file)
    {
        var code =CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", file));

        Assert.Single(Regex.Matches(code, @"foreach \(var lowerBound in (?:PgTargetFactCollector\.)?ConfigSnapshotLowerBounds\("));
        Assert.Single(Regex.Matches(code, @"new NpgsqlCommand\(" + name + @", connection\)"));
        Assert.Single(Regex.Matches(code, @"cmd\.Parameters\.AddWithValue\(lowerBound\);"));
    }

    /// <summary>
    /// The census that makes the theory above complete: the five files are the only analysis code that reads the
    /// table, and the fact collector's statements over it are exactly the four the theory names. A sixth read joins
    /// the theory, or this reds.
    /// </summary>
    [Fact]
    public void TheFiveReads_AreEveryAnalysisReadOfTheTable()
    {
        var analysisDir = Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Analysis");
        var readers = Directory.GetFiles(analysisDir, "*.cs", SearchOption.AllDirectories)
            .Where(path => !path.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                           && !path.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .Where(path => File.ReadAllText(path).Contains("FROM pg_server_config", StringComparison.Ordinal))
            .Select(Path.GetFileName)
            .OrderBy(f => f, StringComparer.Ordinal)
            .ToArray();

        var expectedFiles = s_reads.Select(read => read.File).OrderBy(f => f, StringComparer.Ordinal).ToArray();
        Assert.Equal(expectedFiles, readers);

        var factReads = typeof(PgTargetFactCollector)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral && f.Name.EndsWith("Sql", StringComparison.Ordinal)
                        && ((string)f.GetRawConstantValue()!).Contains("FROM pg_server_config", StringComparison.Ordinal))
            .Select(f => f.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();
        var expectedReads = s_reads.Select(read => read.Name)
            .Where(n => n != nameof(PgTargetBaselineProvider.PgTargetClockSql))
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();
        Assert.Equal(expectedReads, factReads);
    }

    /// <summary>The day before the window's end, naive for the <c>timestamp</c> column, then no bound at all.</summary>
    [Fact]
    public void TheBounds_AreTheDayBeforeTheWindowsEnd_ThenNone()
    {
        var bounds = PgTargetFactCollector.ConfigSnapshotLowerBounds(new DateTime(2026, 9, 22, 12, 0, 0, DateTimeKind.Utc));

        Assert.Equal(2, bounds.Length);
        Assert.Equal(new DateTime(2026, 9, 21, 12, 0, 0), bounds[0]);
        Assert.Equal(DateTimeKind.Unspecified, bounds[0].Kind);
        Assert.Equal(TimeSpan.FromHours(24), AnalysisContext.LatestValueLookback);
        Assert.Equal(DateTime.MinValue, bounds[1]);
    }

    internal static string SqlFor(string name)
    {
        var field = typeof(PgTargetFactCollector).GetField(name, BindingFlags.Public | BindingFlags.Static)
                    ?? typeof(PgTargetBaselineProvider).GetField(name, BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(field);
        return (string)field!.GetRawConstantValue()!;
    }

    private static string StripComments(string sql) =>
        Regex.Replace(sql, @"/\*.*?\*/", " ", RegexOptions.Singleline);
}

/// <summary>
/// Gated (DARLING_TEST_PG) proof for <see cref="PgTargetConfigSnapshotBoundTests"/>: the planner prunes to the day,
/// and the answers do not move. Serialized through the "live-postgres" collection with sentinel server ids, cleaned
/// up through <see cref="LiveStoreCleanup"/>.
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetConfigSnapshotBoundLiveTests
{
    /// <summary>Sentinel ids: a real server_id is a storage-name hash, never these.</summary>
    private const int PlanShapeServerId = -392_801;
    private const int DarkServerId = -392_802;
    private const int HistoricalServerId = -392_803;
    private const string ServerName = "pg-target-config-bound-e2e";

    private static readonly int[] s_serverIds = { PlanShapeServerId, DarkServerId, HistoricalServerId };

    /// <summary>
    /// Builds the store the way the service does (ladder, then the hypertable conversion where TimescaleDB is
    /// present), seeds one target's hourly snapshot across ten days (eleven one-day chunks), and EXPLAINs the shipped
    /// config read with each bound it runs with: the day plans at most the two chunks a 24-hour span can touch, and
    /// the fallback plans every one of them, as every read did before. The facts come from the newest snapshot.
    /// </summary>
    [Fact]
    public async Task TheDayPlansOnlyItsOwnChunks_AndTheNewestSnapshotStillWins_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the PostgreSQL-target config plan-shape test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* #1922: probe on its own connection. */
        var timescaleEnabled = await LiveTimescaleProbe.TryEnableAsync(connectionString!, ct);
        if (timescaleEnabled)
        {
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        }

        var bodySucceeded = false;
        try
        {
            await DeleteSentinelsAsync(connection, ct);
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, PlanShapeServerId, ServerName, "postgres", 18, ct);

            /* shared_buffers 128 MB (16384 x 8kB) on every snapshot but the newest, which says 256 MB. */
            var end = TruncateToSeconds(DateTime.UtcNow);
            using (var seed = new NpgsqlCommand(@"
INSERT INTO pg_server_config (collection_id, collection_time, server_id, server_name, name, setting, unit, source)
SELECT 9_392_801_000 + (EXTRACT(EPOCH FROM t)::bigint % 1_000_000) * 10 + s.k, t, $1, $2, s.name,
       CASE WHEN s.name = 'shared_buffers' AND t = $3 THEN '32768' ELSE s.setting END, s.unit, 'configuration file'
FROM generate_series($3::timestamp - interval '10 days', $3::timestamp, interval '1 hour') AS t
CROSS JOIN (VALUES (1, 'shared_buffers', '16384', '8kB'), (2, 'fsync', 'on', NULL), (3, 'TimeZone', 'UTC', NULL))
    AS s(k, name, setting, unit)", connection))
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

            var bounds = PgTargetFactCollector.ConfigSnapshotLowerBounds(end);
            var day = await ExplainAsync(connection, PgTargetFactCollector.PgTargetConfigSnapshotSql, [PlanShapeServerId, end, bounds[0]], ct);
            var fallback = await ExplainAsync(connection, PgTargetFactCollector.PgTargetConfigSnapshotSql, [PlanShapeServerId, end, bounds[1]], ct);

            if (timescaleEnabled)
            {
                var dayChunks = ChunkScans(day);
                var fallbackChunks = ChunkScans(fallback);
                Assert.True(fallbackChunks >= 11,
                    $"the fallback should plan every seeded chunk (eleven days of snapshots), planned {fallbackChunks}:\n{fallback}");
                Assert.True(dayChunks is >= 1 and <= 4,
                    $"the day's read planned {dayChunks} chunk scans; the day spans at most two chunks, each scanned once by the anchor and once by the row scan:\n{day}");
            }

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var facts = await new PgTargetFactCollector(postgres).CollectFactsAsync(Context(PlanShapeServerId, end));
            var sharedBuffers = Assert.Single(facts, f => f.Key == PgTargetFactKeys.ConfigSharedBuffers);
            Assert.Equal(256.0, sharedBuffers.Value, precision: 6);
            Assert.Equal(0.0, sharedBuffers.Metadata["snapshot_age_s"], precision: 6);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSentinelsAsync);
        }
    }

    /// <summary>
    /// The answers the bound must not move. A target whose config collector went dark three days before the window's
    /// end still states its newest snapshot, in the config facts, the posture facts and the baseline clock, with the
    /// snapshot's age on the fact. A historical window reads the snapshot at or before its own end for the config
    /// facts and the clock, while posture still reads the newest snapshot the target has.
    /// </summary>
    [Fact]
    public async Task ADarkTargetAndAHistoricalWindow_ReadTheSameSnapshotsAsTheUnboundedReads_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the PostgreSQL-target config fallback test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DeleteSentinelsAsync(connection, ct);
            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var now = TruncateToSeconds(DateTime.UtcNow);

            /* Dark: the newest snapshot is 72 hours before the window's end; an older one disagrees with it. */
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, DarkServerId, ServerName, "postgres", 18, ct);
            await PlantSnapshotAsync(connection, DarkServerId, now.AddHours(-96), sharedBuffers8k: "16384", fsync: "on", timeZone: "Asia/Tokyo", ct);
            await PlantSnapshotAsync(connection, DarkServerId, now.AddHours(-72), sharedBuffers8k: "32768", fsync: "off", timeZone: "America/New_York", ct);

            var dark = await new PgTargetFactCollector(postgres).CollectFactsAsync(Context(DarkServerId, now));
            var darkBuffers = Assert.Single(dark, f => f.Key == PgTargetFactKeys.ConfigSharedBuffers);
            Assert.Equal(256.0, darkBuffers.Value, precision: 6);
            Assert.Equal(72 * 3600.0, darkBuffers.Metadata["snapshot_age_s"], precision: 6);
            var darkFsync = Assert.Single(dark, f => f.Key == PgTargetFactKeys.PostureFsync);
            Assert.Equal(1.0, darkFsync.Value);
            Assert.Equal(72 * 60.0, darkFsync.Metadata["snapshot_age_minutes"], precision: 6);
            Assert.Equal("America/New_York", await ClockZoneAsync(postgres, DarkServerId, now, ct));

            /* Historical: the window ended five days ago, the snapshot that applied then is 30 hours older than its
               end, and a newer one exists today. */
            var historicalEnd = now.AddDays(-5);
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, HistoricalServerId, ServerName, "postgres", 18, ct);
            await PlantSnapshotAsync(connection, HistoricalServerId, historicalEnd.AddHours(-30), sharedBuffers8k: "16384", fsync: "on", timeZone: "America/New_York", ct);
            await PlantSnapshotAsync(connection, HistoricalServerId, now.AddHours(-1), sharedBuffers8k: "32768", fsync: "off", timeZone: "Asia/Tokyo", ct);

            var historical = await new PgTargetFactCollector(postgres).CollectFactsAsync(Context(HistoricalServerId, historicalEnd));
            var historicalBuffers = Assert.Single(historical, f => f.Key == PgTargetFactKeys.ConfigSharedBuffers);
            Assert.Equal(128.0, historicalBuffers.Value, precision: 6);
            Assert.Equal(30 * 3600.0, historicalBuffers.Metadata["snapshot_age_s"], precision: 6);
            Assert.Equal(1.0, Assert.Single(historical, f => f.Key == PgTargetFactKeys.PostureFsync).Value);
            Assert.Equal("America/New_York", await ClockZoneAsync(postgres, HistoricalServerId, historicalEnd, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSentinelsAsync);
        }
    }

    private static AnalysisContext Context(int serverId, DateTime end) => new()
    {
        ServerId = serverId,
        ServerName = ServerName,
        TimeRangeStart = end.AddHours(-4),
        TimeRangeEnd = end,
        ServerUtcOffset = TimeSpan.Zero,
    };

    /// <summary>The clock seam is protected; the provider's own compute reaches it only through a baseline, and this
    /// pins the read, not the bucket.</summary>
    private static async Task<string?> ClockZoneAsync(NpgsqlDataSource postgres, int serverId, DateTime windowEnd, CancellationToken ct)
    {
        var seam = typeof(PgTargetBaselineProvider).GetMethod(
            "ReadServerClockAsync", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.DeclaredOnly);
        Assert.NotNull(seam);

        await using var connection = await postgres.OpenConnectionAsync(ct);
        var read = (Task<(int? UtcOffsetMinutes, string? TimeZoneId)>)seam!.Invoke(
            new PgTargetBaselineProvider(postgres), [connection, serverId, windowEnd, ct])!;
        return (await read).TimeZoneId;
    }

    private static async Task PlantSnapshotAsync(NpgsqlConnection connection, int serverId, DateTime at,
        string sharedBuffers8k, string fsync, string timeZone, CancellationToken ct)
    {
        var collectionId = CollectionIdGenerator.Next();
        foreach (var (name, setting, unit) in new[] { ("shared_buffers", sharedBuffers8k, "8kB"), ("fsync", fsync, (string?)null), ("TimeZone", timeZone, null) })
        {
            using var command = new NpgsqlCommand(@"
INSERT INTO pg_server_config (collection_id, collection_time, server_id, server_name, name, setting, unit, source)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'configuration file')", connection);
            command.Parameters.AddWithValue(collectionId);
            command.Parameters.AddWithValue(at);
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(ServerName);
            command.Parameters.AddWithValue(name);
            command.Parameters.AddWithValue(setting);
            command.Parameters.Add(new NpgsqlParameter { Value = (object?)unit ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            await command.ExecuteNonQueryAsync(ct);
        }
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

    /// <summary>Scan nodes over a hypertable chunk, compressed or not.</summary>
    private static int ChunkScans(string plan) =>
        plan.Split('\n').Count(l => Regex.IsMatch(l, @"(?:Scan|ColumnarScan)[^\n]* on _hyper_\d+_\d+_chunk"));

    private static async Task DeleteSentinelsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var ids = string.Join(", ", s_serverIds.Select(id => id.ToString(CultureInfo.InvariantCulture)));
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_server_config WHERE server_id IN ({ids}); " +
            $"DELETE FROM analysis_findings WHERE server_id IN ({ids}); " +
            $"DELETE FROM servers WHERE server_id IN ({ids});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);
}
