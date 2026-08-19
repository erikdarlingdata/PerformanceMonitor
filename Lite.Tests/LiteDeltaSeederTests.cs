/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Pins the restart re-seed's time bound (#1772) on the Lite side, against a real DuckDB.
///
/// <para>The bound is the whole fix, and it is invisible to any test that only checks the values a
/// seed restores: an unbounded seed returns the same baselines, it just reads the entire table to
/// find them. On a 276 GB Postgres field store that meant every chunk of the hypertable on a
/// 30-second command timeout, so the seed threw, the warning went to a log nobody reads, and every
/// service start silently degraded to first-cycle-zero deltas. What has to be pinned is therefore
/// the SHAPE — the cutoff reaching both the outer read and the inner MAX() — and the BEHAVIOUR that
/// only a real engine can show: that a row outside the window is genuinely not read.</para>
///
/// <para><see cref="DarlingDeltaSeederTests"/> pins the same properties on the Postgres side. The
/// two apps hold their own copy of these four queries by deliberate design, so each side pins its
/// own; a change to one that is not mirrored fails the other's suite.</para>
/// </summary>
public sealed class LiteDeltaSeederTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    /// <summary>Distinctive fake id — a real server_id is a storage-name hash, never this.</summary>
    private const int RecentServerId = -616161;

    /// <summary>A second server whose only row predates the lookback window.</summary>
    private const int StaleServerId = -626262;

    private const string TestWaitType = "LITE_SEED_WINDOW_WAIT";

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public LiteDeltaSeederTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    public static TheoryData<string, string> SeedQueries() => new()
    {
        { DeltaCalculator.WaitStatsSeedSql, "wait_stats" },
        { DeltaCalculator.FileIoStatsSeedSql, "file_io_stats" },
        { DeltaCalculator.PerfmonStatsSeedSql, "perfmon_stats" },
        { DeltaCalculator.MemoryGrantStatsSeedSql, "memory_grant_stats" },
    };

    /// <summary>
    /// Both halves carry the bound. Bounding only the outer read still lets the inner MAX() aggregate
    /// the whole table; bounding only the inner one still lets the outer row-value probe scan it. The
    /// pin is deliberately blunt — the cutoff appears exactly twice — so removing EITHER goes red.
    /// </summary>
    [Theory]
    [MemberData(nameof(SeedQueries))]
    public void SeedSql_BoundsTheOuterReadAndTheInnerMax(string sql, string table)
    {
        Assert.Equal(2, CountOccurrences(sql, "collection_time >= $1"));

        /* The inner aggregate, verbatim on one line so the assertion cannot be satisfied by a bound
           that sits anywhere else in the statement. */
        Assert.Contains(
            $"SELECT server_id, MAX(collection_time) FROM {table} WHERE collection_time >= $1 GROUP BY server_id",
            sql, StringComparison.Ordinal);

        /* ...and the other occurrence is the OUTER one: it precedes the row-value probe. */
        Assert.True(
            sql.IndexOf("collection_time >= $1", StringComparison.Ordinal)
            < sql.IndexOf("(server_id, collection_time) IN (", StringComparison.Ordinal),
            "the first cutoff must bound the outer read, before the row-value probe");
    }

    /// <summary>
    /// One placeholder, reused. Both engines resolve a repeated <c>$1</c> to a single bound
    /// parameter, which is what keeps these queries byte-identical across the two apps; a second
    /// placeholder would mean the callers' single Add is short and the read fails outright.
    /// </summary>
    [Theory]
    [MemberData(nameof(SeedQueries))]
    public void SeedSql_ReferencesExactlyOneParameter(string sql, string table)
    {
        Assert.DoesNotContain("$2", sql, StringComparison.Ordinal);
        Assert.Contains($"FROM {table}", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The latest row inside the window is still the baseline — the bound must not cost the seed the
    /// thing it exists for.
    /// </summary>
    [Fact]
    public async Task Seed_LatestRowInsideTheWindow_BecomesTheBaseline()
    {
        var older = DateTime.UtcNow.AddMinutes(-3);
        var latest = DateTime.UtcNow.AddMinutes(-1);
        await InsertWaitStatsAsync(RecentServerId, older, waitingTasks: 10, waitTimeMs: 2000, signalWaitTimeMs: 500);
        await InsertWaitStatsAsync(RecentServerId, latest, waitingTasks: 40, waitTimeMs: 5000, signalWaitTimeMs: 800);

        var deltas = new DeltaCalculator(NullLogger.Instance);
        await deltas.SeedFromDatabaseAsync(_duckDb);

        /* Baseline 5000 (the LATEST row), current 5100 => 100. Unseeded this first sighting returns
           0; had the older row seeded instead it would be 3100. */
        var now = DateTime.UtcNow;
        Assert.Equal(100, deltas.CalculateDelta(RecentServerId, "wait_stats_time", TestWaitType, 5100, now, 300));
        Assert.Equal(5, deltas.CalculateDelta(RecentServerId, "wait_stats_tasks", TestWaitType, 45, now, 300));
    }

    /// <summary>
    /// THE pin for #1772, and the one that needs a real engine: a row older than
    /// <see cref="CollectorDeltaCalculator.SeedLookback"/> is not read at all, so it seeds nothing and
    /// the key's first sighting behaves like a first sighting.
    ///
    /// <para>Restore the unbounded query and this goes red: the stale row seeds a baseline of 1000 and
    /// the delta below comes back 200 instead of 0. The two servers are seeded in ONE pass so the
    /// bound is doing the discriminating, not the absence of data.</para>
    ///
    /// <para><b>The assertions pass maxGapSeconds 0 deliberately</b>, and that is the whole reason this
    /// fix is free rather than a trade. With the production 300 armed, a seeded stale baseline and no
    /// baseline at all are INDISTINGUISHABLE — the gap policy rejects the stale one and returns 0,
    /// which is exactly what a first sighting returns — so an unbounded seed spends a full-table scan
    /// to reach an outcome it already had. Disarming the policy is what makes "was this row read?"
    /// observable at all; it is the question under test, not the production call shape.</para>
    /// </summary>
    [Fact]
    public async Task Seed_RowOlderThanTheLookback_IsNotRead()
    {
        var insideWindow = DateTime.UtcNow.AddMinutes(-1);
        var outsideWindow = DateTime.UtcNow - CollectorDeltaCalculator.SeedLookback - TimeSpan.FromMinutes(5);

        await InsertWaitStatsAsync(RecentServerId, insideWindow, waitingTasks: 40, waitTimeMs: 5000, signalWaitTimeMs: 800);
        await InsertWaitStatsAsync(StaleServerId, outsideWindow, waitingTasks: 7, waitTimeMs: 1000, signalWaitTimeMs: 100);

        var deltas = new DeltaCalculator(NullLogger.Instance);
        await deltas.SeedFromDatabaseAsync(_duckDb);

        var now = DateTime.UtcNow;

        /* Nothing was seeded for the stale server: 1200 is a first sighting, which is 0 even with the
           gap policy off. Seeded, this would be 1200 - 1000 = 200. */
        Assert.Equal(0, deltas.CalculateDelta(StaleServerId, "wait_stats_time", TestWaitType, 1200, now, 0));

        /* ...while the same pass DID seed the server whose row is inside the window. */
        Assert.Equal(100, deltas.CalculateDelta(RecentServerId, "wait_stats_time", TestWaitType, 5100, now, 0));
    }

    /// <summary>
    /// The memory-grant baselines carry their collection_time, so the gap policy can reject a stale
    /// one. Seeded with a null timestamp (as they were before #1772) the policy cannot fire at all,
    /// and the row below — inside the 15-minute read window but well outside the gap passed below —
    /// produces a fabricated spike of 40 instead of 0 on the first cycle after a restart.
    /// </summary>
    [Fact]
    public async Task Seed_MemoryGrantBaselineOutsideTheGapPolicy_YieldsZeroRatherThanASpike()
    {
        await InsertMemoryGrantStatsAsync(RecentServerId, DateTime.UtcNow.AddMinutes(-10), timeouts: 10, forced: 3);

        var deltas = new DeltaCalculator(NullLogger.Instance);
        await deltas.SeedFromDatabaseAsync(_duckDb);

        var now = DateTime.UtcNow;
        Assert.Equal(0, deltas.CalculateDelta(RecentServerId, "memory_grants_timeouts", "0_1", 50, now, 300));
        Assert.Equal(0, deltas.CalculateDelta(RecentServerId, "memory_grants_forced", "0_1", 20, now, 300));
    }

    private static int CountOccurrences(string haystack, string needle)
    {
        var count = 0;
        for (var i = haystack.IndexOf(needle, StringComparison.Ordinal); i >= 0;
             i = haystack.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }
        return count;
    }

    /* Naive-UTC storage by convention across the product; seconds resolution matches the collectors. */
    private static DateTime Truncate(DateTime t) =>
        new(t.Year, t.Month, t.Day, t.Hour, t.Minute, t.Second, DateTimeKind.Unspecified);

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task InsertWaitStatsAsync(
        int serverId, DateTime collectionTimeUtc, long waitingTasks, long waitTimeMs, long signalWaitTimeMs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = Truncate(collectionTimeUtc) });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "delta-seed-window" });
        cmd.Parameters.Add(new DuckDBParameter { Value = TestWaitType });
        cmd.Parameters.Add(new DuckDBParameter { Value = waitingTasks });
        cmd.Parameters.Add(new DuckDBParameter { Value = waitTimeMs });
        cmd.Parameters.Add(new DuckDBParameter { Value = signalWaitTimeMs });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task InsertMemoryGrantStatsAsync(
        int serverId, DateTime collectionTimeUtc, long timeouts, long forced)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO memory_grant_stats
    (collection_id, collection_time, server_id, server_name, resource_semaphore_id, pool_id, timeout_error_count, forced_grant_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = Truncate(collectionTimeUtc) });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "delta-seed-window" });
        cmd.Parameters.Add(new DuckDBParameter { Value = (short)1 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 0 });
        cmd.Parameters.Add(new DuckDBParameter { Value = timeouts });
        cmd.Parameters.Add(new DuckDBParameter { Value = forced });
        await cmd.ExecuteNonQueryAsync();
    }
}
