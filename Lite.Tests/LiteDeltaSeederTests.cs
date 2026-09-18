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
/// two apps hold their own copy of these queries by deliberate design, so each side pins its
/// own; a change to one that is not mirrored fails the other's suite — and
/// <see cref="DeltaFamilySeedingCensusTests"/> reads both sources and asserts the shared ones are
/// byte-identical.</para>
///
/// <para>#3540 A4 widened the seed from four families to every delta family Lite monitors, plus the
/// per-group pass window the #2235 series-age rescue reads. The end-to-end tests below run the real
/// <c>SeedFromDatabaseAsync</c> against this class's DuckDB and prove, per family, that the first
/// <c>CalculateDelta*</c> after "restart" is a real delta rather than the (0, 0) first-sighting marker,
/// and that the rescue fires on that first pass.</para>
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
        /* #3540 A4: the two families that mirror wait_stats (every key every pass) take its exact shape. */
        { DeltaCalculator.LatchStatsSeedSql, "latch_stats" },
        { DeltaCalculator.SpinlockStatsSeedSql, "spinlock_stats" },
    };

    /// <summary>
    /// The latest-row-per-key shape (#3540 A4), for the family whose collector does not write every key
    /// every pass: one table read, ONE bound on it, DISTINCT ON over the window ordered newest-first. No
    /// MAX(collection_time) probe — that returns the latest COLLECTION, which for a churning TOP (150) is
    /// missing every plan that fell out on the last pass. The twin pin is DarlingDeltaSeederTests'
    /// PerKeySeedSql theory, over three families there.
    /// </summary>
    [Fact]
    public void ProcedureStatsSeedSql_BuildsTheCollectorsKey_AndBoundsItsOnlyTableReadOnce()
    {
        var sql = DeltaCalculator.ProcedureStatsSeedSql;
        Assert.Equal(1, CountOccurrences(sql, "collection_time >= $1"));
        Assert.Equal(1, CountOccurrences(sql, "FROM procedure_stats"));
        Assert.Contains("SELECT DISTINCT ON (server_id, delta_key)", sql, StringComparison.Ordinal);
        Assert.Contains(
            "COALESCE(plan_handle, COALESCE(database_name, '') || '.' || COALESCE(schema_name, '') || '.' || COALESCE(object_name, '')) AS delta_key",
            sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY server_id, delta_key, collection_time DESC", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("MAX(collection_time)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("$2", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// query_stats seeds its PASS WINDOW only: the store persists neither statement offset the delta key
    /// carries, so no row can reproduce the key. The read is the distinct collection times per server
    /// inside the cutoff and nothing else.
    /// </summary>
    [Fact]
    public void QueryStatsPassSeedSql_IsThePassWindowOnly()
    {
        var sql = DeltaCalculator.QueryStatsPassSeedSql;
        Assert.Contains("SELECT server_id, collection_time", sql, StringComparison.Ordinal);
        Assert.Contains("FROM query_stats", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY server_id, collection_time", sql, StringComparison.Ordinal);
        Assert.Equal(1, CountOccurrences(sql, "collection_time >= $1"));
        Assert.DoesNotContain("sql_handle", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("plan_handle", sql, StringComparison.Ordinal);
    }

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

    /* ---------------- #3540 A4: every family, end to end against DuckDB ---------------- */

    /// <summary>
    /// latch_stats and spinlock_stats, the wait_stats shape: the LATEST pass inside the window seeds
    /// every counter group, an older pass does not, and the first post-restart call is a real delta over
    /// a real interval. Expected values worked by hand from the rows and reproduced on DuckDB 1.5.5 and
    /// PG18 before this was written.
    /// </summary>
    [Fact]
    public async Task Seed_LatchAndSpinlock_LatestPassBecomesTheBaseline()
    {
        var older = DateTime.UtcNow.AddMinutes(-4);
        var latest = DateTime.UtcNow.AddMinutes(-2);
        await InsertLatchStatsAsync(RecentServerId, older, requests: 100, waitMs: 1000, maxWaitMs: 50);
        await InsertLatchStatsAsync(RecentServerId, latest, requests: 140, waitMs: 1500, maxWaitMs: 60);
        await InsertSpinlockStatsAsync(RecentServerId, older, collisions: 10, spins: 100, sleepTime: 5, backoffs: 2);
        await InsertSpinlockStatsAsync(RecentServerId, latest, collisions: 20, spins: 200, sleepTime: 8, backoffs: 3);

        var deltas = new DeltaCalculator(NullLogger.Instance);
        await deltas.SeedFromDatabaseAsync(_duckDb);

        var now = DateTime.UtcNow;
        const int Gap = CollectorDeltaCalculator.DefaultMaxGapSeconds;

        /* 1500 is the baseline (the LATEST row): 1600 => 100. Had the older row seeded it would be 600;
           unseeded, this first sighting is 0. The interval is the two minutes since that row. */
        Assert.Equal(100, deltas.CalculateDeltaWithInterval(RecentServerId, "latch_stats_wait_time", "BUFFER", 1600, out var interval, now, Gap));
        Assert.InRange(interval, 118, 122);
        Assert.Equal(10, deltas.CalculateDelta(RecentServerId, "latch_stats_waiting_requests", "BUFFER", 150, now, Gap));
        /* max_wait_time_ms is a high-water mark: unchanged is genuinely idle, (0, real interval). */
        Assert.Equal(0, deltas.CalculateDeltaWithInterval(RecentServerId, "latch_stats_max_wait", "BUFFER", 60, out var idle, now, Gap));
        Assert.InRange(idle, 118, 122);

        Assert.Equal(5, deltas.CalculateDelta(RecentServerId, "spinlock_stats_collisions", "LOCK_HASH", 25, now, Gap));
        Assert.Equal(60, deltas.CalculateDelta(RecentServerId, "spinlock_stats_spins", "LOCK_HASH", 260, now, Gap));
        Assert.Equal(1, deltas.CalculateDelta(RecentServerId, "spinlock_stats_sleep_time", "LOCK_HASH", 9, now, Gap));
        Assert.Equal(4, deltas.CalculateDelta(RecentServerId, "spinlock_stats_backoffs", "LOCK_HASH", 7, now, Gap));
    }

    /// <summary>
    /// procedure_stats, the latest-row-PER-KEY shape. Three keys: one in both passes (seeded from the
    /// latest), one only in the OLDER pass — it fell out of the TOP (150) — which the latest-collection
    /// shape would have missed and this seeds from its older row, and one with a null plan_handle whose
    /// key is the collector's <c>db.schema.object</c> fallback. A stale row outside the window for the
    /// second key proves the bound still discriminates inside this shape.
    /// </summary>
    [Fact]
    public async Task Seed_ProcedureStats_LatestRowPerKey_IncludingAKeyAbsentFromTheLatestPass()
    {
        var stale = DateTime.UtcNow - CollectorDeltaCalculator.SeedLookback - TimeSpan.FromMinutes(5);
        var older = DateTime.UtcNow.AddMinutes(-4);
        var latest = DateTime.UtcNow.AddMinutes(-2);
        await InsertProcedureStatsAsync(RecentServerId, older, "0x01", "db", "dbo", "p1", executions: 10);
        await InsertProcedureStatsAsync(RecentServerId, latest, "0x01", "db", "dbo", "p1", executions: 15);
        await InsertProcedureStatsAsync(RecentServerId, stale, "0x02", "db", "dbo", "p2", executions: 1);
        await InsertProcedureStatsAsync(RecentServerId, older, "0x02", "db", "dbo", "p2", executions: 7);
        await InsertProcedureStatsAsync(RecentServerId, latest, null, "db", "dbo", "proc3", executions: 3);

        var deltas = new DeltaCalculator(NullLogger.Instance);
        await deltas.SeedFromDatabaseAsync(_duckDb);

        var now = DateTime.UtcNow;
        const int Gap = CollectorDeltaCalculator.DefaultMaxGapSeconds;

        /* 0x01: the latest row (15) is the baseline => 18 - 15 = 3; every counter group, by its multiple. */
        Assert.Equal(3, deltas.CalculateDelta(RecentServerId, "proc_stats_exec", "0x01", 18, now, Gap));
        Assert.Equal(30, deltas.CalculateDelta(RecentServerId, "proc_stats_worker", "0x01", 180, now, Gap));
        Assert.Equal(60, deltas.CalculateDelta(RecentServerId, "proc_stats_elapsed", "0x01", 360, now, Gap));
        Assert.Equal(90, deltas.CalculateDelta(RecentServerId, "proc_stats_reads", "0x01", 540, now, Gap));
        Assert.Equal(120, deltas.CalculateDelta(RecentServerId, "proc_stats_writes", "0x01", 720, now, Gap));
        Assert.Equal(150, deltas.CalculateDelta(RecentServerId, "proc_stats_phys_reads", "0x01", 900, now, Gap));
        Assert.Equal(180, deltas.CalculateDelta(RecentServerId, "proc_stats_spills", "0x01", 1080, now, Gap));

        /* 0x02: absent from the latest pass, seeded from the OLDER row (7) over its four-minute span — and
           NOT from the stale row (1), which would make this 8. */
        Assert.Equal(2, deltas.CalculateDeltaWithInterval(RecentServerId, "proc_stats_exec", "0x02", 9, out var span, now, Gap));
        Assert.InRange(span, 238, 242);

        /* The null-handle fallback key, spelled exactly as ProcedureStatsCollector spells it. */
        Assert.Equal(2, deltas.CalculateDelta(RecentServerId, "proc_stats_exec", "db.dbo.proc3", 5, now, Gap));
    }

    /// <summary>
    /// THE pass-window pin (#3540 A4 / #2235). query_stats has no key seed — the store cannot reproduce
    /// its key — but its pass window is seeded from the table's collection times, so on the FIRST
    /// post-restart pass a plan compiled since the last pre-restart pass is credited in full with a
    /// real interval, while a plan older than that gap baselines honestly. On an unseeded calculator both
    /// are (0, 0): the rescue was inert on exactly the cycle it exists for. The window is also seeded
    /// for the ORIGINAL families (wait_stats here), proven through the same path; and a server whose
    /// only rows predate the window gets no pass window.
    /// </summary>
    [Fact]
    public async Task Seed_PassWindow_ArmsTheSeriesAgeRescueOnTheFirstPostRestartPass()
    {
        var stale = DateTime.UtcNow - CollectorDeltaCalculator.SeedLookback - TimeSpan.FromMinutes(5);
        var older = DateTime.UtcNow.AddMinutes(-4);
        var latest = DateTime.UtcNow.AddMinutes(-2);
        foreach (var t in new[] { stale, older, latest })
        {
            await InsertQueryStatsAsync(RecentServerId, t);
        }
        await InsertQueryStatsAsync(StaleServerId, stale);
        await InsertWaitStatsAsync(RecentServerId, older, waitingTasks: 10, waitTimeMs: 2000, signalWaitTimeMs: 500);
        await InsertWaitStatsAsync(RecentServerId, latest, waitingTasks: 40, waitTimeMs: 5000, signalWaitTimeMs: 800);

        var deltas = new DeltaCalculator(NullLogger.Instance);
        await deltas.SeedFromDatabaseAsync(_duckDb);

        var now = DateTime.UtcNow;
        const int Gap = CollectorDeltaCalculator.DefaultMaxGapSeconds;

        /* A plan compiled 30 s ago, inside the ~120 s since the last pre-restart pass: credited in full. */
        Assert.Equal(900, deltas.CalculateDeltaWithSeriesAge(RecentServerId, "query_stats_worker", "sh:0:99:newplan", 900, 30, out var interval, now, Gap));
        Assert.InRange(interval, 118, 122);

        /* Every query_stats group is armed, not just the one the first assertion happened to use. */
        foreach (var group in new[] { "query_stats_exec", "query_stats_elapsed", "query_stats_reads", "query_stats_writes", "query_stats_phys_reads", "query_stats_rows", "query_stats_spills" })
        {
            Assert.Equal(900, deltas.CalculateDeltaWithSeriesAge(RecentServerId, group, "sh:0:99:newplan", 900, 30, out _, now, Gap));
        }

        /* A plan older than the gap since the last pass: baselined, (0, 0) — the window does not loosen the rule. */
        Assert.Equal(0, deltas.CalculateDeltaWithSeriesAge(RecentServerId, "query_stats_worker", "sh:0:99:oldplan", 900, 3_000, out var oldInterval, now, Gap));
        Assert.Equal(0, oldInterval);

        /* An original family's window is seeded too. */
        Assert.Equal(77, deltas.CalculateDeltaWithSeriesAge(RecentServerId, "wait_stats_tasks", "NEW_WAIT", 77, 10, out _, now, Gap));

        /* No window for the server whose only pass predates the cutoff. */
        Assert.Equal(0, deltas.CalculateDeltaWithSeriesAge(StaleServerId, "query_stats_worker", "k", 900, 30, out _, now, Gap));

        /* The control, on the same rows: an unseeded calculator is the pre-#3540 first pass. */
        var cold = new DeltaCalculator(NullLogger.Instance);
        Assert.Equal(0, cold.CalculateDeltaWithSeriesAge(RecentServerId, "query_stats_worker", "sh:0:99:newplan", 900, 30, out var coldInterval, now, Gap));
        Assert.Equal(0, coldInterval);
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

    private async Task InsertLatchStatsAsync(int serverId, DateTime collectionTimeUtc, long requests, long waitMs, long maxWaitMs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO latch_stats
    (collection_id, collection_time, server_id, server_name, latch_class, waiting_requests_count, wait_time_ms, max_wait_time_ms)
VALUES ($1, $2, $3, $4, 'BUFFER', $5, $6, $7)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = Truncate(collectionTimeUtc) });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "delta-seed-window" });
        cmd.Parameters.Add(new DuckDBParameter { Value = requests });
        cmd.Parameters.Add(new DuckDBParameter { Value = waitMs });
        cmd.Parameters.Add(new DuckDBParameter { Value = maxWaitMs });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task InsertSpinlockStatsAsync(int serverId, DateTime collectionTimeUtc, long collisions, long spins, long sleepTime, long backoffs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO spinlock_stats
    (collection_id, collection_time, server_id, server_name, spinlock_name, collisions, spins, spins_per_collision, sleep_time, backoffs)
VALUES ($1, $2, $3, $4, 'LOCK_HASH', $5, $6, 10.0, $7, $8)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = Truncate(collectionTimeUtc) });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "delta-seed-window" });
        cmd.Parameters.Add(new DuckDBParameter { Value = collisions });
        cmd.Parameters.Add(new DuckDBParameter { Value = spins });
        cmd.Parameters.Add(new DuckDBParameter { Value = sleepTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = backoffs });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>Counters are multiples of the execution count so every group's expected delta is derivable.</summary>
    private async Task InsertProcedureStatsAsync(int serverId, DateTime collectionTimeUtc, string? planHandle, string db, string schema, string obj, long executions)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type,
     execution_count, total_worker_time, total_elapsed_time, total_logical_reads, total_logical_writes, total_physical_reads, total_spills, plan_handle)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'P', $8, $9, $10, $11, $12, $13, $14, $15)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = Truncate(collectionTimeUtc) });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "delta-seed-window" });
        cmd.Parameters.Add(new DuckDBParameter { Value = db });
        cmd.Parameters.Add(new DuckDBParameter { Value = schema });
        cmd.Parameters.Add(new DuckDBParameter { Value = obj });
        cmd.Parameters.Add(new DuckDBParameter { Value = executions });
        cmd.Parameters.Add(new DuckDBParameter { Value = executions * 10 });
        cmd.Parameters.Add(new DuckDBParameter { Value = executions * 20 });
        cmd.Parameters.Add(new DuckDBParameter { Value = executions * 30 });
        cmd.Parameters.Add(new DuckDBParameter { Value = executions * 40 });
        cmd.Parameters.Add(new DuckDBParameter { Value = executions * 50 });
        cmd.Parameters.Add(new DuckDBParameter { Value = executions * 60 });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)planHandle ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task InsertQueryStatsAsync(int serverId, DateTime collectionTimeUtc)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, query_hash, sql_handle, plan_handle, execution_count)
VALUES ($1, $2, $3, $4, 'qh', 'sh', 'ph', 1)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = Truncate(collectionTimeUtc) });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "delta-seed-window" });
        await cmd.ExecuteNonQueryAsync();
    }
}
