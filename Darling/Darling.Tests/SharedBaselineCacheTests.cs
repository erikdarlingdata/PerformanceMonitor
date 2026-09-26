/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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
/// #3941: the process's shared baseline tier. Every scheduled pass built a fresh <see cref="DarlingAnalysisService"/>,
/// whose providers' bucket cache died with the pass, so every pass recomputed every 30-day baseline and the MCP and web
/// hosts paid for them again. <see cref="BaselineCache"/> is the second tier the three hosts share; these pin the rule
/// that decides when one of its entries answers (hour, engine, TTL, success only) and the wiring that hands the ONE
/// instance to every host.
/// </summary>
public sealed class SharedBaselineCacheTests
{
    private static readonly DateTime Hour = new(2026, 9, 22, 6, 0, 0, DateTimeKind.Unspecified);

    private static PgBaselineProvider.CachedBaseline Entry(DateTime analysisHour, DateTime realTime) => new()
    {
        ComputedAt = analysisHour,
        RealTime = realTime,
        Buckets = new Dictionary<(int HourOfDay, int DayOfWeek), BaselineBucket>(),
        Clock = LocalClockWindow.Utc(analysisHour),
    };

    [Fact]
    public void AnEntry_AnswersOnlyItsOwnHourAndEngine_AndOnlyInsideTheTtl()
    {
        var cache = new BaselineCache();
        var entry = Entry(Hour, DateTime.UtcNow);
        cache.Put("sql", 7, "7:cpu", entry);

        Assert.True(cache.TryGet("sql", 7, "7:cpu", Hour, out var hit));
        Assert.Same(entry, hit);

        /* The next analysis hour is a different window: never this entry. */
        Assert.False(cache.TryGet("sql", 7, "7:cpu", Hour.AddHours(1), out _));
        /* Another engine's provider, the same server id and cache key: never this entry. */
        Assert.False(cache.TryGet("pg", 7, "7:cpu", Hour, out _));
        /* Another series or server. */
        Assert.False(cache.TryGet("sql", 7, "7:waits", Hour, out _));
        Assert.False(cache.TryGet("sql", 8, "7:cpu", Hour, out _));

        /* A compute a full TTL old is dead whatever its hour. */
        cache.Put("sql", 7, "7:io", Entry(Hour, DateTime.UtcNow - PgBaselineProvider.CacheTtl - TimeSpan.FromSeconds(1)));
        Assert.False(cache.TryGet("sql", 7, "7:io", Hour, out _));
    }

    [Fact]
    public void TheSweep_DropsOnlyTheEntriesNoLookupCanTake()
    {
        var cache = new BaselineCache();
        var now = DateTime.UtcNow;
        cache.Put("sql", 1, "1:cpu", Entry(Hour, now - PgBaselineProvider.CacheTtl - TimeSpan.FromMinutes(1)));
        cache.Put("sql", 1, "1:waits", Entry(Hour, now));
        cache.Put("pg", 2, "2:pg_statement_share:42", Entry(Hour.AddHours(-3), now - TimeSpan.FromHours(5)));
        Assert.Equal(3, cache.Count);

        /* Not due yet: the tier was built moments ago, and a sweep runs at most once per quarter TTL. */
        cache.SweepIfDue(now);
        Assert.Equal(3, cache.Count);

        cache.SweepIfDue(now + PgBaselineProvider.CacheTtl / 3);
        Assert.Equal(1, cache.Count);
        Assert.True(cache.TryGet("sql", 1, "1:waits", Hour, out _));
    }

    [Fact]
    public void Invalidate_DropsEveryEntryForTheServer_OfEveryEngineAndHour()
    {
        var cache = new BaselineCache();
        var now = DateTime.UtcNow;
        cache.Put("sql", 1, "1:cpu", Entry(Hour, now));
        cache.Put("pg", 1, "1:pg_tps", Entry(Hour.AddHours(-1), now));
        cache.Put("pg", 12, "12:pg_tps", Entry(Hour, now));

        cache.Invalidate(1);

        Assert.Equal(1, cache.Count);
        Assert.True(cache.TryGet("pg", 12, "12:pg_tps", Hour, out _));
    }

    /// <summary>The sweep's two halves, shared by the tier and every provider's own cache: the gate opens once per quarter
    /// TTL, to one caller; the removal takes exactly the entries no lookup can take.</summary>
    [Fact]
    public void TheSweepGate_OpensOncePerQuarterTtl_AndRemoveDead_TakesOnlyTheDead()
    {
        var now = DateTime.UtcNow;
        var lastSweep = now.Ticks;
        Assert.False(BaselineCache.SweepIsDue(ref lastSweep, now + PgBaselineProvider.CacheTtl / 5));
        Assert.True(BaselineCache.SweepIsDue(ref lastSweep, now + PgBaselineProvider.CacheTtl / 3));
        Assert.False(BaselineCache.SweepIsDue(ref lastSweep, now + PgBaselineProvider.CacheTtl / 3));

        var map = new ConcurrentDictionary<string, PgBaselineProvider.CachedBaseline>(StringComparer.Ordinal)
        {
            ["1:cpu"] = Entry(Hour, now),
            ["1:pg_statement_share:42"] = Entry(Hour, now - PgBaselineProvider.CacheTtl),
            ["1:pg_statement_share:43"] = Entry(Hour.AddHours(-2), now - TimeSpan.FromHours(3)),
        };
        Assert.Equal(2, BaselineCache.RemoveDead(map, now));
        Assert.Equal(["1:cpu"], map.Keys);
    }

    /// <summary>
    /// The wiring: ONE instance per process, registered as a singleton and handed to every host's analysis service — the
    /// worker's per-pass services, the MCP host's and the web host's. Any host constructing its service without it would
    /// keep a private cache and quietly reopen the gap (a scheduled pass cannot warm an MCP call it does not share with).
    /// Scanned as code, so the comments that explain the wiring are free to name it.
    /// </summary>
    [Fact]
    public void TheService_HandsOneBaselineCacheToEveryHostsAnalysis()
    {
        string Code(params string[] path) => CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile(path));

        var program = Code("Darling", "PerformanceMonitor.Darling.Service", "Program.cs");
        Assert.Contains("builder.Services.AddSingleton<BaselineCache>();", program, StringComparison.Ordinal);

        var worker = Code("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        Assert.Contains("BaselineCache baselineCache)", worker, StringComparison.Ordinal);
        Assert.Contains("new DarlingAnalysisService(_postgres!, planFetcher, _logger, _baselineCache)", worker, StringComparison.Ordinal);

        var mcp = Code("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpHostService.cs");
        Assert.Contains("new DarlingAnalysisService(postgres, planFetcher, _logger, _baselineCache)", mcp, StringComparison.Ordinal);

        var web = Code("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingWebHostService.cs");
        Assert.Contains("DarlingWebEndpoints.MapAll(app, postgres, _collectorState, _logger, _baselineCache, postgresConfig, _readLatency);", web, StringComparison.Ordinal);
        var endpoints = Code("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs");
        Assert.Contains("new DarlingAnalysisService(postgres, logger: logger, baselineCache: baselineCache)", endpoints, StringComparison.Ordinal);

        /* And no other production site builds an analysis service at all (a new one must be wired, or say why not). */
        var sites = new[] { worker, mcp, endpoints }.Sum(code => CountOf(code, "new DarlingAnalysisService("));
        Assert.Equal(3, sites);
    }

    private static int CountOf(string text, string needle)
    {
        var count = 0;
        for (var at = text.IndexOf(needle, StringComparison.Ordinal); at >= 0; at = text.IndexOf(needle, at + 1, StringComparison.Ordinal))
            count++;
        return count;
    }
}

/// <summary>
/// #3941, live: the answer a shared entry gives is the answer a fresh compute gives, the cache removes the reads, and
/// every invalidation case the tier states. #1776 own-store: a private scratch database per test, because the whole
/// point is two providers (or two passes) over the same server id, and a shared store would let another class's rows
/// into the window.
/// </summary>
public sealed class SharedBaselineCacheLiveTests
{
    private const int ServerId = -3941_01;
    private const string ServerName = "3941-shared-baselines";

    [Fact]
    public async Task Live_TwoInstantsInOneAnalysisHour_ComputeTheSameBaseline()
    {
        await using var store = await SeededStore.CreateAsync();
        if (store is null) return;
        var (h, ct) = (store.Hour, TestContext.Current.CancellationToken);

        /* Two PRIVATE providers, so no cache is involved: the window itself must end on the hour. Before #3941 the
           later instant's window held the 45 minutes of rows between the two instants, all in the looked-up bucket. */
        foreach (var metric in store.UnkeyedMetrics)
        {
            var early = await new PgTargetBaselineProvider(store.Postgres).GetBaselineAsync(ServerId, metric, h.AddMinutes(5), ct);
            var late = await new PgTargetBaselineProvider(store.Postgres).GetBaselineAsync(ServerId, metric, h.AddMinutes(50), ct);
            Assert.True(early.SampleCount > 0, $"{metric}: the seed produced no baseline — the comparison would prove nothing");
            AssertSameBucket(early, late, metric);
        }
    }

    [Fact]
    public async Task Live_ASharedEntry_IsTheFreshAnswer_AndReadsNothing()
    {
        await using var store = await SeededStore.CreateAsync();
        if (store is null) return;
        var (h, ct) = (store.Hour, TestContext.Current.CancellationToken);
        var shared = new BaselineCache();

        foreach (var metric in store.UnkeyedMetrics)
        {
            var (_, firstReads) = await CommandCapture.CountBaselineReadsAsync(
                () => new PgTargetBaselineProvider(store.Postgres, null, shared).GetBaselineAsync(ServerId, metric, h.AddMinutes(5), ct));
            Assert.Equal(1, firstReads);

            /* Another provider — another pass, or another host — later in the same hour. */
            var (hit, hitReads) = await CommandCapture.CountBaselineReadsAsync(
                () => new PgTargetBaselineProvider(store.Postgres, null, shared).GetBaselineAsync(ServerId, metric, h.AddMinutes(50), ct));
            Assert.Equal(0, hitReads);

            var fresh = await new PgTargetBaselineProvider(store.Postgres).GetBaselineAsync(ServerId, metric, h.AddMinutes(50), ct);
            AssertSameBucket(fresh, hit, metric);
        }

        /* The keyed series share the same way: one read for the set, none for the next provider. */
        var keys = store.StatementKeys;
        foreach (var metric in new[] { MetricNames.PgStatementShare, MetricNames.PgStatementMeanMs })
        {
            var (_, firstReads) = await CommandCapture.CountBaselineReadsAsync(
                () => new PgTargetBaselineProvider(store.Postgres, null, shared).GetBaselinesAsync(ServerId, metric, keys, h.AddMinutes(5), ct));
            Assert.Equal(1, firstReads);

            var (hits, hitReads) = await CommandCapture.CountBaselineReadsAsync(
                () => new PgTargetBaselineProvider(store.Postgres, null, shared).GetBaselinesAsync(ServerId, metric, keys, h.AddMinutes(50), ct));
            Assert.Equal(0, hitReads);

            var fresh = await new PgTargetBaselineProvider(store.Postgres).GetBaselinesAsync(ServerId, metric, keys, h.AddMinutes(50), ct);
            foreach (var key in keys)
                AssertSameBucket(fresh[key], hits[key], $"{metric} {key}");
        }
    }

    [Fact]
    public async Task Live_TheNextAnalysisHour_AndAnInvalidation_Recompute()
    {
        await using var store = await SeededStore.CreateAsync();
        if (store is null) return;

        /* Anchor to midnight of store.Hour's own UTC day, not store.Hour itself: store.Hour is End's hour minus 6,
           which wraps to hour 23 of the PREVIOUS day whenever CI's wall clock is 00:00-05:59 UTC — the exact window
           this test started failing in. h.AddHours(1) below must stay inside the SAME RoundedDay key as h for the
           "next hour is a cache hit" assertion to hold no matter what hour CI runs at. */
        var h = PgBaselineProvider.RoundedDay(store.Hour);
        var ct = TestContext.Current.CancellationToken;
        var shared = new BaselineCache();
        var provider = new PgTargetBaselineProvider(store.Postgres, null, shared);

        await provider.GetBaselineAsync(ServerId, MetricNames.PgTps, h.AddMinutes(5), ct);

        /* #4298: PgTps is a daily-cache arm through PgTargetBaselineProvider, so the next HOUR — still the same
           UTC day — is a shared hit, not a recompute. */
        var (_, nextHour) = await CommandCapture.CountBaselineReadsAsync(
            () => new PgTargetBaselineProvider(store.Postgres, null, shared).GetBaselineAsync(ServerId, MetricNames.PgTps, h.AddMinutes(65), ct));
        Assert.Equal(0, nextHour);

        /* The next UTC day is a different day-grain key (midnight moved), so a fresh compute. */
        var (_, nextDay) = await CommandCapture.CountBaselineReadsAsync(
            () => new PgTargetBaselineProvider(store.Postgres, null, shared).GetBaselineAsync(
                ServerId, MetricNames.PgTps, PgBaselineProvider.RoundedDay(h).AddDays(1).AddHours(1), ct));
        Assert.Equal(1, nextDay);

        provider.InvalidateCache(ServerId);
        var (_, afterInvalidate) = await CommandCapture.CountBaselineReadsAsync(
            () => new PgTargetBaselineProvider(store.Postgres, null, shared).GetBaselineAsync(ServerId, MetricNames.PgTps, h.AddMinutes(5), ct));
        Assert.Equal(1, afterInvalidate);
    }

    /// <summary>
    /// A long-lived provider (the MCP and web hosts' singletons) forgets the series no lookup can take any more: its own
    /// cache is swept like the shared tier, so the keyed series it was asked for on earlier passes — keys that follow each
    /// server's top statements — stop counting toward the keyed-cardinality note's bar once they are dead. PgStatementShare
    /// is a daily-cache arm (#4298), so an entry here is fresh until its FreshUntilUtc — a rolling 24 hours from when it
    /// was computed — not until CacheTtl, and the sweep proves both bounds: alive at CacheTtl's own mark, gone a day on.
    /// </summary>
    [Fact]
    public async Task Live_ALongLivedProvider_SweepsItsDeadSeries()
    {
        await using var store = await SeededStore.CreateAsync();
        if (store is null) return;
        var (h, ct) = (store.Hour, TestContext.Current.CancellationToken);
        var provider = new PgTargetBaselineProvider(store.Postgres);

        await provider.GetBaselinesAsync(ServerId, MetricNames.PgStatementShare, store.StatementKeys, h.AddMinutes(5), ct);
        Assert.Equal(store.StatementKeys.Length, provider.KeyedEntryCount);

        provider.SweepIfDue(DateTime.UtcNow + PgBaselineProvider.CacheTtl / 2);
        Assert.Equal(store.StatementKeys.Length, provider.KeyedEntryCount);

        /* Past CacheTtl but still well inside FreshUntilUtc's rolling day: still alive under the daily tier. */
        provider.SweepIfDue(DateTime.UtcNow + PgBaselineProvider.CacheTtl + TimeSpan.FromMinutes(1));
        Assert.Equal(store.StatementKeys.Length, provider.KeyedEntryCount);

        provider.SweepIfDue(DateTime.UtcNow + TimeSpan.FromDays(1) + TimeSpan.FromMinutes(1));
        Assert.Equal(0, provider.KeyedEntryCount);
    }

    /// <summary>
    /// A failed compute is its caller's "no baseline this pass" and nobody else's: a provider that cannot read the
    /// relations (here a store with no schema at all — the shape of a role denied a relation, or a timeout) caches its
    /// failure locally, the next provider in the process computes for itself instead of inheriting it, and once that
    /// one has succeeded the failed provider takes the shared success over its own failure.
    /// </summary>
    [Fact]
    public async Task Live_AFailedCompute_IsNotShared_AndASharedSuccessBeatsIt()
    {
        await using var store = await SeededStore.CreateAsync();
        if (store is null) return;
        var (h, ct) = (store.Hour, TestContext.Current.CancellationToken);

        await using var empty = await ScratchPostgres.CreateAsync(store.BaseConnectionString, ct);
        await using var unreadable = NpgsqlDataSource.Create(empty.ConnectionString);

        var shared = new BaselineCache();
        var failing = new PgTargetBaselineProvider(unreadable, null, shared);
        var failed = await failing.GetBaselineAsync(ServerId, MetricNames.PgTps, h.AddMinutes(5), ct);
        Assert.Equal(0, failed.SampleCount);
        Assert.Equal(0, shared.Count);

        var (good, goodReads) = await CommandCapture.CountBaselineReadsAsync(
            () => new PgTargetBaselineProvider(store.Postgres, null, shared).GetBaselineAsync(ServerId, MetricNames.PgTps, h.AddMinutes(5), ct));
        Assert.Equal(1, goodReads);
        Assert.True(good.SampleCount > 0);

        var (recovered, recoveredReads) = await CommandCapture.CountBaselineReadsAsync(
            () => failing.GetBaselineAsync(ServerId, MetricNames.PgTps, h.AddMinutes(30), ct));
        Assert.Equal(0, recoveredReads);
        AssertSameBucket(good, recovered, MetricNames.PgTps);
    }

    /// <summary>
    /// The issue's shape end to end: two scheduled passes of one server inside one analysis hour, each on a FRESH
    /// <see cref="DarlingAnalysisService"/> (as the worker builds them) sharing the process's tier. The first reads every
    /// baseline; the second reads none — before #3941 it read them all again. And what the baselines feed is unchanged:
    /// the anomaly detector's facts off a shared, warm provider are the facts off a private, cold one.
    /// </summary>
    [Fact]
    public async Task Live_ASecondPassInTheHour_ReadsNoBaseline_AndDetectsTheSameAnomalies()
    {
        await using var store = await SeededStore.CreateAsync();
        if (store is null) return;
        var ct = TestContext.Current.CancellationToken;
        var shared = new BaselineCache();

        /* Two pass ends forty minutes apart whose windows START in one hour — the hour the baselines are asked at — and
           end inside the seeded spike. */
        var top = PgBaselineProvider.RoundedHour(store.End);
        var (firstEnd, secondEnd) = (top.AddMinutes(-50), top.AddMinutes(-10));
        var (_, firstPass) = await CommandCapture.CountBaselineReadsAsync(
            () => new DarlingAnalysisService(store.Postgres, baselineCache: shared).AnalyzeAsync(ServerId, ServerName, 4, ct, asOfUtc: firstEnd));
        var (_, secondPass) = await CommandCapture.CountBaselineReadsAsync(
            () => new DarlingAnalysisService(store.Postgres, baselineCache: shared).AnalyzeAsync(ServerId, ServerName, 4, ct, asOfUtc: secondEnd));
        var (_, privatePass) = await CommandCapture.CountBaselineReadsAsync(
            () => new DarlingAnalysisService(store.Postgres).AnalyzeAsync(ServerId, ServerName, 4, ct, asOfUtc: secondEnd));

        Assert.True(firstPass >= 10, $"the first pass read {firstPass} baselines — the seed is not exercising the arms");
        Assert.Equal(0, secondPass);
        Assert.Equal(firstPass, privatePass);

        /* The detector, fed from each: warm off the shared tier (computed at an earlier instant in the hour) vs cold. */
        var context = new AnalysisContext
        {
            ServerId = ServerId,
            ServerName = ServerName,
            TimeRangeStart = secondEnd.AddHours(-4),
            TimeRangeEnd = secondEnd,
            ServerUtcOffset = TimeSpan.Zero,
            Coverage = new WindowCoverage { NominalMs = 4 * 3_600_000, ObservedMs = 4 * 3_600_000, SampleCount = 240 },
        };
        var warm = await new PgTargetAnomalyDetector(store.Postgres, new PgTargetBaselineProvider(store.Postgres, null, shared)).DetectAnomaliesAsync(context);
        var cold = await new PgTargetAnomalyDetector(store.Postgres, new PgTargetBaselineProvider(store.Postgres)).DetectAnomaliesAsync(context);

        Assert.NotEmpty(cold);
        Assert.Equal(Describe(cold), Describe(warm));
    }

    /* ───────────────────────── helpers ───────────────────────── */

    private static List<string> Describe(IEnumerable<Fact> facts) => facts
        .Select(f => $"{f.Key}|{f.DatabaseName}|{f.Value:R}|" + string.Join(",", f.Metadata.OrderBy(m => m.Key, StringComparer.Ordinal).Select(m => $"{m.Key}={Math.Round(m.Value, 9):R}")))
        .OrderBy(s => s, StringComparer.Ordinal)
        .ToList();

    /// <summary>Equal, or equal up to float8 summation order in the mean and deviation (a relative 1e-12, the #3901
    /// equivalence test's bar): two computes over the same rows are not promised the same accumulation order.</summary>
    private static void AssertSameBucket(BaselineBucket expected, BaselineBucket actual, string what)
    {
        Assert.Equal(expected.Tier, actual.Tier);
        Assert.Equal((expected.HourOfDay, expected.DayOfWeek), (actual.HourOfDay, actual.DayOfWeek));
        Assert.True(expected.SampleCount == actual.SampleCount, $"{what}: {expected.SampleCount} samples vs {actual.SampleCount}");
        Assert.Equal(expected.DistinctDays, actual.DistinctDays);
        Assert.Equal(expected.Median, actual.Median);
        Assert.Equal(expected.Mad, actual.Mad);
        AssertSameReal(expected.Mean, actual.Mean, what + " mean");
        AssertSameReal(expected.StdDev, actual.StdDev, what + " stddev");
    }

    private static void AssertSameReal(double expected, double actual, string what)
    {
        if (expected == actual) return;
        Assert.True(Math.Abs(expected - actual) <= 1e-12 * Math.Max(Math.Abs(expected), Math.Abs(actual)), $"{what}: {expected:R} vs {actual:R}");
    }

    /// <summary>
    /// A private store holding one PostgreSQL target with 31 days of the series the anomaly detector baselines —
    /// one-minute TPS and sessions, five-minute CPU and statements — and a spike over the last 250 minutes, so a pass
    /// over the newest four hours has anomalies to find. Statement <c>q</c> spends about <c>100 q</c> ms per collection,
    /// so every window's top statements are the same five and a pass's keyed candidate set does not move with it.
    /// </summary>
    private sealed class SeededStore : IAsyncDisposable
    {
        private readonly ScratchPostgres _scratch;

        private SeededStore(ScratchPostgres scratch, string baseConnectionString, DateTime end)
        {
            _scratch = scratch;
            BaseConnectionString = baseConnectionString;
            End = end;
            Postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        }

        public NpgsqlDataSource Postgres { get; }

        public string BaseConnectionString { get; }

        /// <summary>The newest seeded minute.</summary>
        public DateTime End { get; }

        /// <summary>An analysis hour whose 30-day window lies inside the seed and before the spike.</summary>
        public DateTime Hour => PgBaselineProvider.RoundedHour(End).AddHours(-6);

        public string[] UnkeyedMetrics { get; } = [MetricNames.PgTps, MetricNames.PgSessionCount, MetricNames.PgCpu, MetricNames.PgStatementMeanMs];

        public string[] StatementKeys { get; } = ["4", "5", "6", "7", "8"];

        public static async Task<SeededStore?> CreateAsync()
        {
            var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
            Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the #3941 shared-baseline tests.");
            var ct = TestContext.Current.CancellationToken;

            var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
            await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);
                await PgTargetFactCollectorTests.RegisterServerAsync(connection, ServerId, ServerName, MonitoredEngineKind.Postgres, 17, ct);

                var now = DateTime.UtcNow;
                var end = DateTime.SpecifyKind(new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified).AddMinutes(-1);
                const int minutes = 31 * 24 * 60;
                var start = end.AddMinutes(-minutes);

                await PlantAsync(connection, @"
WITH s AS (SELECT n, CASE WHEN n >= $5 - 250 THEN 3600 ELSE 600 + round(60 * sin(n)) END AS commits FROM generate_series(0, $5) AS n)
INSERT INTO pg_database_stats (collection_id, collection_time, server_id, server_name, database_name,
     xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset)
SELECT $1 + n, $2 + (n * interval '1 minute'), $3, $4, 'appdb', SUM(commits) OVER (ORDER BY n), 0, 100 * n, 9000 * n, 0, 0, 0, NULL FROM s", start, minutes, ct);
                await PlantAsync(connection, @"
INSERT INTO pg_session_states (collection_id, collection_time, server_id, server_name, state_is_redacted,
     total_sessions, active_sessions, idle_in_transaction_sessions, reportable_sessions)
SELECT $1 + n, $2 + (n * interval '1 minute'), $3, $4, FALSE, CASE WHEN n >= $5 - 250 THEN 120 ELSE 20 + (n % 3) END, 4, 1, 1
FROM generate_series(0, $5) AS n", start, minutes, ct);
                await PlantAsync(connection, @"
INSERT INTO pg_cpu_utilization (collection_id, collection_time, server_id, server_name, sample_time,
     cpu_percent, acu_utilization_percent, serverless_capacity_acu, max_configured_acu)
SELECT $1 + n, $2 + (n * interval '1 minute'), $3, $4, $2 + (n * interval '1 minute'),
       CASE WHEN n >= $5 - 250 THEN 100 ELSE 50 + (n % 5) END, CASE WHEN n >= $5 - 250 THEN 90 ELSE 30 + (n % 4) END, 3.8, 12
FROM generate_series(0, $5, 5) AS n", start, minutes, ct);
                await PlantAsync(connection, @"
INSERT INTO pg_statement_stats (collection_id, collection_time, server_id, server_name, queryid, database_id, user_id, toplevel,
     calls, total_exec_time_ms, max_exec_time_ms, rows_returned, shared_blks_hit, shared_blks_read,
     temp_blks_read, temp_blks_written, wal_bytes, delta_calls, delta_total_exec_time_ms, delta_rows, sample_interval_seconds)
SELECT $1 + (ROW_NUMBER() OVER ()), $2 + (c.n * interval '1 minute'), $3, $4, q.q, 16384, 10, TRUE,
       0, 0, 50.0, 10, 10, 5, 0, 0, 0, 1 + (c.n + q.q) % 7, 100 * q.q + (c.n % 13), 1 + q.q % 3, 300
FROM generate_series(0, $5, 5) AS c(n) CROSS JOIN generate_series(1, 8) AS q(q)", start, minutes, ct);

                return new SeededStore(scratch, cs!, end);
            }
        }

        private static async Task PlantAsync(NpgsqlConnection connection, string sql, DateTime start, int minutes, CancellationToken ct)
        {
            await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
            command.Parameters.AddWithValue(CollectionIdGenerator.Next());
            command.Parameters.AddWithValue(start);
            command.Parameters.AddWithValue(ServerId);
            command.Parameters.AddWithValue(ServerName);
            command.Parameters.AddWithValue(minutes);
            await command.ExecuteNonQueryAsync(ct);
        }

        public async ValueTask DisposeAsync()
        {
            await Postgres.DisposeAsync();
            await _scratch.DisposeAsync();
        }
    }
}

/// <summary>
/// The statements Npgsql executed inside a call, off its own tracing activities (source <c>Npgsql</c>, the text in
/// <c>db.query.text</c>), kept only when they descend from the call's own root activity — so statements other tests
/// run concurrently are never counted. The #3901 equivalence test's capture, shared here for the baseline count.
/// </summary>
internal static class CommandCapture
{
    /// <summary>A baseline COMPUTE: every bucket statement keys on the target's local clock through the one shared
    /// spelling (<see cref="BaselineLocalClock.LocalCollectionTimeSql"/>, which <c>LocalClockBucketKeyTests</c> holds
    /// every arm to), and nothing else the pass runs does, except B's tiled window reads (#3653), which key their
    /// tiles through WindowTiles.LocalHourSql and are excluded by name.</summary>
    internal static bool IsBaselineRead(string sql) =>
        sql.Contains(BaselineLocalClock.LocalCollectionTimeSql, StringComparison.Ordinal)
        && !sql.Contains(WindowTiles.LocalHourSql, StringComparison.Ordinal);

    internal static async Task<(T Result, int BaselineReads)> CountBaselineReadsAsync<T>(Func<Task<T>> body)
    {
        var (result, commands) = await CaptureAsync(body);
        return (result, commands.Count(IsBaselineRead));
    }

    internal static async Task<(T Result, List<string> Commands)> CaptureAsync<T>(Func<Task<T>> body)
    {
        using var source = new ActivitySource("Darling.Tests.3941");
        var captured = new ConcurrentQueue<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == "Npgsql" || s.Name == source.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = captured.Enqueue,
        };
        ActivitySource.AddActivityListener(listener);

        T result;
        ActivityTraceId trace;
        using (var root = source.StartActivity("shared-baseline-probe"))
        {
            Assert.NotNull(root);
            trace = root!.TraceId;
            result = await body();
        }

        var commands = captured
            .Where(a => a.Source.Name == "Npgsql" && a.TraceId == trace)
            .Select(a => a.TagObjects.Select(t => t.Value as string).FirstOrDefault(v => v is not null && v.Contains("SELECT", StringComparison.OrdinalIgnoreCase)))
            .Where(text => text is not null)
            .Select(text => text!)
            .ToList();
        return (result, commands);
    }
}
