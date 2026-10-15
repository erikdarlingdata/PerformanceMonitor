/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The reuse rule of the PLAN_REGRESSION candidate cache (#5812), pinned without a store: a current pass of the same
/// shape reuses a read younger than the refresh period, and nothing else does.
/// </summary>
public sealed class PlanRegressionCandidateReuseRuleTests
{
    private static readonly DateTime Now = new(2026, 10, 14, 12, 0, 0, DateTimeKind.Utc);

    /// <summary>The reuse window: one analysis cadence, 30 minutes by default (ruling R3 on #5812).</summary>
    private static readonly TimeSpan Window = PgFactCollector.PlanRegressionCandidateCache.DefaultRefreshAfter;

    [Fact]
    public void TheReuseWindow_IsOneAnalysisCadence_SetByTheWorker_AndTheScheduledPassAlwaysReads()
    {
        Assert.Equal(TimeSpan.FromMinutes(30), PgFactCollector.PlanRegressionCandidateCache.DefaultRefreshAfter);
        var cache = new PgFactCollector.PlanRegressionCandidateCache();
        Assert.Equal(Window, cache.RefreshAfter);
        cache.RefreshAfter = TimeSpan.FromMinutes(15);
        cache.Store("store", 1, "s", Now.AddMinutes(-14), Array.Empty<PgFactCollector.PlanRegressionCandidate>());
        Assert.NotNull(cache.TryGet("store", 1, "s", Now, Now));
        cache.Store("store", 1, "s", Now.AddMinutes(-15), Array.Empty<PgFactCollector.PlanRegressionCandidate>());
        Assert.Null(cache.TryGet("store", 1, "s", Now, Now));

        /* The worker sets the shared window from the clamped cadence; the scheduled pass (the one caller that persists
           caveats) refreshes and never reuses; an on-demand current call reuses. */
        var worker = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        Assert.Contains("PlanRegressionCandidateCache.Shared.RefreshAfter = TimeSpan.FromMinutes(analysisIntervalMinutes);", worker, StringComparison.Ordinal);
        var service = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "DarlingAnalysisService.cs");
        Assert.Contains("ReusePlanRegressionCandidates = asOfUtc is null && !persistCollectionCaveats,", service, StringComparison.Ordinal);
        Assert.Contains("RefreshPlanRegressionCandidates = asOfUtc is null && persistCollectionCaveats,", service, StringComparison.Ordinal);
        var read = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgFactCollector.QueryPerf.cs");
        Assert.Contains("var reused = shape is null || context.RefreshPlanRegressionCandidates", read, StringComparison.Ordinal);
    }

    private static PgFactCollector.PlanRegressionCandidateCache.Entry EntryAt(DateTime readAt, string shape = "s") =>
        new(shape, readAt, Array.Empty<PgFactCollector.PlanRegressionCandidate>());

    [Fact]
    public void ACurrentPassOfTheSameShape_ReusesAReadYoungerThanTheRefreshPeriod()
    {
        var entry = EntryAt(Now.AddMinutes(-10));
        Assert.True(PgFactCollector.PlanRegressionCandidateCache.CanReuse(entry, "s", Now, Now, Window));
        Assert.True(PgFactCollector.PlanRegressionCandidateCache.CanReuse(
            EntryAt(Now - Window + TimeSpan.FromSeconds(1)), "s", Now, Now, Window));
    }

    [Fact]
    public void AReadAsOldAsTheRefreshPeriod_IsReadAgain()
    {
        Assert.False(PgFactCollector.PlanRegressionCandidateCache.CanReuse(
            EntryAt(Now - Window), "s", Now, Now, Window));
    }

    [Fact]
    public void AnotherShape_NoEntry_APastAsOf_OrAReadFromTheFuture_IsReadLive()
    {
        var entry = EntryAt(Now.AddMinutes(-30));
        Assert.False(PgFactCollector.PlanRegressionCandidateCache.CanReuse(entry, "other", Now, Now, Window));
        Assert.False(PgFactCollector.PlanRegressionCandidateCache.CanReuse(null, "s", Now, Now, Window));
        Assert.False(PgFactCollector.PlanRegressionCandidateCache.CanReuse(entry, "s", Now.AddHours(-1), Now, Window));
        Assert.False(PgFactCollector.PlanRegressionCandidateCache.CanReuse(EntryAt(Now.AddMinutes(1)), "s", Now, Now, Window));
    }

    [Fact]
    public void TheShape_CarriesTheWindowDay_TheBuiltDays_AndTheScopeValues_ButNotTheTimeBounds()
    {
        static NpgsqlCommand Command(DateTime bound, string scope)
        {
            var cmd = new NpgsqlCommand("SELECT 1");
            cmd.Parameters.AddWithValue(7);
            cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, bound);
            cmd.Parameters.AddWithValue(new[] { scope });
            return cmd;
        }

        var floor = new DateTime(2026, 9, 30);
        var days = new List<DateOnly> { new(2026, 10, 1), new(2026, 10, 2) };
        var a = PgFactCollector.PlanRegressionCandidateCache.ShapeOf(Command(Now, "db1"), 2, floor, days);
        var b = PgFactCollector.PlanRegressionCandidateCache.ShapeOf(Command(Now.AddMinutes(30), "db1"), 2, floor, days);
        Assert.Equal(a, b);
        Assert.NotEqual(a, PgFactCollector.PlanRegressionCandidateCache.ShapeOf(Command(Now, "db2"), 2, floor, days));
        Assert.NotEqual(a, PgFactCollector.PlanRegressionCandidateCache.ShapeOf(Command(Now, "db1"), 2, floor.AddDays(1), days));
        Assert.NotEqual(a, PgFactCollector.PlanRegressionCandidateCache.ShapeOf(Command(Now, "db1"), 2, floor, days.Take(1).ToList()));
    }
}

/// <summary>
/// The PLAN_REGRESSION candidate reuse through the real collector on a scratch store (#5812). A regression is seeded and
/// the window's closed days are built; a current pass reads it and fills the cache. Then the costly plan's rows are
/// deleted from the interval table and the day totals: a second current pass still reports the regression, which it can
/// only do without reading either table, while a pass that may not reuse (a past <c>as_of</c>) reads live and finds none.
/// A built day turning stale changes the read's shape, so the next current pass reads live too.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and then works entirely inside it. */
public sealed class PlanRegressionCandidateReuseLiveTests
{
    private const int ServerId = -5812001;
    private const string ServerName = "CandidateReuseSrv";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ASecondCurrentPass_ReusesTheRead_AndAPassThatMayNotReuse_ReadsLive()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5812 reuse test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var (start, end) = await SeedAndBuildAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var cache = new PgFactCollector.PlanRegressionCandidateCache();

        var first = NewContext(start, end, reuse: true);
        var firstFact = (await Collector(postgres, cache).CollectFactsAsync(first)).Single(f => f.Key == "PLAN_REGRESSION");
        Assert.Equal(0, cache.Hits);
        Assert.Equal(1, cache.Misses);
        Assert.NotNull(first.PlanRegressionWindowStart);

        /* The regression's costly plan is gone from both tables the read can take it from. */
        await ExecAsync(connection, $"DELETE FROM collect.query_store_interval_latest WHERE server_id = {ServerId} AND plan_id = 12", ct);
        await ExecAsync(connection, $"DELETE FROM collect.plan_regression_daily WHERE server_id = {ServerId} AND plan_id = 12", ct);

        var second = NewContext(start, end.AddSeconds(1), reuse: true);
        var secondFact = (await Collector(postgres, cache).CollectFactsAsync(second)).Single(f => f.Key == "PLAN_REGRESSION");
        Assert.Equal(1, cache.Hits);
        Assert.Equal(firstFact.Value, secondFact.Value);
        Assert.Equal(firstFact.Metadata["offender_count"], secondFact.Metadata["offender_count"]);
        Assert.Equal(first.PlanRegressionWindowStart, second.PlanRegressionWindowStart);
        Assert.Equal(first.PlanRegressionOffenders, second.PlanRegressionOffenders);

        /* A pass that may not reuse reads the store as it is now: the regression is gone. It neither reads nor fills the cache. */
        var live = NewContext(start, end, reuse: false);
        Assert.DoesNotContain(await Collector(postgres, cache).CollectFactsAsync(live), f => f.Key == "PLAN_REGRESSION");
        Assert.Equal(1, cache.Hits);
        Assert.Equal(1, cache.Misses);

        /* The scheduled pass (R3): it never reuses, reads the store as it is now, and refills the entry; the on-demand call
           after it reuses that fresh read, so it no longer shows the regression either. */
        var scheduled = NewContext(start, end.AddSeconds(2), reuse: false);
        scheduled.RefreshPlanRegressionCandidates = true;
        Assert.DoesNotContain(await Collector(postgres, cache).CollectFactsAsync(scheduled), f => f.Key == "PLAN_REGRESSION");
        Assert.Equal(1, cache.Hits);
        Assert.Equal(1, cache.Misses);
        Assert.DoesNotContain(await Collector(postgres, cache).CollectFactsAsync(NewContext(start, end.AddSeconds(3), reuse: true)), f => f.Key == "PLAN_REGRESSION");
        Assert.Equal(2, cache.Hits);
    }

    [Fact]
    public async Task ABuiltDayTurningStale_ChangesTheShape_SoTheNextCurrentPassReadsLive()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5812 reuse test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var (start, end) = await SeedAndBuildAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var cache = new PgFactCollector.PlanRegressionCandidateCache();

        Assert.Contains(await Collector(postgres, cache).CollectFactsAsync(NewContext(start, end, reuse: true)), f => f.Key == "PLAN_REGRESSION");
        Assert.Equal(1, cache.Misses);

        await ExecAsync(connection, $"UPDATE collect.plan_regression_daily_built SET late_seq = late_seq + 1 WHERE server_id = {ServerId} AND day = (SELECT max(day) FROM collect.plan_regression_daily_built WHERE server_id = {ServerId})", ct);
        await ExecAsync(connection, $"DELETE FROM collect.query_store_interval_latest WHERE server_id = {ServerId} AND plan_id = 12", ct);

        Assert.DoesNotContain(await Collector(postgres, cache).CollectFactsAsync(NewContext(start, end, reuse: true)), f => f.Key == "PLAN_REGRESSION");
        Assert.Equal(0, cache.Hits);
        Assert.Equal(2, cache.Misses);
    }

    [Fact]
    public async Task GetAnalysisFactsAtOneHour_AfterTheScheduledPass_ReusesItsCandidates_AndReadsFreshOncePastRefreshAfter()
    {
        /* #5812: the scheduled pass (AnalyzeAsync with persistCollectionCaveats, as DarlingWorker calls it, hours back 4)
           reads and fills the entry; a get_analysis_facts call (CollectAndScoreFactsAsync, the tool's own entry point) with
           hours_back 1 and no as_of then makes no PLAN_REGRESSION read. Proved the way the first test proves it: after the
           pass the costly plan's rows are deleted from both tables a read can take them from, so the call reports the
           regression only if it used the pass's candidates, and a read past RefreshAfter finds none. The test's own cache's
           hit and miss counters (never the process-wide Shared one, which other classes and the worker touch) say which
           path ran.
           It once read pg_stat_statements' entries for the reads instead. Those are cluster-wide: this own-store class runs
           in parallel with classes that call pg_stat_statements_reset() for the whole cluster or fill the hash table until
           it evicts, so the pass's entry could vanish with no read at all (CI run 38702741139: hits 1, misses 0, and no
           entry after the call). */
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5812 reuse test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await SeedAndBuildAsync(connection, ct);

        /* Eight days of wait_stats, so the pass clears its 24-hour history gate and collects facts. */
        await ExecAsync(connection, $@"
INSERT INTO collect.wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms)
VALUES (1, now() - interval '8 days', {ServerId}, '{ServerName}', 'AN5812_WAIT', 50, 1000),
       (2, now() - interval '3 hours', {ServerId}, '{ServerName}', 'AN5812_WAIT', 50, 1000)", ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var service = new DarlingAnalysisService(postgres);
        var cache = new PgFactCollector.PlanRegressionCandidateCache();
        ((PgFactCollector)service.EngineFor(null).Collector).PlanRegressionCache = cache;

        /* The scheduled pass refreshes: it reads live and fills the entry without looking it up, so neither counter moves. */
        var passStarted = DateTime.UtcNow;
        await service.AnalyzeAsync(ServerId, ServerName, 4, ct, persistCollectionCaveats: true);
        var passEnded = DateTime.UtcNow;
        Assert.Equal(0, cache.Hits);
        Assert.Equal(0, cache.Misses);

        /* The regression's costly plan is gone from both tables the read can take it from. */
        await ExecAsync(connection, $"DELETE FROM collect.query_store_interval_latest WHERE server_id = {ServerId} AND plan_id = 12", ct);
        await ExecAsync(connection, $"DELETE FROM collect.plan_regression_daily WHERE server_id = {ServerId} AND plan_id = 12", ct);

        /* The shape carries the day the 14-day window starts on (PlanRegressionWindowFloor). A 1-hour call and the 4-hour
           pass start it on the same day except from 01:00 to 04:00 UTC (from 00:00 to 01:00 both windows start on the day
           before); then the call is a different shape and reads live. Each window ends on a clock read inside its own call,
           so each floor is computed at both ends of that call; when either pair crosses a midnight the test cannot tell
           which shape the call had, and it skips (#5812 review L5: a clock read after the pass, crossing 04:00 UTC, made
           sameDay true for a pass whose window started a day earlier). */
        static DateTime Floor(DateTime windowEnd, int hours) =>
            PgFactCollector.PlanRegressionWindowFloor(windowEnd.AddHours(-hours - 24 * PgFactCollector.PlanRegressionWindowDays));
        var passFloor = Floor(passStarted, 4);
        var callStarted = DateTime.UtcNow;
        var (facts, _, _) = await service.CollectAndScoreFactsAsync(ServerId, ServerName, hoursBack: 1, cancellationToken: ct);
        var callEnded = DateTime.UtcNow;
        var callFloor = Floor(callStarted, 1);
        Assert.SkipWhen(passFloor != Floor(passEnded, 4) || callFloor != Floor(callEnded, 1),
            "The pass's or the call's 14-day window crossed a UTC midnight while it ran, so its shape is not known; the #5812 reuse check needs both.");

        if (passFloor == callFloor)
        {
            /* Only the pass's candidates still hold the costly plan: the call reused them and read neither table. */
            Assert.Contains(facts, f => f.Key == "PLAN_REGRESSION");
            Assert.Equal(1, cache.Hits);
            Assert.Equal(0, cache.Misses);
        }
        else
        {
            /* A different shape: the call looked the entry up, missed, and read the store as it is now. */
            Assert.DoesNotContain(facts, f => f.Key == "PLAN_REGRESSION");
            Assert.Equal(0, cache.Hits);
            Assert.Equal(1, cache.Misses);
        }

        /* Past RefreshAfter (set to zero, so the entry is already older than it) the same call reads fresh: one more miss,
           and the store as it is now has no regression. */
        var hits = cache.Hits;
        var misses = cache.Misses;
        cache.RefreshAfter = TimeSpan.Zero;
        var (fresh, _, _) = await service.CollectAndScoreFactsAsync(ServerId, ServerName, hoursBack: 1, cancellationToken: ct);
        Assert.DoesNotContain(fresh, f => f.Key == "PLAN_REGRESSION");
        Assert.Equal(hits, cache.Hits);
        Assert.Equal(misses + 1, cache.Misses);
    }

    private static PgFactCollector Collector(NpgsqlDataSource postgres, PgFactCollector.PlanRegressionCandidateCache cache) =>
        new(postgres) { PlanRegressionCache = cache };

    private static AnalysisContext NewContext(DateTime periodStart, DateTime periodEnd, bool reuse) => new()
    {
        ServerId = ServerId,
        ServerName = ServerName,
        TimeRangeStart = periodStart,
        TimeRangeEnd = periodEnd,
        ServerUtcOffset = TimeSpan.Zero,
        ReusePlanRegressionCandidates = reuse,
    };

    /// <summary>One query with a cheap plan five days before the pass window and a 10x costlier one in it, an old steady
    /// query, a coverage claim below the window, and the window's closed days built (the seed of
    /// <c>PlanRegressionBuiltDaysFallbackLiveTests</c>).</summary>
    private static async Task<(DateTime Start, DateTime End)> SeedAndBuildAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var end = new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Unspecified);
        var start = end.AddHours(-4);
        var windowDay = start.Date;

        await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_latest DISABLE TRIGGER trg_plan_regression_daily_late", ct);
        await InsertIntervalAsync(connection, queryId: 1, planId: 11, "0xCHEAP", cpuUs: 20_000, execs: 100, firstExec: start.AddDays(-5).AddHours(-1), ct);
        await InsertIntervalAsync(connection, queryId: 1, planId: 12, "0xCOSTLY", cpuUs: 200_000, execs: 100, firstExec: end.AddHours(-1), ct);
        await InsertIntervalAsync(connection, queryId: 2, planId: 21, "0xSTEADY", cpuUs: 2000, execs: 100, firstExec: start.AddDays(-16), ct);
        await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_latest ENABLE TRIGGER trg_plan_regression_daily_late", ct);

        await using (var coverage = new NpgsqlCommand(
            "INSERT INTO collect.query_store_interval_latest_coverage (server_id, filled_since, applied_through) VALUES ($1, $2, $2) "
            + "ON CONFLICT (server_id) DO UPDATE SET filled_since = EXCLUDED.filled_since", connection))
        {
            coverage.Parameters.AddWithValue(ServerId);
            coverage.Parameters.AddWithValue(NpgsqlDbType.Timestamp, now.Date.AddDays(-40));
            await coverage.ExecuteNonQueryAsync(ct);
        }

        for (var d = -PgFactCollector.PlanRegressionWindowDays; d <= -3; d++)
        {
            await PlanRegressionDaily.BuildDayAsync(connection, ServerId, DateOnly.FromDateTime(windowDay.AddDays(d)), now, ct);
        }

        return (start, end);
    }

    private static async Task InsertIntervalAsync(
        NpgsqlConnection connection, long queryId, long planId, string planHash, long cpuUs, long execs, DateTime firstExec, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(@"
INSERT INTO collect.query_store_interval_latest
(
    server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time,
    collection_time, query_plan_hash, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
    last_execution_time, is_forced_plan, force_failure_count
)
VALUES ($1, 'reusedb', $2, $3, NULL, $3, $4, $5, $6, $7, $8, $9, $10, $11, false, 0)", connection);
        cmd.Parameters.AddWithValue(ServerId);
        cmd.Parameters.AddWithValue(queryId);
        cmd.Parameters.AddWithValue(planId);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, firstExec);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, firstExec.AddMinutes(35));
        cmd.Parameters.AddWithValue(planHash);
        cmd.Parameters.AddWithValue("qh" + queryId);
        cmd.Parameters.AddWithValue(execs);
        cmd.Parameters.AddWithValue(cpuUs);
        cmd.Parameters.AddWithValue(cpuUs * 3);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, firstExec.AddMinutes(30));
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }
}
