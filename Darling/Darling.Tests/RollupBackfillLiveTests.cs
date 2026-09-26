/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #1759 end-to-end against a REAL TimescaleDB, on a store built into the exact broken shape the issue
/// describes: pre-existing raw history, a rollup created <c>WITH NO DATA</c> whose materialization starts
/// well after raw does.
///
/// <para>This is the leg that the unit tests cannot stand in for, because the defect lives in the ENGINE'S
/// semantics, not in ours. Both halves of the premise that produced #1759 read fine in C#: that real-time
/// aggregation is on by default (it is not, since 2.13), and that it would union in older raw anyway (it
/// cannot — the watermark is a hard partition). Only a live aggregate can show that a window below the
/// materialized floor really does come back EMPTY while raw holds the rows, and that the backfill really does
/// make it stop.</para>
///
/// <para><b>#1776 own-store</b> — mints its own scratch database rather than sharing the live fixture, so it
/// is deliberately NOT in the <c>live-postgres</c> collection: it cannot race the shared store, and
/// serializing it against classes it cannot collide with would be pure slowdown. It has to own its store
/// anyway, because it creates continuous aggregates and retention policies that the shared fixture must never
/// inherit from a test.</para>
/// </summary>
public sealed class RollupBackfillLiveTests
{
    /// <summary>Distinctive fake id — a real server_id is a storage-name hash, never this.</summary>
    private const int TestServerId = -915915;

    /// <summary>How much pre-existing history to plant before the rollup is created. Comfortably past both the
    /// 3-day refresh-policy window and the 3-day raw route margin, so the routing assertions are not sitting on
    /// a boundary.</summary>
    private const int HistoryDays = 12;

    [Fact]
    public async Task RollupCreatedOverExistingHistory_ReadsEmptyBelowItsFloor_UntilTheBackfillFixesIt()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #1759 backfill test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        /* All timestamps Kind-Unspecified — naive-UTC storage, see PgCollectorRowWriter. */
        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var rawOldest = now.AddDays(-HistoryDays);

        /* ── 1. PRE-EXISTING HISTORY: raw rows going back well before any rollup exists. ── */
        await SeedHourlyQueryStatsAsync(connection, rawOldest, now, ct);

        /* ── 2. The rollup is created over that history, WITH NO DATA — the #1759 shape exactly. ── */
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        /* #3653 LC: query_stats_hourly is one of the frozen six — it still stands up WITH NO DATA, but nothing
           ever gives it a refresh policy or feeds the raw purge gate off its coverage again (RawTierCoverage
           now names the successor). The #1759 shape this test proves — a rollup created over pre-existing
           history reads empty below its shallow floor until backfilled, and the purge gate then releases —
           only still happens on a view the freeze left live, so query_stats_interval_hourly stands in as the
           example, keeping the same source, grain and daily companion. */

        /* Materialize only the recent window, which is all the 3-day refresh policy would ever have done on a
           store that existed before its rollups. This is what makes the floor shallow. */
        await RollupBackfill.RunSliceAsync(
            connection, TimescaleSupport.QueryStatsIntervalHourlyView, now.Date.AddDays(-2), now.Date.AddDays(1), SilentDisclosure(), ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var rollups = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
        Assert.True(rollups.QueryGrainIntervalHourly, "the query_stats_interval_hourly rollup should exist after the ensure sweep");

        var before = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, rollups, ct);
        var floorBefore = before.FloorOf(TimescaleSupport.QueryStatsIntervalHourlyView);
        var rawFloor = before.RawOldestOf("query_stats");

        Assert.NotNull(floorBefore);
        Assert.NotNull(rawFloor);
        Assert.True(floorBefore > rawFloor,
            $"the rollup should start AFTER raw does on this store shape (rollup {floorBefore:O}, raw {rawFloor:O})");

        /* ── 3. THE DEFECT, MEASURED. A window below the floor really is empty on the rollup while raw holds
               the rows — this is the hard partition, not a slow path. If this assertion ever stops holding,
               the premise behind all of #1759 has changed and the fix should be revisited, not patched. ── */
        var windowStart = now.AddDays(-8);
        var rollupRows = await CountAsync(connection, $"SELECT count(*) FROM collect.{TimescaleSupport.QueryStatsIntervalHourlyView} WHERE bucket >= $1 AND bucket < $2", windowStart, now.AddDays(-6), ct);
        var rawRows = await CountAsync(connection, "SELECT count(*) FROM collect.query_stats WHERE collection_time >= $1 AND collection_time < $2", windowStart, now.AddDays(-6), ct);

        Assert.True(rawRows > 0, "raw must hold rows in the pre-coverage window, or the test is not exercising #1759");
        Assert.Equal(0, rollupRows);

        /* ── 4. PHASE 1: the router must send that window to RAW, where the data actually is. Age alone puts an
               8-day window on the hourly rollup, which would answer with silence. ── */
        var coverageLadder = before.For(TimescaleSupport.QueryStatsIntervalHourlyView, TimescaleSupport.QueryStatsIntervalDailyView);

        Assert.Equal(
            RetentionTier.Hourly,
            RetentionTierRouter.Resolve(now, windowStart, rollups.QueryGrainIntervalHourly, rollups.QueryGrainIntervalDaily));

        Assert.Equal(
            RetentionTier.Raw,
            RetentionTierRouter.Resolve(now, windowStart, rollups.QueryGrainIntervalHourly, rollups.QueryGrainIntervalDaily, coverageLadder));

        /* ── 5. PHASE 2: back fill in slices, newest-first, exactly as the verb does. ── */
        var plan = RollupBackfill.Plan(
            TimescaleSupport.QueryStatsIntervalHourlyView, rawFloor, floorBefore,
            materializedBuckets: 48, materializedBytes: 48 * 1024,
            rawBytes: 0, bucketWidth: TimeSpan.FromHours(1));

        Assert.False(plan.IsComplete);
        Assert.True(plan.Slices > 1, "a 12-day history should plan more than one slice");

        var slicesRun = 0;
        var previousFloor = floorBefore;
        foreach (var (from, to) in RollupBackfill.Slices(plan.FromUtc, plan.ToUtc))
        {
            var floorDuring = await RollupBackfill.RunSliceAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, from, to, SilentDisclosure(), ct);
            slicesRun++;

            /* Progress only ever goes BACKWARDS. Deliberately NOT "the floor reached this slice's start": a
               slice's range can hold no source rows at all — raw's oldest row lands partway into the first
               slice, and a collection gap does the same mid-run — so a healthy slice can leave the floor
               short of its own start. Asserting otherwise failed on the first slice of every run, which is
               how the product's per-slice check was found to be measuring the wrong thing. */
            Assert.NotNull(floorDuring);
            Assert.True(floorDuring <= previousFloor,
                $"slice {from:O} moved the coverage floor FORWARD ({previousFloor:O} -> {floorDuring:O}); the backfill must only ever extend coverage backwards");
            previousFloor = floorDuring;
        }

        Assert.Equal(plan.Slices, slicesRun);

        /* The very first slice must have moved the floor at all, or the run did nothing and every assertion
           after this would be vacuous. */
        Assert.True(previousFloor < floorBefore, "the backfill did not move the coverage floor at all");

        /* ── 6. CONVERGENCE, MEASURED FROM DATA — never from the fact that the calls returned. ── */
        var after = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, rollups, ct);
        var floorAfter = after.FloorOf(TimescaleSupport.QueryStatsIntervalHourlyView);

        Assert.NotNull(floorAfter);
        Assert.True(floorAfter <= rawFloor,
            $"after the backfill the rollup must reach at or before raw's oldest row (rollup {floorAfter:O}, raw {rawFloor:O})");

        /* The window that was empty in step 3 now has rows. */
        var rollupRowsAfter = await CountAsync(connection, $"SELECT count(*) FROM collect.{TimescaleSupport.QueryStatsIntervalHourlyView} WHERE bucket >= $1 AND bucket < $2", windowStart, now.AddDays(-6), ct);
        Assert.True(rollupRowsAfter > 0, "the backfilled window must now be materialized");

        /* ── 7. The router goes BACK to the rollup — the backfill restored acceleration rather than stranding
               every old read on raw forever. ── */
        Assert.Equal(
            RetentionTier.Hourly,
            RetentionTierRouter.Resolve(
                now, windowStart, rollups.QueryGrainIntervalHourly, rollups.QueryGrainIntervalDaily,
                after.For(TimescaleSupport.QueryStatsIntervalHourlyView, TimescaleSupport.QueryStatsIntervalDailyView)));

        /* ── 8. And the ARMING GATE now reports safe, which is the whole point: the held raw purge releases
               itself on the next evaluation — the running service's hourly pass since #3812, or the next start
               — with no manual step and nothing armed by the backfill. Run
               through EnsureRetentionPoliciesAsync — the real seam — rather than re-evaluating its predicate,
               because "the gate would say yes" and "the gate DID arm the policy" are different claims. ── */

        /* #3653 LC: RawTierCoverage now requires BOTH query_stats_interval_hourly AND
           query_stats_db_interval_hourly to cover the raw data before arming the purge gate. The test
           only backfilled the former. The db-grain companion must be force-refreshed: the backfill slices
           above consumed query_stats' shared invalidation log, so a plain refresh finds no entries and
           leaves query_stats_db_interval_hourly empty. force=true bypasses the log and scans the source. */
        /* Floor rawOldest to the hourly bucket boundary: refresh_continuous_aggregate interprets window_start
           as "only refresh buckets whose START >= window_start", so passing a sub-hour timestamp skips the
           first bucket (whose start is rawOldest.Truncate(hour)) and leaves query_stats_db_interval_hourly
           one bucket short of raw's oldest row, causing the coverage gate to remain Short. */
        var dbRefreshFrom = new DateTime(rawOldest.Year, rawOldest.Month, rawOldest.Day, rawOldest.Hour, 0, 0, DateTimeKind.Unspecified);
        using (var dbRefresh = new NpgsqlCommand(TimescaleSupport.RefreshContinuousAggregateSql(TimescaleSupport.QueryStatsDbIntervalHourlyView, force: true), connection))
        {
            dbRefresh.Parameters.AddWithValue(dbRefreshFrom);
            await dbRefresh.ExecuteNonQueryAsync(ct);
        }

        await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, ct);

        /* #4299 (d′): query_stats is a raw relation now — its armed verdict is config->>'darling_armed'
           through the shipped RawArmedStateSql, never 'scheduled' (which this pass converges to false
           unconditionally for the three raw jobs). */
        await using var armedRead = new NpgsqlCommand(TimescaleSupport.RawArmedStateSql("query_stats"), connection);
        var armedFlag = await armedRead.ExecuteScalarAsync(ct);

        Assert.True(armedFlag is bool flag && flag, "query_stats' retention policy should have armed itself once the rollup covered it");

        /* Same reason as above: no background worker left running against a database about to vanish. */
        await UnscheduleAllJobsAsync(connection, ct);
    }

    /// <summary>
    /// The other half of the same store shape, and the one a naive fix breaks: a rollup whose floor is later
    /// than the window but where RAW IS SHALLOWER — the healthy, armed-purge store. Falling back to raw there
    /// would return LESS than the rollup does, so the router must stay put. Proven against a live aggregate so
    /// it is the engine's row counts making the case, not our arithmetic.
    /// </summary>
    [Fact]
    public async Task HealthyStore_WhereRawIsShallowerThanTheRollup_DoesNotFallBackToRaw()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #1759 no-regression test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct));
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

        /* Plant history, materialize ALL of it, then purge raw back to a short horizon — the steady state a
           healthy store reaches once its purges are armed. */
        await SeedHourlyQueryStatsAsync(connection, now.AddDays(-HistoryDays), now, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        await RollupBackfill.RunSliceAsync(
            connection, TimescaleSupport.QueryStatsHourlyView, now.Date.AddDays(-HistoryDays - 1), now.Date.AddDays(1), SilentDisclosure(), ct);

        await using (var purge = new NpgsqlCommand("DELETE FROM collect.query_stats WHERE collection_time < $1", connection))
        {
            purge.Parameters.AddWithValue(now.AddDays(-4));
            await purge.ExecuteNonQueryAsync(ct);
        }

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var rollups = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
        var coverage = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, rollups, ct);

        var floor = coverage.FloorOf(TimescaleSupport.QueryStatsHourlyView);
        var rawFloor = coverage.RawOldestOf("query_stats");
        Assert.NotNull(floor);
        Assert.NotNull(rawFloor);
        Assert.True(rawFloor > floor, "raw must be SHALLOWER than the rollup here, or this is not the healthy shape");

        /* A window older than BOTH floors. A naive "window predates the rollup's floor -> use raw" rule sends
           this to a table holding 4 days when the rollup holds 12. */
        var windowStart = now.AddDays(-30);

        Assert.Equal(
            RetentionTier.Hourly,
            RetentionTierRouter.Resolve(
                now, windowStart, rollups.QueryGrainHourly, rollups.QueryGrainDaily,
                coverage.For(TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsDailyView)));

        /* And the row counts say why: the rollup genuinely answers more of that window than raw does. */
        var rollupRows = await CountAsync(connection, $"SELECT count(*) FROM collect.{TimescaleSupport.QueryStatsHourlyView} WHERE bucket >= $1", windowStart, ct);
        var rawRows = await CountAsync(connection, "SELECT count(*) FROM collect.query_stats WHERE collection_time >= $1", windowStart, ct);
        Assert.True(rollupRows > 0);
        Assert.True(rawRows > 0);
    }

    /// <summary>
    /// The VERB itself, end to end, against a real store — not a re-implementation of its loop.
    ///
    /// <para>This is the seam the two tests above cannot reach. They exercise the plan, the slice runner and
    /// the router, all of which can be perfectly correct while the verb that wires them together reports
    /// success it did not earn: the convergence verdict, the preflight refusal and the dry-run stop all live in
    /// the verb body, and every one of them is a place where "returned 0" and "actually did the thing" can
    /// come apart. Driven through <c>BackfillRollupsAsync</c> with a bring-your-own darling.json pointed at the
    /// scratch store, so the exit codes and the operator output are the real ones.</para>
    /// </summary>
    [Fact]
    public async Task BackfillRollupsVerb_DryRunsThenBackfills_AndReportsCoverageItActuallyReached()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live --backfill-rollups verb test.");
        Assert.SkipUnless(OperatingSystem.IsWindows(), "--backfill-rollups is Windows-only (DPAPI store credential in managed mode).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var setup = new NpgsqlConnection(scratch.ConnectionString))
        {
            await setup.OpenAsync(ct);
            await PgMigrations.MigrateAsync(setup, ct);
            Assert.True(await TimescaleSupport.TryEnableAsync(setup, null, ct));
            await TimescaleSupport.ConvertToHypertablesAsync(setup, null, ct);

            var seedNow = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
            await SeedHourlyQueryStatsAsync(setup, seedNow.AddDays(-HistoryDays), seedNow, ct);
            await TimescaleSupport.EnsureContinuousAggregatesAsync(setup, null, ct);
        }

        var configPath = Path.Combine(Path.GetTempPath(), $"darling-1759-{Guid.NewGuid():N}.json");
        await File.WriteAllTextAsync(
            configPath,
            $$"""{"postgres":{"managed":false,"connectionString":{{System.Text.Json.JsonSerializer.Serialize(scratch.ConnectionString)}}},"servers":[]}""",
            ct);

        try
        {
            /* ── DRY RUN: reports a real plan and changes NOTHING. A dry run that quietly materialized would be
                  the worst possible bug in a verb whose entire reason to exist is not writing until it is
                  safe to. ── */
            var dryOut = new StringWriter();
            var dryErr = new StringWriter();
            Assert.Equal(0, await DarlingCliCommands.BackfillRollupsAsync(configPath, dryRun: true, dryOut, dryErr, ct));

            var dryText = dryOut.ToString();
            Assert.Contains("--dry-run: nothing was materialized", dryText, StringComparison.Ordinal);
            Assert.Contains("Free space on the store volume", dryText, StringComparison.Ordinal);
            /* #3653 LC: query_stats_hourly is one of the frozen six and left RollupBackfill.Targets — the verb
               would never plan it and its name would not appear here. query_stats_interval_hourly, its
               interval-honest successor, is still a live target and stands in as the example. */
            Assert.Contains(TimescaleSupport.QueryStatsIntervalHourlyView, dryText, StringComparison.Ordinal);

            await using (var check = new NpgsqlConnection(scratch.ConnectionString))
            {
                await check.OpenAsync(ct);
                var probe = await RollupBackfill.ProbeAsync(check, TimescaleSupport.QueryStatsIntervalHourlyView, "query_stats", "collection_time", ct);

                /* Deliberately NOT "the rollup is still empty": the aggregate's own refresh policy is attached
                   by the ensure sweep and fires immediately, so a trailing window IS materialized by the time a
                   dry run finishes — by the POLICY, not by this verb. The property that actually distinguishes
                   a dry run from a real one is that the pre-existing history is still uncovered. */
                Assert.True(probe.CoverageOldestUtc is null || probe.CoverageOldestUtc > probe.SourceOldestUtc,
                    $"--dry-run materialized the backfill: the rollup already covers back to {probe.CoverageOldestUtc:O} against raw's {probe.SourceOldestUtc:O}");
            }

            /* ── REAL RUN ── */
            var runOut = new StringWriter();
            var runErr = new StringWriter();
            var exit = await DarlingCliCommands.BackfillRollupsAsync(configPath, dryRun: false, runOut, runErr, ct);

            var runText = runOut.ToString();
            Assert.True(exit == 0, $"the verb reported failure.\nSTDOUT:\n{runText}\nSTDERR:\n{runErr}");
            /* SOURCE-relative since #1798: a hierarchical daily's gate measures its HOURLY, not raw, so the
               contract line has to promise what the gate actually checks or DONE keeps over-claiming. */
            Assert.Contains("DONE. Every rollup now covers its own source", runText, StringComparison.Ordinal);

            /* The operator's next step is to WAIT for the hourly re-evaluation (#3812) — the restart is the
               hurry-up option, not the remedy — and the trim race is the one thing that can waste the run. All
               three must be in the output, not just in a comment: the pre-#3812 text made the restart
               mandatory ("NEXT: restart", "Do not delay the restart"), which after #3812 sends an operator to
               restart a service that would have released the hold on its own within the hour. */
            Assert.Contains("hourly store-maintenance tick", runText, StringComparison.Ordinal);
            Assert.Contains("'Retention re-evaluation: 0 policies held", runText, StringComparison.Ordinal);
            Assert.Contains("To arm immediately instead, restart the", runText, StringComparison.Ordinal);
            Assert.DoesNotContain("NEXT: restart", runText, StringComparison.Ordinal);
            Assert.DoesNotContain("Do not delay the restart", runText, StringComparison.Ordinal);
            Assert.Contains("Do not let it wait a day", runText, StringComparison.Ordinal);

            /* ── The verb's success claim is CHECKED against the store, not taken at its word. ── */
            await using (var verify = new NpgsqlConnection(scratch.ConnectionString))
            {
                await verify.OpenAsync(ct);
                foreach (var target in RollupBackfill.Targets)
                {
                    var probe = await RollupBackfill.ProbeAsync(verify, target.View, target.Source, target.SourceTimeColumn, ct);
                    if (probe.SourceOldestUtc is null)
                    {
                        continue; /* that raw table was never seeded, so there was nothing to cover. */
                    }

                    Assert.True(probe.CoverageOldestUtc is not null && probe.CoverageOldestUtc <= probe.SourceOldestUtc,
                        $"the verb reported DONE but {target.View} starts at {probe.CoverageOldestUtc:O}, after {target.RawTable}'s oldest row {probe.SourceOldestUtc:O}");
                }
            }

            /* ── IDEMPOTENT: a second run finds nothing to do and says so, rather than re-materializing. ── */
            var againOut = new StringWriter();
            Assert.Equal(0, await DarlingCliCommands.BackfillRollupsAsync(configPath, dryRun: false, againOut, new StringWriter(), ct));
            Assert.Contains("Every rollup already covers its own source", againOut.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            if (File.Exists(configPath))
            {
                File.Delete(configPath);
            }
        }
    }

    /// <summary>
    /// #1869: the backfill verb converges the THREE-level Query Store chain, not just its first two rungs.
    ///
    /// <para>The verb derives its targets from <c>RollupViews</c>, so a new rollup is picked up automatically —
    /// but "picked up" and "converged" are different claims when the chain gets deeper. A rollup refreshed
    /// before its source reads an empty source, materializes nothing, RETURNS SUCCESS, and burns the
    /// invalidation records that would have let a later pass repair it. Two levels could be ordered by a
    /// boolean; three cannot, and the failure mode is silent. So this drives the real verb over a store holding
    /// real Query Store history and then asks the STORE whether each level actually reaches its own source.</para>
    ///
    /// <para>The day-grain daily is the one that can only pass by ordering: its source is
    /// <c>query_store_stats_interval_daily</c>, which is itself hierarchical, so it converges only if BOTH
    /// hops ran in order.</para>
    /// </summary>
    [Fact]
    public async Task BackfillRollupsVerb_ConvergesTheThreeLevelQueryStoreChain_IncludingTheDayGrainDaily()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #1869 backfill test.");
        Assert.SkipUnless(OperatingSystem.IsWindows(), "--backfill-rollups is Windows-only (DPAPI store credential in managed mode).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var setup = new NpgsqlConnection(scratch.ConnectionString))
        {
            await setup.OpenAsync(ct);
            await PgMigrations.MigrateAsync(setup, ct);
            Assert.True(await TimescaleSupport.TryEnableAsync(setup, null, ct));
            await TimescaleSupport.ConvertToHypertablesAsync(setup, null, ct);

            var seedNow = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
            await SeedHourlyQueryStoreStatsAsync(setup, seedNow.AddDays(-HistoryDays), seedNow, ct);
            await TimescaleSupport.EnsureContinuousAggregatesAsync(setup, null, ct);
        }

        var configPath = Path.Combine(Path.GetTempPath(), $"darling-1869-{Guid.NewGuid():N}.json");
        await File.WriteAllTextAsync(
            configPath,
            $$"""{"postgres":{"managed":false,"connectionString":{{System.Text.Json.JsonSerializer.Serialize(scratch.ConnectionString)}}},"servers":[]}""",
            ct);

        try
        {
            var runOut = new StringWriter();
            var runErr = new StringWriter();
            var exit = await DarlingCliCommands.BackfillRollupsAsync(configPath, dryRun: false, runOut, runErr, ct);

            var runText = runOut.ToString();
            Assert.True(exit == 0, $"the verb reported failure.\nSTDOUT:\n{runText}\nSTDERR:\n{runErr}");
            Assert.Contains("DONE. Every rollup now covers its own source", runText, StringComparison.Ordinal);

            /* Named in the operator's own output, because a rollup the verb silently skipped would still let
               the run report DONE — the contract line is about the targets it PROCESSED. */
            Assert.Contains(TimescaleSupport.QueryStoreStatsIntervalDailyView, runText, StringComparison.Ordinal);
            Assert.Contains(TimescaleSupport.QueryStoreStatsDayGrainDailyView, runText, StringComparison.Ordinal);

            await using var verify = new NpgsqlConnection(scratch.ConnectionString);
            await verify.OpenAsync(ct);

            foreach (var view in new[]
            {
                TimescaleSupport.QueryStoreStatsIntervalHourlyView,
                TimescaleSupport.QueryStoreStatsCorrectedHourlyView,
                TimescaleSupport.QueryStoreStatsCorrectedDailyView,
                TimescaleSupport.QueryStoreStatsIntervalDailyView,
                TimescaleSupport.QueryStoreStatsDayGrainDailyView,
            })
            {
                var target = RollupBackfill.Targets.Single(t => string.Equals(t.View, view, StringComparison.Ordinal));
                var probe = await RollupBackfill.ProbeAsync(verify, target.View, target.Source, target.SourceTimeColumn, ct);

                Assert.True(probe.CoverageOldestUtc is not null,
                    $"{view} materialized nothing at all — the verb reported DONE over an empty rollup.");
                Assert.True(probe.CoverageOldestUtc <= probe.SourceOldestUtc,
                    $"{view} starts at {probe.CoverageOldestUtc:O}, after its source {target.Source}'s oldest row " +
                    $"{probe.SourceOldestUtc:O} — refreshed before its source had been deepened.");
            }
        }
        finally
        {
            if (File.Exists(configPath))
            {
                File.Delete(configPath);
            }
        }
    }

    /// <summary>
    /// THE ESTIMATOR'S CALIBRATION SAMPLE IS BUCKETS, NOT ROWS — proven against a live rollup rather than a
    /// string pin, because the defect was a UNIT mismatch and only real data shows the two diverging.
    ///
    /// <para>The probe's count is divided into the materialized byte size to get bytes-per-BUCKET, then
    /// multiplied by a BUCKET count. <c>count(*)</c> returns ROWS, and a rollup holds one row per
    /// (server, database, query_hash, sql_handle, bucket), so the estimate came out under by the number of
    /// distinct queries per hour — 205x on a 200-row/hour store, worse on a real fleet box, in the one
    /// direction the preflight exists to prevent.</para>
    ///
    /// <para>This is also the mutation: restore <c>count(*)</c> in <c>RollupBackfill.ProbeSql</c> and this goes
    /// red, because the fixture now seeds <see cref="QueriesPerBucket"/> queries per bucket. Against the
    /// original one-row-per-bucket fixture the two counts were identical and nothing could have caught it.</para>
    /// </summary>
    [Fact]
    public async Task Probe_CalibrationSampleCountsBuckets_NotRows()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live estimator-units test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct));
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        await SeedHourlyQueryStatsAsync(connection, now.AddDays(-HistoryDays), now, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var view = TimescaleSupport.QueryStatsHourlyView;
        await RollupBackfill.RunSliceAsync(connection, view, now.Date.AddDays(-2), now.Date.AddDays(1), SilentDisclosure(), ct);

        var probe = await RollupBackfill.ProbeAsync(connection, view, "query_stats", "collection_time", ct);

        /* Both counts in ONE statement, and compared as a RATIO rather than against a separately-timed read:
           the aggregate's refresh policy is live and materializes more buckets between any two round trips,
           which made an equality assertion flake (56 vs 76 on the first run). The ratio is what carries the
           meaning anyway — a bucket count must be far smaller than a row count. */
        await using var counts = new NpgsqlCommand(
            $"SELECT count(*), count(DISTINCT bucket) FROM collect.{view}", connection);
        await using var reader = await counts.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        var rows = reader.GetInt64(0);
        var buckets = reader.GetInt64(1);

        /* The fixture must actually exercise the distinction, or this test is as blind as the one it replaces. */
        Assert.True(buckets > 0);
        Assert.True(rows >= buckets * (QueriesPerBucket - 1),
            $"fixture is too degenerate to catch the units bug: {rows} rows over {buckets} buckets");

        /* THE ASSERTION: what the probe returned is a BUCKET count, so the store's row count must dwarf it.
           Under count(*) the probe returns the row count itself and this fails by a factor of ~12. */
        Assert.True(probe.MaterializedBuckets > 0);
        Assert.True(rows >= probe.MaterializedBuckets * (QueriesPerBucket - 1),
            $"the probe returned {probe.MaterializedBuckets} against {rows} rows / {buckets} buckets — that is a ROW count, not a bucket count, and the disk estimate is under by the queries-per-bucket factor");
    }

    /// <summary>
    /// THE MID-SLICE PREMISE, GUARDED BEHAVIOURALLY.
    ///
    /// <para>Resuming from the measured floor needs no bookkeeping only because a refresh cancelled PART WAY
    /// THROUGH leaves its NEWEST batches committed — the floor lands INSIDE the cancelled range, and the next
    /// run's top slice re-covers the remainder. Every other test in this file kills BETWEEN slices (each slice
    /// taken runs to completion), so until now that premise had nothing watching it: its only evidence was a
    /// reviewer's hand-executed kills, living in a transcript. A true-today engine behaviour with no guard is
    /// the exact shape that produced #1759.</para>
    ///
    /// <para>Deliberately tests the BEHAVIOUR, not the mechanism. It never mentions
    /// <c>refresh_newest_first</c>, so it holds on any engine — including a bring-your-own store too old to
    /// accept the option at all, where a mechanism pin would govern nothing — and a bundled-runtime bump
    /// re-runs it automatically without anyone remembering why it matters. If a future engine flips its
    /// batching order, THIS goes red.</para>
    /// </summary>
    [Fact]
    public async Task MidSliceCancellation_LeavesTheFloorInsideTheRange_WithNoGapsAbove()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live mid-slice cancellation test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct));
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        /* Wide enough that one refresh spans many internal batches, so there is a real window to cancel in. */
        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var rangeFrom = now.Date.AddDays(-CancellationHistoryDays);
        var rangeTo = now.Date;

        await SeedHourlyQueryStatsAsync(connection, rangeFrom, rangeTo, ct);
        /* THE AGGREGATE IS CREATED WITHOUT ITS REFRESH POLICY, deliberately, and this is what makes the test
           mean anything. EnsureContinuousAggregatesAsync also attaches a policy, and TimescaleDB runs a new
           policy's first check IMMEDIATELY — that policy then materializes a trailing window CONTIGUOUSLY,
           which on its own satisfies both assertions below (a floor inside the range, no gaps above it) no
           matter what the cancelled refresh did. Unscheduling it afterwards is too late and does not hold:
           an oldest-first mutation still PASSED, because the policy's own work was carrying the test. Creating
           the aggregate from its own DDL leaves NO policy at all, so the only materialization in this database
           is the one being cancelled here. */
        await using (var create = new NpgsqlCommand(TimescaleSupport.CreateQueryStatsHourlySql, connection))
        {
            await create.ExecuteNonQueryAsync(ct);
        }

        var view = TimescaleSupport.QueryStatsHourlyView;
        Assert.Null(await RollupBackfill.ReadCoverageFloorAsync(connection, view, ct));

        /* ONE wide refresh over the whole un-materialized range, cancelled once it has demonstrably started.
           Polling for the floor to appear is deterministic where a fixed delay would race the machine. */
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(ct);
        await using var refreshConnection = new NpgsqlConnection(scratch.ConnectionString);
        await refreshConnection.OpenAsync(ct);

        var refresh = Task.Run(
            async () =>
            {
                try
                {
                    await RollupBackfill.RunSliceAsync(refreshConnection, view, rangeFrom, rangeTo, SilentDisclosure(), cancellation.Token);
                }
                catch (Exception ex) when (ex is OperationCanceledException or PostgresException or NpgsqlException)
                {
                    /* Cancelling a CALL mid-flight surfaces as any of these; the outcome is measured below. */
                    _ = ex;
                }
            },
            CancellationToken.None);

        DateTime? floorDuring = null;
        for (var attempt = 0; attempt < 200 && !refresh.IsCompleted; attempt++)
        {
            floorDuring = await RollupBackfill.ReadCoverageFloorAsync(connection, view, ct);
            if (floorDuring is not null && floorDuring > rangeFrom)
            {
                break;
            }

            await Task.Delay(25, ct);
        }

        await cancellation.CancelAsync();
        await refresh;

        /* Release the cancelled backend BEFORE anything else touches this store. `await refresh` returns when
           the client-side task completes, which is not the same instant the server-side backend finishes
           unwinding an aborted CALL — and ScratchPostgres ends this test with DROP DATABASE ... WITH (FORCE).
           Closing explicitly removes that window rather than relying on disposal order to close it. This is
           the same lifecycle-hardening the arming tests got: the shared-store flake class on this rig is
           connection-level, so a test that deliberately aborts a statement should not leave the cleanup to
           chance. */
        await refreshConnection.CloseAsync();

        var floor = await RollupBackfill.ReadCoverageFloorAsync(connection, view, ct);

        Assert.True(floor is not null,
            "the cancelled refresh materialized nothing at all, so the mid-slice property was never exercised");

        /* (b) ZERO gaps above the floor — the newest batches committed contiguously downward, which is exactly
               what lets the next run resume from this floor without leaving a hole behind it. Measured FIRST,
               and against raw itself, because it is the property; the vacuity check below only interprets it. */
        var gaps = await CountAsync(connection, $@"
SELECT count(*)
FROM (
    SELECT DISTINCT time_bucket('1 hour', collection_time) AS b
    FROM collect.query_stats
    WHERE collection_time >= $1 AND collection_time < $2
) AS src
WHERE NOT EXISTS (SELECT 1 FROM collect.{view} AS h WHERE h.bucket = src.b)",
            floor!.Value, rangeTo, ct);

        Assert.True(gaps == 0,
            $"{gaps} bucket(s) are missing ABOVE the coverage floor {floor:O} — this engine did NOT commit its " +
            "batches newest-first, so a cancelled refresh leaves holes behind the floor. Resuming from the " +
            "measured floor would skip them and report success over a gap, which is #1759's data-loss shape. " +
            "The backfill needs per-slice verification before it can trust this engine's resume.");

        /* (a) The floor landed INSIDE the cancelled range. Checked AFTER the gap test on purpose: floor-at-the-
               bottom WITH gaps is a flipped engine (reported above, accurately), while floor-at-the-bottom with
               NO gaps just means the refresh finished before it could be caught — a vacuous pass, which must
               fail loudly and say what to widen rather than bank an assertion that proved nothing. */
        Assert.True(floor > rangeFrom,
            $"the refresh completed the whole range before cancellation (floor {floor:O} reached the bottom {rangeFrom:O}) " +
            $"with no gaps, so nothing mid-flight was exercised — widen {nameof(CancellationHistoryDays)} rather than " +
            "accepting a vacuous pass");

        Assert.True(floor < rangeTo, $"floor {floor:O} is not inside the cancelled range [{rangeFrom:O}, {rangeTo:O})");
    }

    /// <summary>History for the cancellation pin: wide enough that one refresh spans many internal batches, so
    /// there is a real window to cancel inside. Widen it if that test ever reports the refresh completing
    /// first.</summary>
    private const int CancellationHistoryDays = 120;

    /// <summary>
    /// THE INTERRUPTED-BACKFILL ACCEPTANCE — the reviewer's data-loss proof, inverted.
    ///
    /// <para>What was proven against a live store before the fix: slices ran oldest-first, so the FIRST slice
    /// drove <c>min(bucket)</c> to its final value. Killing the run at slice 32 of 43 left 47 of 264 buckets
    /// missing in the un-run region; the re-run re-planned from that already-bottomed floor, printed "nothing
    /// to do — DONE" and exited 0; and the #1680 arming gate, reading the very same floor, then ARMED the
    /// 4-day raw purge over a window whose only surviving copy was the raw rows it was about to drop.</para>
    ///
    /// <para>This test asserts the inverse at every step: a partial run must NOT look complete, the gate must
    /// REFUSE while it is partial, and a resumed run must finish with ZERO missing buckets across the whole
    /// originally-planned range. It is also the mutation — restore ascending order in
    /// <c>RollupBackfill.Slices</c> and the partial-run assertions go red.</para>
    /// </summary>
    [Fact]
    public async Task InterruptedBackfill_DoesNotLookComplete_AndTheArmingGateRefusesUntilItGenuinelyIs()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live interrupted-backfill test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct));
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        await SeedHourlyQueryStatsAsync(connection, now.AddDays(-HistoryDays), now, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        /* #3653 LC: query_stats_hourly is one of the frozen six now — nothing ever advances its watermark
           again, so backfilling IT can never arm the raw purge gate. RawTierCoverage arms query_stats'
           retention off the interval-honest successor's coverage instead (RequireSuccessorOf), so that is
           the view this test's arming-gate assertions have to drive. */
        var view = TimescaleSupport.QueryStatsIntervalHourlyView;
        var probe = await RollupBackfill.ProbeAsync(connection, view, "query_stats", "collection_time", ct);
        var plan = RollupBackfill.Plan(
            view, probe.SourceOldestUtc, probe.CoverageOldestUtc,
            probe.MaterializedBuckets, probe.MaterializedBytes, rawBytes: 0, bucketWidth: TimeSpan.FromHours(1));

        Assert.False(plan.IsComplete);
        var allSlices = RollupBackfill.Slices(plan.FromUtc, plan.ToUtc).ToArray();
        Assert.True(allSlices.Length >= 4, $"need several slices to interrupt meaningfully, planned {allSlices.Length}");

        /* ── THE KILL: run only the first two-thirds of the slices, then stop dead. ── */
        var killAt = (allSlices.Length * 2) / 3;
        foreach (var (from, to) in allSlices.Take(killAt))
        {
            await RollupBackfill.RunSliceAsync(connection, view, from, to, SilentDisclosure(), ct);
        }

        /* (1) The partial run must not LOOK complete. This is the assertion that was false before: under
               oldest-first the floor had already reached the bottom after slice one. */
        var partialFloor = await RollupBackfill.ReadCoverageFloorAsync(connection, view, ct);
        Assert.NotNull(partialFloor);
        Assert.True(partialFloor > probe.SourceOldestUtc,
            $"an interrupted backfill reports coverage {partialFloor:O} at or below raw's oldest {probe.SourceOldestUtc:O} — it looks COMPLETE, which is the false-DONE that lets the arming gate destroy data");

        /* (2) A re-plan from the measured floor must still have work to do — not "nothing to do". */
        var partialProbe = await RollupBackfill.ProbeAsync(connection, view, "query_stats", "collection_time", ct);
        var resumePlan = RollupBackfill.Plan(
            view, partialProbe.SourceOldestUtc, partialProbe.CoverageOldestUtc,
            partialProbe.MaterializedBuckets, partialProbe.MaterializedBytes, rawBytes: 0, bucketWidth: TimeSpan.FromHours(1));

        Assert.False(resumePlan.IsComplete, "the resumed plan reported nothing to do while buckets were still missing");

        /* (3) THE ARMING GATE MUST REFUSE while coverage is partial — the step that turns a hole into data
               loss. Run the REAL policy sweep, not its predicate, and read the job's scheduled flag. */
        await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, ct);
        Assert.False(await RawArmedAsync(connection, "query_stats", ct));

        /* ── THE RESUME: finish the remaining slices from the measured floor. ── */
        foreach (var (from, to) in RollupBackfill.Slices(resumePlan.FromUtc, resumePlan.ToUtc))
        {
            await RollupBackfill.RunSliceAsync(connection, view, from, to, SilentDisclosure(), ct);
        }

        /* (4) ZERO missing buckets across the WHOLE originally-planned range — measured against raw itself,
               not against our own arithmetic about what we intended to do. */
        var missing = await CountAsync(connection, $@"
SELECT count(*)
FROM (
    SELECT DISTINCT time_bucket('1 hour', collection_time) AS b
    FROM collect.query_stats
    WHERE collection_time >= $1 AND collection_time < $2
) AS src
WHERE NOT EXISTS (SELECT 1 FROM collect.{view} AS h WHERE h.bucket = src.b)",
            plan.FromUtc, plan.ToUtc, ct);

        Assert.True(missing == 0, $"{missing} bucket(s) raw holds are still missing from {view} after a resumed backfill");

        /* (5) And only NOW does the gate arm — coverage genuinely reaches raw. */
        Assert.True(await RollupBackfill.ReadCoverageFloorAsync(connection, view, ct) <= probe.SourceOldestUtc);

        /* #3653 LC: RawTierCoverage for query_stats now requires BOTH query_stats_interval_hourly AND
           query_stats_db_interval_hourly. Force-refresh the db-grain companion so the gate can arm; the
           force is needed because the backfill slices above consumed query_stats' shared invalidation log,
           leaving the db-grain companion with no entries to process on a plain refresh. Floor to the hourly
           bucket boundary: refresh_continuous_aggregate window_start excludes buckets whose START < start,
           so a sub-hour timestamp would skip the first bucket and leave coverage one bucket short of raw. */
        var dbRefreshFrom = new DateTime(plan.FromUtc.Year, plan.FromUtc.Month, plan.FromUtc.Day, plan.FromUtc.Hour, 0, 0, DateTimeKind.Unspecified);
        using (var dbRefresh = new NpgsqlCommand(TimescaleSupport.RefreshContinuousAggregateSql(TimescaleSupport.QueryStatsDbIntervalHourlyView, force: true), connection))
        {
            dbRefresh.Parameters.AddWithValue(dbRefreshFrom);
            await dbRefresh.ExecuteNonQueryAsync(ct);
        }

        await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, ct);
        Assert.True(await RawArmedAsync(connection, "query_stats", ct));

        /* Stand the scheduler down before this scratch database is force-dropped underneath it. */
        await UnscheduleAllJobsAsync(connection, ct);
    }

    /// <summary>
    /// THE VERB'S REFUSAL BRANCH, end to end. Until now only the pure <c>HasRoom</c> arithmetic and the
    /// refusal TEXT were covered — never the wiring that has to carry a refusal out of the planner, onto
    /// stderr, and into a non-zero exit without ever printing DONE.
    ///
    /// <para>Driven through the sanity clamp because it is the one refusal reachable from real data: a single
    /// stray ancient timestamp. Observed live producing a 739,825-slice plan that burned CPU for days before
    /// materializing anything. The properties under test are the wiring's, not the clamp's — a refusal must
    /// not be reported as a skip, must not leave the operator believing every rollup is covered, and must
    /// exit non-zero.</para>
    /// </summary>
    [Fact]
    public async Task BackfillRollupsVerb_RefusesAnAbsurdPlan_WithoutClaimingSuccess()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live verb-refusal test.");
        Assert.SkipUnless(OperatingSystem.IsWindows(), "--backfill-rollups is Windows-only (DPAPI store credential in managed mode).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var setup = new NpgsqlConnection(scratch.ConnectionString))
        {
            await setup.OpenAsync(ct);
            await PgMigrations.MigrateAsync(setup, ct);
            Assert.True(await TimescaleSupport.TryEnableAsync(setup, null, ct));
            await TimescaleSupport.ConvertToHypertablesAsync(setup, null, ct);

            var seedNow = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
            await SeedHourlyQueryStatsAsync(setup, seedNow.AddDays(-2), seedNow, ct);

            /* ONE corrupt row, which is all it takes. */
            await using var poison = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count)
VALUES ($1, $2, $3, 'backfill-e2e', 'TestDb', decode(md5('poison'), 'hex'), decode(md5('h'), 'hex'), 1, 1, 1)", setup);
            poison.Parameters.AddWithValue(1L);
            poison.Parameters.AddWithValue(new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Unspecified));
            poison.Parameters.AddWithValue(TestServerId);
            await poison.ExecuteNonQueryAsync(ct);

            await TimescaleSupport.EnsureContinuousAggregatesAsync(setup, null, ct);
        }

        var configPath = Path.Combine(Path.GetTempPath(), $"darling-1759-refuse-{Guid.NewGuid():N}.json");
        await File.WriteAllTextAsync(
            configPath,
            $$"""{"postgres":{"managed":false,"connectionString":{{System.Text.Json.JsonSerializer.Serialize(scratch.ConnectionString)}}},"servers":[]}""",
            ct);

        try
        {
            var output = new StringWriter();
            var error = new StringWriter();
            var exit = await DarlingCliCommands.BackfillRollupsAsync(configPath, dryRun: false, output, error, ct);

            var combined = output + error.ToString();

            Assert.True(exit == 1, $"a refused plan must exit non-zero.\nSTDOUT:\n{output}\nSTDERR:\n{error}");
            Assert.Contains("[REFUSED]", combined, StringComparison.Ordinal);
            Assert.Contains("1970-01-01", combined, StringComparison.Ordinal);
            Assert.Contains("corrupt row", combined, StringComparison.Ordinal);

            /* The properties that make it a refusal rather than a skip. */
            Assert.DoesNotContain("DONE. Every rollup now covers its own source", combined, StringComparison.Ordinal);
            Assert.DoesNotContain("Every rollup already covers its own source", combined, StringComparison.Ordinal);
        }
        finally
        {
            if (File.Exists(configPath))
            {
                File.Delete(configPath);
            }
        }
    }

    /// <summary>
    /// Unschedules every background job in this scratch database.
    ///
    /// <para>These tests ARM real retention policies, because the gate's decision is the thing under test —
    /// but an armed policy means TimescaleDB's scheduler will launch a background worker to run
    /// <c>drop_chunks</c> against a database <see cref="ScratchPostgres"/> is about to
    /// <c>DROP … WITH (FORCE)</c>. Leaving a worker running against a vanishing database is a hazard I
    /// introduced with the arming assertions, and it costs one statement to remove.</para>
    ///
    /// <para>Called after the assertions, so it takes nothing away from them: what is under test is whether
    /// the gate DECIDED to arm, which has already been read from the job catalog by then. Nothing here needs
    /// the purge to actually execute.</para>
    ///
    /// <para>Best-effort by design — a cleanup failure must never fail a passing test, which would invert the
    /// signal.</para>
    /// </summary>
    private static async Task UnscheduleAllJobsAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        try
        {
            await using var command = new NpgsqlCommand(
                "SELECT alter_job(job_id, scheduled => false) FROM timescaledb_information.jobs WHERE job_id >= 1000",
                connection);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _ = ex;
        }
    }


    /// <summary>
    /// A disclosure sink for tests that are not exercising the pre-2.21 degrade. It FAILS if it ever fires:
    /// on the bundled 2.28.1 these tests must take the explicit-options path, so a disclosure here means the
    /// store silently fell back and every "the option is carried" claim in this file would be hollow.
    /// </summary>
    private static RefreshDisclosure SilentDisclosure() =>
        new(message => Assert.Fail($"the refresh degraded unexpectedly on a 2.28.1 store: {message}"));

    /// <summary>
    /// #1798: a hierarchical daily converges to its HOURLY, not to raw — and on a healthy store those are
    /// different instants by weeks.
    ///
    /// <para>The shape that exposed it: raw purges already armed, so raw holds a few days while the hourly
    /// holds far more. The #1680 gate for an HOURLY-tier policy asks whether the daily covers what the HOURLY
    /// holds, but the verb converged every rollup to RAW's oldest row — so the daily stopped weeks short, the
    /// gate correctly refused, and the verb printed DONE with its zero-held promise. Observed on a lab box as
    /// <c>16/16 retention policies in place, 14 armed, 2 held</c> immediately after a fully-DONE run.</para>
    ///
    /// <para>This asserts the fix at the level the defect lives: the daily's floor reaches the HOURLY's oldest
    /// bucket, which is strictly older than raw's oldest row, and the hourly-tier policy then ARMS. The
    /// mutation is converging to raw instead — the shipped behaviour — which leaves the daily short and the
    /// policy held.</para>
    ///
    /// <para>NO-POLICY FIXTURE, deliberately: the ensure sweep's refresh policy runs immediately and
    /// materializes a trailing window contiguously, which can satisfy a coverage assertion on its own. That
    /// trap already made a pin in this file vacuous twice, so the aggregates here are created from their own
    /// DDL and the only materialization is the one under test.</para>
    /// </summary>
    [Fact]
    public async Task HierarchicalDaily_ConvergesToItsHourly_NotToRaw_SoTheHourlyTierPolicyArms()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #1798 source-convergence test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct));
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var historyFrom = now.Date.AddDays(-HistoryDays);

        await SeedHourlyQueryStatsAsync(connection, historyFrom, now, ct);

        /* Aggregates WITHOUT their refresh policies — see the remarks.
           #3653 LC: query_stats_hourly/query_stats_daily are two of the frozen six now, and left
           RollupBackfill.Targets entirely — the Single() lookup below would throw. The interval-honest
           successor pair is still hierarchical the same way (daily reads the hourly's bucket, not raw) and
           still a live Targets entry, so it stands in as the example. */
        foreach (var createSql in new[] { TimescaleSupport.CreateQueryStatsIntervalHourlySql, TimescaleSupport.CreateQueryStatsIntervalDailySql })
        {
            await using var create = new NpgsqlCommand(createSql, connection);
            await create.ExecuteNonQueryAsync(ct);
        }

        var hourly = TimescaleSupport.QueryStatsIntervalHourlyView;
        var daily = TimescaleSupport.QueryStatsIntervalDailyView;

        /* The hourly holds the WHOLE history... */
        await RollupBackfill.RunSliceAsync(connection, hourly, historyFrom, now.Date, SilentDisclosure(), ct);

        /* ...and then raw is purged back to a short horizon, which is the steady state of a healthy store and
           the configuration where raw and the hourly stop being interchangeable. */
        await using (var purge = new NpgsqlCommand("DELETE FROM collect.query_stats WHERE collection_time < $1", connection))
        {
            purge.Parameters.AddWithValue(now.AddDays(-4));
            await purge.ExecuteNonQueryAsync(ct);
        }

        var target = RollupBackfill.Targets.Single(t => string.Equals(t.View, daily, StringComparison.Ordinal));
        Assert.Equal(hourly, target.Source);

        var probe = await RollupBackfill.ProbeAsync(connection, daily, target.Source, target.SourceTimeColumn, ct);
        var rawOldest = await ReadScalarInstantAsync(connection, "SELECT min(collection_time) FROM collect.query_stats", ct);

        Assert.NotNull(probe.SourceOldestUtc);
        Assert.NotNull(rawOldest);
        Assert.True(probe.SourceOldestUtc < rawOldest,
            $"the hourly must out-reach raw for this to exercise #1798 (hourly {probe.SourceOldestUtc:O}, raw {rawOldest:O})");

        /* Back fill the daily against the SOURCE-relative plan, exactly as the verb does. */
        var plan = RollupBackfill.Plan(
            daily, probe.SourceOldestUtc, probe.CoverageOldestUtc,
            probe.MaterializedBuckets, probe.MaterializedBytes, rawBytes: 0, bucketWidth: TimeSpan.FromDays(1));

        Assert.False(plan.IsComplete);
        foreach (var (from, to) in RollupBackfill.Slices(plan.FromUtc, plan.ToUtc))
        {
            await RollupBackfill.RunSliceAsync(connection, daily, from, to, SilentDisclosure(), ct);
        }

        /* THE ASSERTION: the daily reached the HOURLY's oldest bucket, which is older than raw's oldest row.
           Converging to raw would stop at rawOldest and fail this by the whole purged span. */
        var dailyFloor = await RollupBackfill.ReadCoverageFloorAsync(connection, daily, ct);
        Assert.NotNull(dailyFloor);
        Assert.True(dailyFloor <= probe.SourceOldestUtc,
            $"the daily stopped at {dailyFloor:O} but its source hourly reaches {probe.SourceOldestUtc:O} — converged to raw, not to source (#1798)");
        Assert.True(dailyFloor < rawOldest,
            $"the daily only reached {dailyFloor:O}, which is no older than raw ({rawOldest:O}) — the hourly-tier gate measures the HOURLY, so this leaves it held");

        /* And the gate agrees: the hourly-tier policy ARMS, which is the operator-visible promise. */
        await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, ct);
        Assert.Equal(1L, await CountAsync(connection, ArmedHourlyPolicySql, ct));

        await UnscheduleAllJobsAsync(connection, ct);
    }

    /// <summary>Is query_stats_interval_hourly's retention policy ARMED? The #1798 observable — it is gated on
    /// the DAILY covering the hourly, which is the comparison the backfill now converges to.</summary>
    private const string ArmedHourlyPolicySql = @"
SELECT count(*)
FROM timescaledb_information.jobs AS j
WHERE j.proc_name = 'policy_retention'
AND   j.hypertable_schema = 'collect'
AND   j.hypertable_name = 'query_stats_interval_hourly'
AND   j.scheduled";

    /// <summary>One nullable timestamp, for reading a relation's oldest instant.</summary>
    private static async Task<DateTime?> ReadScalarInstantAsync(
        NpgsqlConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(cancellationToken) is DateTime instant ? instant : null;
    }

    /// <summary>#4299 (d′): is query_stats' raw retention policy ARMED? query_stats is a raw relation now —
    /// its verdict is config->>'darling_armed' through the shipped RawArmedStateSql, never 'scheduled' (which
    /// this pass converges to false unconditionally for the three raw jobs).</summary>
    private static async Task<bool> RawArmedAsync(NpgsqlConnection connection, string relation, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(TimescaleSupport.RawArmedStateSql(relation), connection);
        var value = await command.ExecuteScalarAsync(ct);
        return value is bool flag && flag;
    }

    /// <summary>
    /// Distinct queries per hourly bucket. <b>MUST stay above 1.</b>
    ///
    /// <para>Both fixtures originally seeded ONE row per bucket, which made rows and buckets the same number
    /// and rendered the whole suite blind to the estimator defect it should have caught: the probe counted
    /// ROWS and the arithmetic multiplied BUCKETS, an error factor of exactly this constant, and at 1 it is
    /// invisible. A degenerate fixture is not a small test, it is a test of a shape the product never
    /// meets.</para>
    /// </summary>
    private const int QueriesPerBucket = 12;

    /// <summary><see cref="QueriesPerBucket"/> query_stats rows per hour across [from, to), each a distinct
    /// query_hash so the rollup materializes one row per (query, bucket) — the real shape, where rows far
    /// outnumber buckets.</summary>
    private static async Task SeedHourlyQueryStatsAsync(
        NpgsqlConnection connection, DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count)
SELECT
    (extract(epoch FROM g)::bigint * 100 + q),
    g,
    $3,
    'backfill-e2e',
    'TestDb',
    decode(md5('q' || q::text), 'hex'),
    decode(md5('handle' || q::text), 'hex'),
    1000,
    2000,
    10
FROM generate_series($1::timestamp, $2::timestamp, INTERVAL '1 hour') AS g
CROSS JOIN generate_series(1, $4::int) AS q", connection);

        insert.Parameters.AddWithValue(from);
        insert.Parameters.AddWithValue(to);
        insert.Parameters.AddWithValue(TestServerId);
        insert.Parameters.AddWithValue(QueriesPerBucket);
        await insert.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary><see cref="QueriesPerBucket"/> Query Store rows per hour across [from, to), each a distinct
    /// query/plan and each carrying a real <c>runtime_stats_interval_id</c> — the identity the whole corrected
    /// chain keys on, so a seed without it would collapse every row into one interval and make the deeper
    /// levels trivially converge.</summary>
    private static async Task SeedHourlyQueryStoreStatsAsync(
        NpgsqlConnection connection, DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash,
     query_id, plan_id, execution_type_desc, replica_role,
     runtime_stats_interval_id, interval_start_time_utc, first_execution_time,
     execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us)
SELECT
    (extract(epoch FROM g)::bigint * 100 + q),
    g,
    $3,
    'backfill-e2e',
    'TestDb',
    'dbo.Proc' || q::text,
    '0x' || lpad(to_hex(q), 16, '0'),
    q,
    q + 1000,
    'Regular',
    'PRIMARY',
    extract(epoch FROM date_trunc('hour', g))::bigint,
    date_trunc('hour', g),
    date_trunc('hour', g),
    q * 10,
    1200,
    600,
    9000,
    4000
FROM generate_series($1::timestamp, $2::timestamp, INTERVAL '1 hour') AS g
CROSS JOIN generate_series(1, $4::int) AS q", connection);

        insert.Parameters.AddWithValue(from);
        insert.Parameters.AddWithValue(to);
        insert.Parameters.AddWithValue(TestServerId);
        insert.Parameters.AddWithValue(QueriesPerBucket);
        await insert.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<long> CountAsync(
        NpgsqlConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (long)(await command.ExecuteScalarAsync(cancellationToken))!;
    }

    private static async Task<long> CountAsync(
        NpgsqlConnection connection, string sql, DateTime p1, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(p1);
        return (long)(await command.ExecuteScalarAsync(cancellationToken))!;
    }

    private static async Task<long> CountAsync(
        NpgsqlConnection connection, string sql, DateTime p1, DateTime p2, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(p1);
        command.Parameters.AddWithValue(p2);
        return (long)(await command.ExecuteScalarAsync(cancellationToken))!;
    }
}
