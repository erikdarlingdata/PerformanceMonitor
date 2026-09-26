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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3653 (A6, lane LC-b): live proofs for the freeze (LC) against a real TimescaleDB store — the four claims a
/// unit/pin test cannot reach because they need actual chunks, jobs and catalog rows: (1) dropping a frozen
/// legacy hourly's chunks (its own retention, unchanged by the freeze) never touches its already-materialized
/// frozen daily; (2) no start-path converge ever re-adds a refresh policy to any of the frozen six
/// (<see cref="TimescaleSupport.FrozenRollupAggregates"/>), while every other continuous aggregate keeps one;
/// (3) the raw purge arms off the successor hourlies' coverage (<see cref="TimescaleSupport.RawTierCoverage"/>)
/// even though the legacy hourlies it used to key on are empty, and stays held when a coverage relation falls
/// short; (4) a frozen daily's compression policy drains only once every chunk it will ever hold is compressed,
/// and never touches a non-frozen daily's policy.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here goes through
   ScratchPostgres.CreateAsync, which reaches DARLING_TEST_PG only to CREATE and DROP its own database and then
   works entirely inside it. It never touches the shared database's tables, so it cannot race the live
   collection, and serializing it would be pure slowdown. Leave it out; this comment is here so the next sweep
   does not "fix" it. */
public sealed class FrozenRollupLiveTests
{
    /// <summary>Distinctive fake id — a real server_id is a storage-name hash, never in this range.</summary>
    private const int ServerId = -936554;
    private const string ServerName = "a6-lcb-frozen-rollup";
    private const string Db = "FrozenDb";

    /// <summary>Fixed anchor date (never wall clock): a Monday, chosen only so the seeded days are unambiguous.</summary>
    private static readonly DateTime D0 = new(2026, 2, 2, 0, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public async Task DroppingFrozenHourlyChunks_LeavesItsFrozenDailyUnchanged()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live A6 freeze test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live A6 freeze test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            /* 1-day materialization chunks on the frozen hourly, set BEFORE any refresh creates one, so a
               day-aligned cutoff below drops exactly the days it names and none of the days it does not. */
            await SetMaterializationChunkIntervalAsync(connection, TimescaleSupport.QueryStatsHourlyView, "1 day", ct);

            /* A pre-#3653 store's own history: the legacy hourly carried a real refresh policy, like any other
               aggregate, before LC froze it off the grid. */
            await using (var addPolicy = new NpgsqlCommand(
                TimescaleSupport.AddContinuousAggregatePolicySql(
                    TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.HourlyRefreshStartOffset,
                    TimescaleSupport.HourlyRefreshScheduleInterval, TimescaleSupport.HourlyRefreshScheduleInterval,
                    phaseMinutes: null),
                connection))
            {
                await addPolicy.ExecuteNonQueryAsync(ct);
            }

            for (var day = 0; day < 10; day++)
            {
                var at = D0.AddDays(day);
                await InsertQueryStatsAsync(connection, at, $"LCBQ{day}A", 1000 + day, 10, 3600, ct);
                await InsertQueryStatsAsync(connection, at.AddHours(2), $"LCBQ{day}B", 2000 + day, 20, 3600, ct);
            }

            var d10 = D0.AddDays(10);
            var cutoff = D0.AddDays(5);

            /* Materialize the legacy hourly and then its daily — the pre-freeze history a real service would
               have built over time through the policy just added above. */
            await RefreshAsync(connection, TimescaleSupport.QueryStatsHourlyView, D0, d10, ct);
            await RefreshAsync(connection, TimescaleSupport.QueryStatsDailyView, D0, d10, ct);

            var before = await ReadDailyTotalsAsync(connection, TimescaleSupport.QueryStatsDailyView, ServerId, D0, cutoff, ct);
            Assert.Equal(5, before.Count);

            /* Run the start path so the freeze applies: detaches the policy added above. */
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

            /* The legacy hourly's OWN retention (unchanged by the freeze), dropping its old chunks. */
            await using (var drop = new NpgsqlCommand(
                $"SELECT drop_chunks('collect.{TimescaleSupport.QueryStatsHourlyView}', older_than => $1::timestamp)", connection))
            {
                drop.Parameters.AddWithValue(cutoff);
                await drop.ExecuteNonQueryAsync(ct);
            }

            /* Confirms the drop actually had teeth against the hourly, so the unchanged-daily assertion below
               is not vacuous. */
            await using (var hourlyLeft = new NpgsqlCommand(
                $"SELECT count(*) FROM collect.{TimescaleSupport.QueryStatsHourlyView} WHERE server_id = $1 AND bucket < $2", connection))
            {
                hourlyLeft.Parameters.AddWithValue(ServerId);
                hourlyLeft.Parameters.AddWithValue(cutoff);
                Assert.Equal(0L, Convert.ToInt64(await hourlyLeft.ExecuteScalarAsync(ct)));
            }

            /* Every start step that could refresh a rollup. RollupBackfill's targets are CLI-only
               (DarlingCliCommands' rollup-backfill verb) — the service start path never runs a backfill slice
               on its own, so there is no third step to run here. */
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
            await TimescaleSupport.RepairMaterializationHolesAsync(connection, null, DateTime.UtcNow, ct);

            var after = await ReadDailyTotalsAsync(connection, TimescaleSupport.QueryStatsDailyView, ServerId, D0, cutoff, ct);
            Assert.Equal(before, after);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }

    [Fact]
    public async Task StartPathRunTwice_NeverReAddsAFrozenRefreshPolicy_AndEveryOtherAggregateKeepsOne()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live A6 freeze test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live A6 freeze test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            /* Two of the six frozen rollups get a hand-attached policy first, mimicking a store that took a
               build before #3653's LC — the only shape that can prove the removal below actually runs, rather
               than there simply never having been a policy to find. */
            await using (var addHourly = new NpgsqlCommand(
                TimescaleSupport.AddContinuousAggregatePolicySql(
                    TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.HourlyRefreshStartOffset,
                    TimescaleSupport.HourlyRefreshScheduleInterval, TimescaleSupport.HourlyRefreshScheduleInterval,
                    phaseMinutes: null),
                connection))
            {
                await addHourly.ExecuteNonQueryAsync(ct);
            }
            await using (var addDaily = new NpgsqlCommand(
                TimescaleSupport.AddContinuousAggregatePolicySql(
                    TimescaleSupport.QueryStatsDailyView, TimescaleSupport.DailyRefreshStartOffset,
                    TimescaleSupport.DailyRefreshScheduleInterval, TimescaleSupport.DailyRefreshScheduleInterval,
                    phaseMinutes: null),
                connection))
            {
                await addDaily.ExecuteNonQueryAsync(ct);
            }

            /* The start path, twice — the second run is the converge a restart performs. */
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

            var scheduled = await ReadScheduledRefreshViewsAsync(connection, ct);

            foreach (var (_, view) in TimescaleSupport.FrozenRollupAggregates)
            {
                Assert.DoesNotContain(view, scheduled);
            }

            var everyOther = TimescaleSupport.HourlyAggregates.Select(a => a.View)
                .Concat(TimescaleSupport.DailyAggregates.Select(a => a.View))
                .Concat(TimescaleSupport.BaselineAggregates.Select(a => a.View))
                .Concat(TimescaleSupport.OffGridAggregates.Select(a => a.View));

            foreach (var view in everyOther)
            {
                Assert.Contains(view, scheduled);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }

    [Fact]
    public async Task RawPurge_ArmsOffSuccessorHourlyCoverage_NotTheEmptyLegacyOnes()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live A6 freeze test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live A6 freeze test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            /* Read from the registry rather than naming the successors by hand — correct whether the
               query_stats row carries one coverage relation or (once LC-a4 lands) two. */
            var queryStatsCoverage = TimescaleSupport.RawTierCoverage.Single(r => string.Equals(r.Relation, "query_stats", StringComparison.Ordinal));
            var procedureStatsCoverage = TimescaleSupport.RawTierCoverage.Single(r => string.Equals(r.Relation, "procedure_stats", StringComparison.Ordinal));

            var d0 = D0;
            var d1 = D0.AddDays(1);
            var d2 = D0.AddDays(2);

            for (var day = 0; day < 2; day++)
            {
                var at = D0.AddDays(day);
                await InsertQueryStatsAsync(connection, at, $"LCBRAWQ{day}", 1000, 10, 3600, ct);
                await InsertProcedureStatsAsync(connection, at, $"lcb_raw_proc_{day}", 900, 9, 3600, ct);
            }

            /* procedure_stats' successor coverage is refreshed fully from raw's oldest row — Covered. */
            foreach (var view in procedureStatsCoverage.Coverage)
            {
                await RefreshAsync(connection, view, d0, d2, ct);
            }

            /* query_stats' successor coverage is refreshed SHORT of raw's oldest row (missing D0) — Short. The
               legacy hourly stays empty throughout, which is exactly the state the freeze leaves it in. */
            foreach (var view in queryStatsCoverage.Coverage)
            {
                await RefreshAsync(connection, view, d1, d2, ct);
            }

            /* The legacy hourlies are never named above, never refreshed, and stay materialized empty — exactly
               the state the freeze leaves them in — so this pins that arming below is NOT reading them: if
               RawTierCoverage's query_stats/procedure_stats rows ever pointed back at these instead of the
               successors, the refresh loops above would populate them (they refresh whatever Coverage names)
               and this assertion would catch it. */
            Assert.Equal(0L, await CountRowsAsync(connection, TimescaleSupport.QueryStatsHourlyView, ct));
            Assert.Equal(0L, await CountRowsAsync(connection, TimescaleSupport.ProcedureStatsHourlyView, ct));

            await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, ct);

            Assert.Equal(false, await IsRetentionScheduledAsync(connection, "query_stats", ct));
            Assert.Equal(true, await IsRetentionScheduledAsync(connection, "procedure_stats", ct));

            /* The contrast, on the SAME relation: backfill the missing day and re-evaluate. Coverage now
               reaches raw's oldest row, and the hold releases on the very next pass, with no restart (#1877). */
            foreach (var view in queryStatsCoverage.Coverage)
            {
                await RefreshAsync(connection, view, d0, d1, ct);
            }

            await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, ct);

            Assert.Equal(true, await IsRetentionScheduledAsync(connection, "query_stats", ct));
            Assert.Equal(true, await IsRetentionScheduledAsync(connection, "procedure_stats", ct));
            Assert.Equal(0L, await CountRowsAsync(connection, TimescaleSupport.QueryStatsHourlyView, ct));
            Assert.Equal(0L, await CountRowsAsync(connection, TimescaleSupport.ProcedureStatsHourlyView, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }

    [Fact]
    public async Task FrozenDailyCompressionPolicy_DrainsOnlyOnceEveryChunkIsCompressed()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live A6 freeze test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live A6 freeze test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            /* A real, non-frozen daily-tier target — untouched by the drain in both phases below. Read off the
               registry rather than named, so this still holds if the roster ever changes. */
            var controlDaily = TimescaleSupport.DailyAggregates.First().View;

            await SetMaterializationChunkIntervalAsync(connection, TimescaleSupport.QueryStatsDailyView, "1 day", ct);

            for (var day = 0; day < 3; day++)
            {
                await InsertQueryStatsAsync(connection, D0.AddDays(day), $"LCBDR{day}", 1000, 10, 3600, ct);
            }

            var d3 = D0.AddDays(3);
            await RefreshAsync(connection, TimescaleSupport.QueryStatsHourlyView, D0, d3, ct);
            await RefreshAsync(connection, TimescaleSupport.QueryStatsDailyView, D0, d3, ct);

            /* A pre-#3653 store's own history: compression already enabled and a policy already attached,
               exactly as every other daily-tier aggregate got before LC froze this trio's daily-band membership
               and stopped compression-managing it going forward. */
            await using (var enable = new NpgsqlCommand(
                $"ALTER MATERIALIZED VIEW collect.{TimescaleSupport.QueryStatsDailyView} SET (timescaledb.compress, " +
                $"timescaledb.compress_segmentby = '{TimescaleSupport.AggregateCompressionSegmentBy}', timescaledb.compress_orderby = 'bucket DESC')",
                connection))
            {
                await enable.ExecuteNonQueryAsync(ct);
            }
            await using (var addPolicy = new NpgsqlCommand(
                $"SELECT add_compression_policy('collect.{TimescaleSupport.QueryStatsDailyView}', compress_after => INTERVAL '1 day', " +
                "schedule_interval => INTERVAL '12 hours', if_not_exists => true)", connection))
            {
                await addPolicy.ExecuteNonQueryAsync(ct);
            }

            var chunks = await ReadMaterializationChunksAsync(connection, TimescaleSupport.QueryStatsDailyView, ct);
            Assert.True(chunks.Count >= 2, "the frozen daily needs at least two chunks to prove a partial compress leaves the policy in place");

            /* Compress every chunk but the last (oldest first), so exactly one chunk stays uncompressed. */
            for (var i = 0; i < chunks.Count - 1; i++)
            {
                await CompressChunkAsync(connection, chunks[i], ct);
            }

            var beforeDrain = await ReadDrainStateAsync(connection, TimescaleSupport.QueryStatsDailyView, ct);
            Assert.NotNull(beforeDrain?.JobId);
            Assert.True(beforeDrain!.UncompressedChunks > 0);

            await TimescaleSupport.EnsureAggregateCompressionAsync(connection, null, ct);

            var afterPartial = await ReadDrainStateAsync(connection, TimescaleSupport.QueryStatsDailyView, ct);
            Assert.Equal(beforeDrain.JobId, afterPartial?.JobId);
            Assert.NotNull(await ReadCompressionJobIdAsync(connection, controlDaily, ct));

            /* Now compress the last chunk — every chunk the frozen daily will ever hold is compressed. */
            await CompressChunkAsync(connection, chunks[^1], ct);

            await TimescaleSupport.EnsureAggregateCompressionAsync(connection, null, ct);

            var afterFull = await ReadDrainStateAsync(connection, TimescaleSupport.QueryStatsDailyView, ct);
            Assert.Null(afterFull?.JobId);
            Assert.NotNull(await ReadCompressionJobIdAsync(connection, controlDaily, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }

    /// <summary>
    /// #3653 high-fix (Option 4): on a store upgrading from the current release, the interval-honest successor
    /// hourlies are empty on first start while the frozen legacy hourlies still hold years of history.
    /// <see cref="TimescaleSupport.IsRawTierDropSafeAsync"/> must return <c>true</c> for such a store — the
    /// stitch covers all of raw's range via the legacy floor, and once the successor has enough history the
    /// legacy term becomes the minimum anyway.
    ///
    /// <para>Also covers the zero-interval source filter: a first-pass row in raw has
    /// <c>sample_interval_seconds = 0</c>; the successors exclude those rows from their CREATE. A 0-interval
    /// row older than any materialized bucket must not hold the gate open permanently.</para>
    /// </summary>
    [Fact]
    public async Task Outage_SeamBetweenFrozenLegacyAndSuccessor_HoleWalkRepairsItAndGateReleases()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live A6 freeze test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live A6 freeze test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            /* #4186: S is the stop. The legacy is refreshed through S-2h only (its own end_offset lag, the
               same reason every hourly rollup trails "now" by roughly an hour). Raw carries admitted rows
               through S — the 2-hour tail [S-1h, S] is the seam: collected, but never materialized by
               either side — plus one interval-0 restart row just after S that the filter must reject, then
               a genuine 2-day outage (no rows at all) before the successor's first post-upgrade refresh at
               U = S+2d, whose own floor (U-1h) starts long after the seam. */
            var s = D0.AddDays(3);

            for (var hour = 0; hour <= 6; hour++)
            {
                await InsertProcedureStatsAsync(connection, s.AddHours(-hour), $"seam_proc_{hour}", 900, 9, 3600, ct);
            }

            await InsertProcedureStatsAsync(connection, s.AddMinutes(30), "seam_proc_restart", 0, 0, 0, ct);

            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsHourlyView, s.AddHours(-6), s.AddHours(-1), ct);

            var u = s.AddDays(2);
            await InsertProcedureStatsAsync(connection, u.AddHours(-1), "seam_proc_successor_floor", 900, 9, 3600, ct);
            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsIntervalHourlyView, u.AddDays(-1), u, ct);

            /* The seam holds raw rows the stitch cannot see through unconditionally — Short, not Covered. */
            Assert.False(await TimescaleSupport.IsRawTierDropSafeAsync(connection, "procedure_stats", ct));

            /* The product's own start-path entry point (DarlingWorker calls this exact method). #4301: the
               walk now fills CONTIGUOUSLY down to raw's own filtered floor (s-6h), not just the seam tail
               above the legacy's boundary — 7 buckets (s-6h..s), not the old 2-bucket seam tail. */
            var summary = await TimescaleSupport.RepairMaterializationHolesAsync(connection, null, u, ct);
            Assert.Equal(7, summary.BucketsRepaired);

            await using (var span = new NpgsqlCommand($"SELECT min(bucket) FROM collect.{TimescaleSupport.ProcedureStatsIntervalHourlyView}", connection))
            {
                var newFloor = (DateTime)(await span.ExecuteScalarAsync(ct))!;
                Assert.Equal(s.AddHours(-6), newFloor);
            }

            /* The seam is now empty (the successor's own floor reaches the legacy's boundary) — Covered. */
            Assert.True(await TimescaleSupport.IsRawTierDropSafeAsync(connection, "procedure_stats", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }

    /// <summary>
    /// #4300: the SAME seam shape as
    /// <see cref="Outage_SeamBetweenFrozenLegacyAndSuccessor_HoleWalkRepairsItAndGateReleases"/>, but for the
    /// SINGLE-start case that method does not exercise: only ONE start's repair runs (it skips the successor
    /// while it is still empty, exactly as the first start after the outage does), the successor's first
    /// refresh then happens on the running service (no restart), and the gate is expected to release from the
    /// hourly Periodic pass's seam-only repair alone — <see cref="TimescaleSupport.RepairMaterializationSeamsAsync"/>,
    /// the exact method <c>DarlingWorker.ReevaluateRetentionPoliciesAsync</c> calls every hour. Before #4300
    /// this pin is RED: the gate still reads Short after the seam-only call, because nothing except a SECOND
    /// full <see cref="TimescaleSupport.RepairMaterializationHolesAsync"/> (a second start) ever re-ran the
    /// walk once the successor had materialized anything.
    /// </summary>
    [Fact]
    public async Task Outage_SeamBetweenFrozenLegacyAndSuccessor_SingleStart_HourlySeamRepairReleasesTheGate()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live A6 freeze test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live A6 freeze test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            /* Same seam shape as the two-day twin: S is the stop, the legacy is refreshed through S-2h, raw
               carries the [S-1h, S] tail plus a rejected interval-0 restart row, then a genuine 2-day outage
               before the successor's first post-upgrade refresh at U = S+2d. */
            var s = D0.AddDays(3);

            for (var hour = 0; hour <= 6; hour++)
            {
                await InsertProcedureStatsAsync(connection, s.AddHours(-hour), $"seam1_proc_{hour}", 900, 9, 3600, ct);
            }

            await InsertProcedureStatsAsync(connection, s.AddMinutes(30), "seam1_proc_restart", 0, 0, 0, ct);

            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsHourlyView, s.AddHours(-6), s.AddHours(-1), ct);

            /* ONE start's repair, at a clock BEFORE the successor's first refresh — the successor is still
               empty, so this walk skips it ("has materialized nothing yet"), exactly as the real first start
               after the outage does. */
            var firstStart = await TimescaleSupport.RepairMaterializationHolesAsync(connection, null, s.AddHours(1), ct);
            Assert.Equal(0, firstStart.BucketsRepaired);

            var u = s.AddDays(2);
            await InsertProcedureStatsAsync(connection, u.AddHours(-1), "seam1_proc_successor_floor", 900, 9, 3600, ct);
            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsIntervalHourlyView, u.AddDays(-1), u, ct);

            /* The seam holds raw rows the stitch cannot see through unconditionally — Short, not Covered —
               with no second start anywhere in this sequence. */
            Assert.False(await TimescaleSupport.IsRawTierDropSafeAsync(connection, "procedure_stats", ct));

            /* The hourly Periodic pass's own seam-only repair — ONE call, ONE tick, no restart. */
            var seamOnly = await TimescaleSupport.RepairMaterializationSeamsAsync(connection, null, u, ct);
            Assert.Equal(7, seamOnly.BucketsRepaired);
            Assert.Equal(0, seamOnly.HolesRemaining);
            Assert.Equal(0, seamOnly.BucketsDeferred);
            Assert.Equal(0, seamOnly.Failures);

            await using (var span = new NpgsqlCommand($"SELECT min(bucket) FROM collect.{TimescaleSupport.ProcedureStatsIntervalHourlyView}", connection))
            {
                var newFloor = (DateTime)(await span.ExecuteScalarAsync(ct))!;
                Assert.Equal(s.AddHours(-6), newFloor);
            }

            /* The seam is now empty — Covered, from the hourly tick alone, no second start. */
            Assert.True(await TimescaleSupport.IsRawTierDropSafeAsync(connection, "procedure_stats", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }

    /// <summary>
    /// #4300: a seam WIDER than the hourly cap (24 buckets) cannot be closed by one seam-only call. The same
    /// shape as <see cref="Outage_SeamWiderThanTheCap_NewestFirstRepairsTheTopAndKeepsTheGateHeldUntilFullyRepaired"/>,
    /// but through <see cref="TimescaleSupport.RepairMaterializationSeamsAsync"/> (the hourly Periodic pass's
    /// own entry point) instead of the full start-path walk. A pass that can only take the newest 24 of a
    /// 36-bucket seam must report the remaining 12 as deferred, leave the raw gate reading not-safe (Short),
    /// and never stamp <c>darling_repair_epoch</c> — that stamp is written only by the full-walk completion
    /// path (<c>DarlingWorker.RunMaterializationHoleRepairAsync</c>), never by the seam-only call itself, so a
    /// pass that could not fully close the seam this tick must not leave behind anything that would let the
    /// raw purge trigger read the epoch as current.
    /// </summary>
    [Fact]
    public async Task Outage_SeamWiderThanTheCap_SeamOnlyRepair_DefersTheRestAndLeavesTheGateHeldWithNoEpochStamped()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live A6 freeze test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live A6 freeze test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            /* Same seam shape as the wider-than-cap start-path pin: the legacy materializes only ONE bucket,
               35 hours back (l.mx = S-35h); raw carries an unbroken run of 36 hourly buckets from S-35h
               through S, so the seam floor (raw's own filtered floor) is 36 buckets wide — wider than the
               24-bucket hourly cap, so ONE seam-only call cannot close it. */
            var s = D0.AddDays(3);

            for (var hour = 0; hour <= 35; hour++)
            {
                await InsertProcedureStatsAsync(connection, s.AddHours(-hour), $"seamcap_proc_{hour}", 900, 9, 3600, ct);
            }

            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsHourlyView, s.AddHours(-35), s.AddHours(-34), ct);

            var u = s.AddDays(2);
            await InsertProcedureStatsAsync(connection, u.AddHours(-1), "seamcap_proc_successor_floor", 900, 9, 3600, ct);
            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsIntervalHourlyView, u.AddDays(-1), u, ct);

            Assert.False(await TimescaleSupport.IsRawTierDropSafeAsync(connection, "procedure_stats", ct));

            /* The hourly Periodic pass's own seam-only repair, ONE call — the cap takes the newest 24 of the
               36 seam buckets, leaving the OLDEST 12 (S-35h..S-24h) deferred. */
            var seamOnly = await TimescaleSupport.RepairMaterializationSeamsAsync(connection, null, u, ct);
            Assert.Equal(24, seamOnly.BucketsRepaired);
            Assert.True(seamOnly.BucketsDeferred > 0, "a 36-bucket seam under a 24-bucket cap must leave a deferred remainder");
            Assert.Equal(12, seamOnly.BucketsDeferred);

            /* The gate stays held — the deferred 12 buckets are still holes below the (partially advanced)
               floor, so a fresh Covered read must still answer Short. */
            Assert.False(await TimescaleSupport.IsRawTierDropSafeAsync(connection, "procedure_stats", ct));

            /* No repair epoch stamped by this call — that stamp belongs only to the full-walk completion
               path (DarlingWorker.RunMaterializationHoleRepairAsync), never to RepairMaterializationSeamsAsync
               itself, whether or not this pass fully closed the seam. */
            await using (var epochCheck = new NpgsqlCommand(TimescaleSupport.RawRepairEpochMatchesSql("procedure_stats"), connection))
            {
                var value = await epochCheck.ExecuteScalarAsync(ct);
                var epochCurrent = value is bool b && b;
                Assert.False(epochCurrent, "a seam-only pass must never stamp the repair epoch, deferred remainder or not");
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }

    /// <summary>
    /// #4300: the seam-only repair touches only legacy-paired targets and only their seam window — an ordinary
    /// interior hole on a target with NO legacy (a plain continuous aggregate) is never repaired by the
    /// seam-only call, even when the ordinary window would otherwise find it. Proven against a real
    /// non-legacy-paired baseline aggregate: a hole is opened inside its span, the seam-only repair runs and
    /// reports it scanned nothing for that target (skipped), and the hole is still there afterward — the full
    /// start-path walk (<see cref="TimescaleSupport.RepairMaterializationHolesAsync"/>) is what closes it.
    /// </summary>
    [Fact]
    public async Task SeamOnlyRepair_NeverTouchesAnOrdinaryInteriorHoleOnANonLegacyTarget()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live A6 freeze test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live A6 freeze test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            /* A plain (non-legacy-paired) target: query_stats_baseline is a BaselineAggregates member,
               never a SupersededHourlyRollups successor, so LegacyOf returns null for it. Materialize an
               OLDER and a NEWER hour, leaving a hole in between, then refresh only the newer one — the
               classic interior hole an outage under HourlyRefreshStartOffset opens. */
            var older = D0.AddDays(3);
            var hole = older.AddHours(1);
            var newer = older.AddHours(3);

            /* The hole bucket gets its OWN source row — a hole is "no materialized bucket AND the source still
               admits a row there" (MaterializationHoleScanSql); a bucket with no source row at all is not a
               hole under that definition, it is legitimately empty, so the fixture has to seed it. */
            await InsertQueryStatsAsync(connection, older, "interior_older", 900, 9, 3600, ct);
            await InsertQueryStatsAsync(connection, hole, "interior_hole", 900, 9, 3600, ct);
            await InsertQueryStatsAsync(connection, newer, "interior_newer", 900, 9, 3600, ct);

            await RefreshAsync(connection, TimescaleSupport.QueryStatsBaselineView, older, older.AddHours(1), ct);
            await RefreshAsync(connection, TimescaleSupport.QueryStatsBaselineView, newer, newer.AddHours(1), ct);

            var seamOnly = await TimescaleSupport.RepairMaterializationSeamsAsync(connection, null, newer.AddHours(2), ct);
            Assert.Equal(0, seamOnly.BucketsRepaired);

            await using (var holeCheck = new NpgsqlCommand($"SELECT count(*) FROM collect.{TimescaleSupport.QueryStatsBaselineView} WHERE bucket = $1", connection))
            {
                holeCheck.Parameters.AddWithValue(DateTime.SpecifyKind(hole, DateTimeKind.Unspecified));
                var stillAHole = Convert.ToInt64(await holeCheck.ExecuteScalarAsync(ct)) == 0;
                Assert.True(stillAHole, "the seam-only repair must not touch an ordinary interior hole on a non-legacy-paired target");
            }

            /* The full start-path walk DOES close it — confirming this is a real hole, not a fixture mistake. */
            var fullWalk = await TimescaleSupport.RepairMaterializationHolesAsync(connection, null, newer.AddHours(2), ct);
            Assert.True(fullWalk.BucketsRepaired > 0, "the full walk should have closed the interior hole the seam-only call left standing");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }

    /// <summary>
    /// #4186 follow-up: the same seam shape as
    /// <see cref="Outage_SeamBetweenFrozenLegacyAndSuccessor_HoleWalkRepairsItAndGateReleases"/>, but the outage
    /// outlasts the raw retention horizon itself (<see cref="TimescaleSupport.MaterializationHoleScanSpanFor"/>:
    /// 4 days for <c>procedure_stats</c>) instead of sitting comfortably inside it. The seam fix's first cut
    /// folded the seam's lower bound into the SAME <c>max(_, horizon)</c> the ordinary scan clamps to, so a seam
    /// older than the horizon was silently dropped every start and the gate held the raw purge forever with no
    /// manual step ever releasing it. The 2-day twin above never exercises that clamp — its horizon sits days
    /// before the seam — so it only proves the short case; this test's outage is 6 days, longer than the
    /// 4-day span, and deliberately exercises it.
    /// </summary>
    [Fact]
    public async Task Outage_SeamOlderThanTheHorizon_HoleWalkStillRepairsItAndGateReleases()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live A6 freeze test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live A6 freeze test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            /* Same seam shape as the 2-day twin: S is the stop, the legacy is refreshed through S-2h, raw
               carries the [S-1h, S] tail plus a rejected interval-0 restart row. The outage here runs 6 days —
               longer than procedure_stats' 4-day raw span — so the successor's first post-upgrade refresh
               lands at U = S+6d, and the seam sits entirely below the horizon RepairMaterializationHolesAsync
               computes from U. */
            var s = D0.AddDays(3);

            for (var hour = 0; hour <= 6; hour++)
            {
                await InsertProcedureStatsAsync(connection, s.AddHours(-hour), $"seam6_proc_{hour}", 900, 9, 3600, ct);
            }

            await InsertProcedureStatsAsync(connection, s.AddMinutes(30), "seam6_proc_restart", 0, 0, 0, ct);

            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsHourlyView, s.AddHours(-6), s.AddHours(-1), ct);

            var u = s.AddDays(6);
            await InsertProcedureStatsAsync(connection, u.AddHours(-1), "seam6_proc_successor_floor", 900, 9, 3600, ct);
            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsIntervalHourlyView, u.AddDays(-1), u, ct);

            /* The seam holds raw rows the stitch cannot see through unconditionally — Short, not Covered. */
            Assert.False(await TimescaleSupport.IsRawTierDropSafeAsync(connection, "procedure_stats", ct));

            /* The product's own start-path entry point (DarlingWorker calls this exact method). Before the
               #4186 follow-up fix, the seam's lower bound was clamped to U minus the 4-day span — comfortably
               ABOVE the seam, since the outage is 6 days — so this repaired 0 buckets and the seam stood
               forever without a manual --backfill-rollups. #4301: the walk now fills CONTIGUOUSLY down to
               raw's own filtered floor (s-6h), not just the seam tail above the legacy's boundary — 7
               buckets (s-6h..s), not the old 2-bucket seam tail. */
            var summary = await TimescaleSupport.RepairMaterializationHolesAsync(connection, null, u, ct);
            Assert.Equal(7, summary.BucketsRepaired);

            await using (var span = new NpgsqlCommand($"SELECT min(bucket) FROM collect.{TimescaleSupport.ProcedureStatsIntervalHourlyView}", connection))
            {
                var newFloor = (DateTime)(await span.ExecuteScalarAsync(ct))!;
                Assert.Equal(s.AddHours(-6), newFloor);
            }

            /* The seam is now empty (the successor's own floor reaches the legacy's boundary) — Covered. */
            Assert.True(await TimescaleSupport.IsRawTierDropSafeAsync(connection, "procedure_stats", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }

    /// <summary>
    /// #4186 round-3 H1: a seam wider than one start's repair cap (24 hourly buckets) used to release the gate
    /// early. Oldest-first repaired the buckets FARTHEST from the successor's floor first, which still moved
    /// the floor (a bare <c>min(bucket)</c>) all the way down to them — stranding the un-repaired NEWER seam
    /// buckets above the new floor and outside <see cref="TimescaleSupport.RetentionArmSafetySql"/>'s probe.
    /// Newest-first must NOT do that: repairing the top 24 of a 36-bucket seam should leave the floor exactly
    /// adjacent to the still-open 12-bucket remainder, so the probe keeps finding it and the gate stays Short
    /// until a second walk closes the rest.
    /// </summary>
    [Fact]
    public async Task Outage_SeamWiderThanTheCap_NewestFirstRepairsTheTopAndKeepsTheGateHeldUntilFullyRepaired()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live A6 freeze test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live A6 freeze test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            /* S is the stop. The legacy materializes only ONE bucket, 35 hours back, so l.mx = S-35h. Raw
               carries an unbroken run of 36 hourly buckets from S-35h through S — #4301: the walk's seam
               floor is raw's own filtered floor (S-35h), not the legacy's max+width (S-34h), so the oldest
               raw bucket is IN the seam too — wider than the 24-bucket cap, so ONE walk cannot close it in
               one pass. */
            var s = D0.AddDays(3);

            for (var hour = 0; hour <= 35; hour++)
            {
                await InsertProcedureStatsAsync(connection, s.AddHours(-hour), $"seam35_proc_{hour}", 900, 9, 3600, ct);
            }

            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsHourlyView, s.AddHours(-35), s.AddHours(-34), ct);

            var u = s.AddDays(2);
            await InsertProcedureStatsAsync(connection, u.AddHours(-1), "seam35_proc_successor_floor", 900, 9, 3600, ct);
            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsIntervalHourlyView, u.AddDays(-1), u, ct);

            Assert.False(await TimescaleSupport.IsRawTierDropSafeAsync(connection, "procedure_stats", ct));

            /* Walk 1: the cap takes the NEWEST 24 of the 36 seam buckets (S-23h..S), leaving the OLDER 12
               (S-35h..S-24h) as a hole immediately below the new floor. The product's own start-path entry
               point (DarlingWorker calls this exact method). #4301: the seam floor is raw's own filtered
               floor (S-35h, the oldest raw row), not the legacy's max+width (S-34h) — one bucket lower, so
               the seam is 36 wide, not 35. */
            var summary1 = await TimescaleSupport.RepairMaterializationHolesAsync(connection, null, u, ct);
            Assert.Equal(24, summary1.BucketsRepaired);

            /* The regression this pins: with the old oldest-first order, this walk would instead have
               repaired S-34h..S-11h (the OLDEST 24) and left S-10h..S as holes ABOVE the new floor — outside
               the probe window entirely, and the gate below would have read true (Covered) after only one
               walk. */
            Assert.False(await TimescaleSupport.IsRawTierDropSafeAsync(connection, "procedure_stats", ct));

            /* Walk 2: the remaining 12-bucket range is now the whole seam (S-35h..S-24h, under the cap), and
               closes it completely. */
            var summary2 = await TimescaleSupport.RepairMaterializationHolesAsync(connection, null, u, ct);
            Assert.Equal(12, summary2.BucketsRepaired);

            Assert.True(await TimescaleSupport.IsRawTierDropSafeAsync(connection, "procedure_stats", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }

    /// <summary>
    /// #4186 round-3 H1, the failure arm: a seam with TWO separate ranges, where the NEWER one fails. A CHECK
    /// constraint added directly to the successor's own materialization chunk (the mechanism verified against
    /// this rig's TimescaleDB before this test was written — <c>ALTER TABLE</c> against the continuous
    /// aggregate's VIEW or its parent materialization hypertable is refused, but a concrete chunk table takes
    /// one) makes the refresh over the newer range raise 23514, the same way a real refresh failure would.
    /// Newest-first must stop there: the older range must be left completely untouched, and the gate must
    /// still read not-safe, exactly as if neither range had been attempted.
    /// </summary>
    [Fact]
    public async Task Outage_SeamTwoRanges_NewerRangeFails_OlderRangeUntouchedAndGateStillNotSafe()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live A6 freeze test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live A6 freeze test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            /* S is the stop. The legacy materializes one bucket 20 hours back (l.mx = S-20h, seam floor
               S-19h). Range A sits right above the seam floor — the OLDEST part of the seam. */
            var s = D0.AddDays(3);
            await InsertProcedureStatsAsync(connection, s.AddHours(-20), "seam2_proc_legacy_floor", 900, 9, 3600, ct);
            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsHourlyView, s.AddHours(-20), s.AddHours(-19), ct);

            await InsertProcedureStatsAsync(connection, s.AddHours(-19), "seam2_proc_a0", 900, 9, 3600, ct);
            await InsertProcedureStatsAsync(connection, s.AddHours(-18), "seam2_proc_a1", 900, 9, 3600, ct);

            /* The successor's own first refresh sets floor = U-1h and, in the same call, creates the
               materialization chunk for that whole calendar day — the chunk range B (below) also falls in. */
            var u = s.AddDays(2);
            await InsertProcedureStatsAsync(connection, u.AddHours(-1), "seam2_proc_floor", 900, 9, 3600, ct);
            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsIntervalHourlyView, u.AddDays(-1), u, ct);

            /* Range B: two buckets below the floor, on the SAME calendar day as it, separated from range A by
               a multi-day gap where raw holds nothing (so A and B are two DISTINCT merged ranges, not one). A
               CHECK constraint on B's exact bucket span, added to the chunk the floor refresh just created,
               blocks it before it ever holds a row. */
            var chunks = await ReadMaterializationChunksAsync(connection, TimescaleSupport.ProcedureStatsIntervalHourlyView, ct);
            Assert.Single(chunks);
            await using (var block = new NpgsqlCommand(
                $"ALTER TABLE {chunks[0]} ADD CONSTRAINT seam2_block_b CHECK (bucket NOT BETWEEN '{u.AddHours(-5):O}'::timestamp AND '{u.AddHours(-4):O}'::timestamp)",
                connection))
            {
                await block.ExecuteNonQueryAsync(ct);
            }

            await InsertProcedureStatsAsync(connection, u.AddHours(-5), "seam2_proc_b0", 900, 9, 3600, ct);
            await InsertProcedureStatsAsync(connection, u.AddHours(-4), "seam2_proc_b1", 900, 9, 3600, ct);

            Assert.False(await TimescaleSupport.IsRawTierDropSafeAsync(connection, "procedure_stats", ct));

            /* Newest-first tries B (closer to the floor) before A. B's refresh raises 23514, caught by this
               target's own per-aggregate catch — the walk must stop there, never reaching A. */
            var summary = await TimescaleSupport.RepairMaterializationHolesAsync(connection, null, u, ct);
            Assert.True(summary.Failures >= 1, $"expected B's constraint violation to be caught as a failure, got {summary.Failures}");

            /* A is untouched: no row near it exists in the successor at all. */
            await using (var probeA = new NpgsqlCommand(
                $"SELECT count(*) FROM collect.{TimescaleSupport.ProcedureStatsIntervalHourlyView} WHERE bucket >= '{s.AddHours(-19):O}'::timestamp AND bucket < '{s.AddHours(-17):O}'::timestamp",
                connection))
            {
                Assert.Equal(0L, (long)(await probeA.ExecuteScalarAsync(ct))!);
            }

            /* The floor never moved off U-1h — B's insert rolled back with its transaction, so both A and B
               still sit inside the probe window, and the gate must still read not-safe. */
            Assert.False(await TimescaleSupport.IsRawTierDropSafeAsync(connection, "procedure_stats", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }

    /// <summary>
    /// #4186: the no-deadlock twin of <see cref="Outage_SeamBetweenFrozenLegacyAndSuccessor_HoleWalkRepairsItAndGateReleases"/> —
    /// same frozen-legacy-plus-outage shape, but the seam itself holds NO raw rows (an outage that began
    /// right at a legacy refresh, or a tail already purged before this fix existed). A FLOOR-only
    /// contiguity test (<c>successor.min &lt;= legacy.max + 1h</c>) would read this as a permanent gap and
    /// hold the purge forever with nothing left to repair it — the deadlock the brief for this fix rules
    /// out. Probing raw directly must report Covered here with no hole walk at all.
    /// </summary>
    [Fact]
    public async Task Outage_EmptySeam_GateReportsCoveredWithNoHoleWalk()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live A6 freeze test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live A6 freeze test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            /* Same S and U as the outage-shape twin, but NOTHING is inserted in (S-2h, S] — the outage began
               exactly at the legacy's last refresh, so there is no un-materialized tail to find. */
            var s = D0.AddDays(3);

            for (var hour = 2; hour <= 6; hour++)
            {
                await InsertProcedureStatsAsync(connection, s.AddHours(-hour), $"nogap_proc_{hour}", 900, 9, 3600, ct);
            }

            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsHourlyView, s.AddHours(-6), s.AddHours(-1), ct);

            var u = s.AddDays(2);
            await InsertProcedureStatsAsync(connection, u.AddHours(-1), "nogap_proc_successor_floor", 900, 9, 3600, ct);
            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsIntervalHourlyView, u.AddDays(-1), u, ct);

            Assert.True(await TimescaleSupport.IsRawTierDropSafeAsync(connection, "procedure_stats", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }

    [Fact]
    public async Task FieldUpgrade_EmptySuccessors_LegacyFilled_ReportsRawPurgeCovered()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live A6 freeze test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live A6 freeze test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            /* 5 days of raw history, with a 0-interval (first-pass) row at day 0 to exercise the
               source-oldest filter. The successors exclude these rows; without the filter the gate
               would hold forever even after the stitch fix. */
            await InsertQueryStatsAsync(connection, D0, "FU_ZERO", 0, 0, 0, ct);
            await InsertProcedureStatsAsync(connection, D0, "fu_zero_proc", 0, 0, 0, ct);

            for (var day = 1; day <= 5; day++)
            {
                var at = D0.AddDays(day);
                await InsertQueryStatsAsync(connection, at, $"FU_QS{day}", 1000, 10, 3600, ct);
                await InsertProcedureStatsAsync(connection, at, $"fu_ps{day}", 900, 9, 3600, ct);
            }

            /* Pre-freeze state: EVERY legacy hourly refreshed to cover all of the interval-1 rows (days
               1-5) — query_stats has TWO (query-grain and db-grain, #1849/LC-a4), and both must be filled
               or the db-grain slot reads NULL (empty legacy AND empty successor) and reports Short on its
               own, independent of anything this test means to exercise. The 0-interval row at day 0 is NOT
               materialized by either legacy or successor (both filter it out via their respective CREATE
               predicates or the source_oldest filter). */
            var d6 = D0.AddDays(6);
            await RefreshAsync(connection, TimescaleSupport.QueryStatsHourlyView, D0.AddDays(1), d6, ct);
            await RefreshAsync(connection, TimescaleSupport.QueryStatsDbHourlyView, D0.AddDays(1), d6, ct);
            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsHourlyView, D0.AddDays(1), d6, ct);

            /* Successors are intentionally NOT refreshed — simulating an upgrading store where LC just
               shipped and the successors are still empty. */
            Assert.Equal(0L, await CountRowsAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, ct));
            Assert.Equal(0L, await CountRowsAsync(connection, TimescaleSupport.QueryStatsDbIntervalHourlyView, ct));
            Assert.Equal(0L, await CountRowsAsync(connection, TimescaleSupport.ProcedureStatsIntervalHourlyView, ct));

            /* The stitched gate must report Covered: the legacy floor (days 1-5) covers raw's interval-1
               oldest row (day 1), so there is no history a purge would destroy. */
            Assert.True(await TimescaleSupport.IsRawTierDropSafeAsync(connection, "query_stats", ct));
            Assert.True(await TimescaleSupport.IsRawTierDropSafeAsync(connection, "procedure_stats", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }

    /// <summary>
    /// PIN (#4300): an upgrade with no outage must not re-hold the raw purge while the successor is still
    /// empty. The legacy is materialized up to a few hours ago and frozen (no ongoing refresh past that
    /// point). Raw carries a row every hour since, right up to now. The successor has not run its first
    /// refresh yet, so it is empty. <see cref="TimescaleSupport.IsRawTierDropSafeAsync"/> must read
    /// <c>true</c>: the successor's own first refresh reaches every bucket newer than
    /// <see cref="TimescaleSupport.HourlyRefreshStartOffset"/>, so none of those recent, unmaterialized hours
    /// are at risk from the raw purge (which only drops chunks older than 4 days). RED on dev: the fallback
    /// probes all the way to a bare <c>now()</c>, calling every one of those recent hours an unprobed hole and
    /// reporting <c>false</c>.
    /// </summary>
    [Fact]
    public async Task Upgrade_NoOutage_EmptySuccessor_RawPurgeStaysCovered()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live A6 freeze test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live A6 freeze test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            /* now() anchor, truncated to the hour so the seeded hourly rows line up with the buckets the
               probe walks. The legacy froze a few hours back (H-6..H-4); raw kept collecting every hour
               since, up to and including the current hour, but the successor never ran its first refresh. */
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var h = new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Unspecified);

            for (var hoursAgo = 0; hoursAgo <= 6; hoursAgo++)
            {
                await InsertProcedureStatsAsync(connection, h.AddHours(-hoursAgo), $"noout_proc_{hoursAgo}", 900, 9, 3600, ct);
            }

            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsHourlyView, h.AddHours(-6), h.AddHours(-4), ct);

            /* Successor never refreshed — the field-upgrade shape this pin exercises. */
            Assert.Equal(0L, await CountRowsAsync(connection, TimescaleSupport.ProcedureStatsIntervalHourlyView, ct));

            Assert.True(await TimescaleSupport.IsRawTierDropSafeAsync(connection, "procedure_stats", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }

    /// <summary>
    /// PIN (#4300): a real outage must still be caught, even inside the new fallback's window. The legacy's
    /// last bucket is 3 days ago; raw carries rows in the hours between 3 days ago and 1 day ago, but neither
    /// side ever materialized any of them — a genuine hole, not a healthy empty successor.
    /// <see cref="TimescaleSupport.IsRawTierDropSafeAsync"/> must stay <c>false</c> on both dev and this
    /// branch: the fix narrows the fallback's UPPER bound, it does not widen what counts as a hole below it.
    /// </summary>
    [Fact]
    public async Task Upgrade_RealOutageInsideTheFallbackWindow_RawPurgeStaysNotSafe()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live A6 freeze test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live A6 freeze test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var h = new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Unspecified);
            var legacyLast = h.AddDays(-3);

            /* Legacy materialized through 3 days ago; raw has hourly rows from 3 days ago up through 1 day
               ago, none of which either side ever refreshed — a genuine outage hole entirely inside the
               fallback's [now - HourlyRefreshStartOffset, now] window this fix narrows the upper bound to. */
            for (var hoursAgo = 24; hoursAgo <= 72; hoursAgo += 6)
            {
                await InsertProcedureStatsAsync(connection, h.AddHours(-hoursAgo), $"outage_proc_{hoursAgo}", 900, 9, 3600, ct);
            }

            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsHourlyView, legacyLast.AddHours(-1), legacyLast, ct);

            /* Successor never refreshed either — same field-upgrade shape, but this time raw holds a real,
               unrepaired hole between the legacy's last bucket and the successor's (nonexistent) floor. */
            Assert.Equal(0L, await CountRowsAsync(connection, TimescaleSupport.ProcedureStatsIntervalHourlyView, ct));

            Assert.False(await TimescaleSupport.IsRawTierDropSafeAsync(connection, "procedure_stats", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }

    /// <summary>
    /// #4301, PIN A (interior hole -&gt; false): the legacy's OWN interior has a gap (hour H2 refreshed by
    /// neither side, despite raw admitting a row there) below its last bucket (H5), and the successor holds a
    /// bucket ABOVE the legacy's last (H6, its own ordinary advance). <see cref="TimescaleSupport.IsRawTierDropSafeAsync"/>
    /// must read <c>false</c>: the gap is a real hole under <see cref="TimescaleSupport.LegacySuccessorHoleExistsSql"/>'s
    /// definition (raw admits a row, neither side materialized it), so the slot must go Short, not Covered.
    /// RED on <c>e970834ba</c>: the old row-level seam-only probe never looks INSIDE the legacy's own span (it
    /// only checks at/after <c>l.mx</c>), so it reports Covered here.
    /// </summary>
    [Fact]
    public async Task InteriorHole_BelowLegacysLastBucket_ReportsRawPurgeNotSafe()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live A6 freeze test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live A6 freeze test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            var b = D0.AddDays(20);

            /* Raw admits rows at H0, H2 (the interior hole) and H5 (legacy's last) and H6 (successor's
               own floor above l.mx). H1, H3, H4 hold no raw rows at all, so their absence from either side
               is never a hole. */
            await InsertProcedureStatsAsync(connection, b, "pinA_h0", 900, 9, 3600, ct);
            await InsertProcedureStatsAsync(connection, b.AddHours(2), "pinA_h2", 900, 9, 3600, ct);
            await InsertProcedureStatsAsync(connection, b.AddHours(5), "pinA_h5", 900, 9, 3600, ct);
            await InsertProcedureStatsAsync(connection, b.AddHours(6), "pinA_h6", 900, 9, 3600, ct);

            /* Legacy materializes H0 and H5 only -- H2 is skipped, the interior hole. */
            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsHourlyView, b, b.AddHours(1), ct);
            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsHourlyView, b.AddHours(5), b.AddHours(6), ct);

            /* Successor materializes H6 only -- its own ordinary advance above l.mx, no interior repair. */
            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsIntervalHourlyView, b.AddHours(6), b.AddHours(7), ct);

            Assert.False(await TimescaleSupport.IsRawTierDropSafeAsync(connection, "procedure_stats", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }

    /// <summary>
    /// #4301, PIN B (the trap -&gt; false): the successor holds ONE bucket BELOW the legacy's last (H3, as an
    /// INTERIOR repair leaves it), which moves <c>s.mn</c> below <c>l.mx</c>, AND a seam hour above the
    /// legacy's last (H6) has a raw row and no bucket anywhere. A probe bounded on <c>s.mn</c> (the old shape)
    /// would see the seam range collapse to <c>(l.mx, s.mn)</c> = <c>(H5, H3)</c>, an EMPTY/inverted range, and
    /// miss H6 entirely -- exactly the trap this lane's bound (the successor's first bucket ABOVE <c>l.mx</c>,
    /// H7) exists to avoid. <see cref="TimescaleSupport.IsRawTierDropSafeAsync"/> must read <c>false</c>.
    /// RED on <c>e970834ba</c>: the old code's seam probe is bounded on <c>COALESCE(s.mn, 'infinity')</c>,
    /// which here is H3, before <c>l.mx + 1h</c> = H6 -- the probe range is empty, no seam row is found, and
    /// the old code falls through to <c>LEAST(l.mn, s.mn)</c>, reporting Covered.
    /// </summary>
    [Fact]
    public async Task InteriorRepairMovesSuccessorFloorBelowSeam_TrapDoesNotHideTheHole_ReportsRawPurgeNotSafe()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live A6 freeze test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live A6 freeze test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            var b = D0.AddDays(30);

            /* Raw admits rows at H0 (raw's floor), H3 (successor's interior repair), H5 (legacy's last),
               H6 (the seam hole -- no bucket anywhere) and H7 (successor's own ordinary advance above
               l.mx, needed so the probe's upper bound resolves to H6 and not the current hour). */
            await InsertProcedureStatsAsync(connection, b, "pinB_h0", 900, 9, 3600, ct);
            await InsertProcedureStatsAsync(connection, b.AddHours(3), "pinB_h3", 900, 9, 3600, ct);
            await InsertProcedureStatsAsync(connection, b.AddHours(5), "pinB_h5", 900, 9, 3600, ct);
            await InsertProcedureStatsAsync(connection, b.AddHours(6), "pinB_h6", 900, 9, 3600, ct);
            await InsertProcedureStatsAsync(connection, b.AddHours(7), "pinB_h7", 900, 9, 3600, ct);

            /* Legacy materializes H0 and H5 only -- l.mx ends at H5. */
            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsHourlyView, b, b.AddHours(1), ct);
            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsHourlyView, b.AddHours(5), b.AddHours(6), ct);

            /* Successor materializes H3 (an interior repair BELOW l.mx) and H7 (its own ordinary advance
               ABOVE l.mx) -- s.mn is H3, below l.mx, the exact shape that moves the successor's floor
               under the seam. H6 (the seam hour) is left uncovered by both sides. */
            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsIntervalHourlyView, b.AddHours(3), b.AddHours(4), ct);
            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsIntervalHourlyView, b.AddHours(7), b.AddHours(8), ct);

            Assert.False(await TimescaleSupport.IsRawTierDropSafeAsync(connection, "procedure_stats", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }

    /// <summary>
    /// #4301, PIN C (clean -&gt; true): the legacy covers every raw-admitted bucket up to its own last bucket
    /// (H5), and the successor's only bucket (H6) sits immediately above it, with no gap anywhere in between.
    /// <see cref="TimescaleSupport.IsRawTierDropSafeAsync"/> must read <c>true</c>. Passes on both this branch
    /// and <c>e970834ba</c> (both find no hole here); included to show the new probe does not fire on a
    /// gap-free stitch.
    /// </summary>
    [Fact]
    public async Task NoGapAnywhere_ReportsRawPurgeSafe()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live A6 freeze test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live A6 freeze test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            var b = D0.AddDays(40);

            await InsertProcedureStatsAsync(connection, b, "pinC_h0", 900, 9, 3600, ct);
            await InsertProcedureStatsAsync(connection, b.AddHours(5), "pinC_h5", 900, 9, 3600, ct);
            await InsertProcedureStatsAsync(connection, b.AddHours(6), "pinC_h6", 900, 9, 3600, ct);

            /* Legacy materializes the whole H0-H5 span, no gap. */
            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsHourlyView, b, b.AddHours(6), ct);

            /* Successor materializes H6, its own ordinary advance immediately above l.mx. */
            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsIntervalHourlyView, b.AddHours(6), b.AddHours(7), ct);

            Assert.True(await TimescaleSupport.IsRawTierDropSafeAsync(connection, "procedure_stats", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }

    /// <summary>
    /// #4301, PIN D (below the floor -&gt; true): the legacy holds no bucket for H0, OLDER than raw's own
    /// filtered floor (H0 carries a first-pass, <c>sample_interval_seconds = 0</c> row that
    /// <see cref="TimescaleSupport.IntervalHonestSourceFilter"/> excludes from admission, so it never sets
    /// <c>fromExpr</c>). Everything from raw's real admitted floor (H2) up to the legacy's last bucket (H5) is
    /// covered. <see cref="TimescaleSupport.IsRawTierDropSafeAsync"/> must read <c>true</c>: a gap below the
    /// probed floor is invisible by construction, exactly as this lane's doc comment states.
    /// </summary>
    [Fact]
    public async Task GapBelowRawsFilteredFloor_IsInvisibleToTheProbe_ReportsRawPurgeSafe()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live A6 freeze test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live A6 freeze test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            var b = D0.AddDays(50);

            /* H0: a first-pass restart row (interval 0), excluded from admission -- never sets fromExpr,
               and the legacy is never refreshed for it (a pre-freeze purge could have taken this bucket
               and it would look identical). H2: raw's real admitted floor. H5: legacy's last bucket.
               H6: successor's own ordinary advance, bounding the probe's upper edge. */
            await InsertProcedureStatsAsync(connection, b, "pinD_h0_restart", 0, 0, 0, ct);
            await InsertProcedureStatsAsync(connection, b.AddHours(2), "pinD_h2", 900, 9, 3600, ct);
            await InsertProcedureStatsAsync(connection, b.AddHours(5), "pinD_h5", 900, 9, 3600, ct);
            await InsertProcedureStatsAsync(connection, b.AddHours(6), "pinD_h6", 900, 9, 3600, ct);

            /* Legacy materializes H2-H5 only -- H0 is never touched, and never needs to be. */
            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsHourlyView, b.AddHours(2), b.AddHours(6), ct);

            /* Successor materializes H6, its own ordinary advance immediately above l.mx. */
            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsIntervalHourlyView, b.AddHours(6), b.AddHours(7), ct);

            Assert.True(await TimescaleSupport.IsRawTierDropSafeAsync(connection, "procedure_stats", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }

    /// <summary>
    /// #4301, PIN 1 (no-op on an A6-backfilled shape): the successor's floor already sits AT OR BELOW raw's
    /// own filtered floor, exactly the shape a store leaves once #4301's walk (or a manual --backfill-rollups)
    /// has already caught the successor up. There is nothing below the successor's floor for the walk to find,
    /// so <see cref="TimescaleSupport.RepairMaterializationHolesAsync"/> must repair NOTHING.
    /// </summary>
    [Fact]
    public async Task ARepairedShape_SuccessorFloorAtOrBelowRawsFloor_RepairsNothing()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live A6 freeze test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live A6 freeze test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            var b = D0.AddDays(30);

            /* Raw admits rows at H0..H3; the successor already refreshed the WHOLE span, so its floor equals
               raw's own filtered floor -- the A6-backfilled shape. No legacy row anywhere. */
            for (var hour = 0; hour <= 3; hour++)
            {
                await InsertProcedureStatsAsync(connection, b.AddHours(hour), $"pin1_h{hour}", 900, 9, 3600, ct);
            }

            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsIntervalHourlyView, b, b.AddHours(4), ct);

            Assert.True(await TimescaleSupport.IsRawTierDropSafeAsync(connection, "procedure_stats", ct));

            var summary = await TimescaleSupport.RepairMaterializationHolesAsync(connection, null, b.AddHours(4), ct);
            Assert.Equal(0, summary.BucketsRepaired);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }

    /// <summary>
    /// #4301, PIN 2 (interior hole: Short, then the walk, then Covered): an interior hole below the legacy's
    /// last bucket (H2 skipped, as PIN A), and the successor floor already sits above l.mx (H6). Before the
    /// walk, <see cref="TimescaleSupport.IsRawTierDropSafeAsync"/> reads <c>false</c>. Running
    /// <see cref="TimescaleSupport.RepairMaterializationHolesAsync"/> repeatedly until it converges must
    /// eventually leave the successor covering the whole interior span, after which the same probe reads
    /// <c>true</c>.
    /// </summary>
    [Fact]
    public async Task InteriorHole_RepairedByTheWalk_ThenReportsCovered()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live A6 freeze test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live A6 freeze test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            var b = D0.AddDays(40);

            /* Same shape as PIN A: raw at H0, H2 (interior hole), H5 (legacy's last), H6 (successor floor). */
            await InsertProcedureStatsAsync(connection, b, "pin2_h0", 900, 9, 3600, ct);
            await InsertProcedureStatsAsync(connection, b.AddHours(2), "pin2_h2", 900, 9, 3600, ct);
            await InsertProcedureStatsAsync(connection, b.AddHours(5), "pin2_h5", 900, 9, 3600, ct);
            await InsertProcedureStatsAsync(connection, b.AddHours(6), "pin2_h6", 900, 9, 3600, ct);

            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsHourlyView, b, b.AddHours(1), ct);
            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsHourlyView, b.AddHours(5), b.AddHours(6), ct);
            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsIntervalHourlyView, b.AddHours(6), b.AddHours(7), ct);

            Assert.False(await TimescaleSupport.IsRawTierDropSafeAsync(connection, "procedure_stats", ct));

            var u = b.AddHours(7);
            for (var pass = 0; pass < 10 && !await TimescaleSupport.IsRawTierDropSafeAsync(connection, "procedure_stats", ct); pass++)
            {
                var summary = await TimescaleSupport.RepairMaterializationHolesAsync(connection, null, u, ct);
                if (summary.BucketsRepaired == 0)
                {
                    break;
                }
            }

            Assert.True(await TimescaleSupport.IsRawTierDropSafeAsync(connection, "procedure_stats", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }

    /// <summary>
    /// #4301, PIN 3 (stitched reads keep every row -- the trap): legacy buckets cover [raw floor, l.mx] with
    /// NO zero-interval rows, so legacy and successor agree on totals; the successor holds only its own
    /// buckets from l.mx + 1h up. A sum read through <see cref="RollupCoverage.StitchedRelationSql"/> over
    /// <c>[raw floor, now)</c> BEFORE the walk must equal the SAME read AFTER the walk -- the walk only fills
    /// successor buckets the legacy already agreed with, so the stitch (which reads whichever side a bucket's
    /// time falls on) must not double count or drop anything. After the walk, the successor's own floor must
    /// reach down to raw's floor.
    /// </summary>
    [Fact]
    public async Task StitchedReadThroughTheWalk_KeepsEveryRow_AndSuccessorFloorReachesRawsFloor()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live A6 freeze test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live A6 freeze test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            var b = D0.AddDays(50);

            /* Raw admits an unbroken run of hourly rows b..b+5h (no zero-interval rows), legacy materializes
               ALL of them (b..b+5h), and the successor only has its own advance from b+6h up. The seam is
               [b, b+5h] -- below the successor's floor, above raw's floor -- exactly the walk's fill range. */
            for (var hour = 0; hour <= 5; hour++)
            {
                await InsertProcedureStatsAsync(connection, b.AddHours(hour), $"pin3_h{hour}", 900, 9, 3600, ct);
            }

            await InsertProcedureStatsAsync(connection, b.AddHours(6), "pin3_h6", 900, 9, 3600, ct);

            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsHourlyView, b, b.AddHours(6), ct);
            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsIntervalHourlyView, b.AddHours(6), b.AddHours(7), ct);

            var u = b.AddHours(7);

            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

            async Task<decimal> ReadStitchedSumAsync()
            {
                var rollups = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
                var coverage = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, rollups, ct);
                var fromClause = coverage.StitchedRelationSql(
                    TimescaleSupport.ProcedureStatsHourlyView, "p", b.AddYears(-1), RollupCoverage.StitchTier.Hourly);
                await using var command = new NpgsqlCommand(
                    $"SELECT coalesce(sum(worker_time_sum), 0) FROM {fromClause}", connection);
                return Convert.ToDecimal(await command.ExecuteScalarAsync(ct));
            }

            var before = await ReadStitchedSumAsync();
            Assert.True(before > 0, "expected the pre-walk stitched read to see the legacy's rows");

            for (var pass = 0; pass < 10; pass++)
            {
                var summary = await TimescaleSupport.RepairMaterializationHolesAsync(connection, null, u, ct);
                if (summary.BucketsRepaired == 0)
                {
                    break;
                }
            }

            var after = await ReadStitchedSumAsync();
            Assert.Equal(before, after);

            await using (var span = new NpgsqlCommand($"SELECT min(bucket) FROM collect.{TimescaleSupport.ProcedureStatsIntervalHourlyView}", connection))
            {
                var newFloor = (DateTime)(await span.ExecuteScalarAsync(ct))!;
                Assert.Equal(b, newFloor);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }

    /// <summary>
    /// #4301, PIN 4 (the cap across passes): a range wider than <see cref="TimescaleSupport.MaterializationHoleRepairCapBuckets"/>
    /// below raw's floor. Pass 1 fills the newest cap-worth from the top; pass 2 continues from the new floor
    /// down. After enough passes every non-empty hour in <c>[raw floor, original floor)</c> has a successor
    /// bucket, with no gap between the two passes' ranges.
    /// </summary>
    [Fact]
    public async Task CapAcrossPasses_LeavesNoGapBetweenPasses()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live A6 freeze test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live A6 freeze test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            /* Raw admits an unbroken run of 30 hourly rows -- wider than the 24-bucket cap -- with NO legacy
               materialization at all, so the seam floor is raw's own filtered floor. The successor holds
               only its own advance above the whole span. */
            var b = D0.AddDays(60);

            for (var hour = 0; hour <= 30; hour++)
            {
                await InsertProcedureStatsAsync(connection, b.AddHours(hour), $"pin4_h{hour}", 900, 9, 3600, ct);
            }

            var u = b.AddHours(31);
            await InsertProcedureStatsAsync(connection, u.AddHours(-1), "pin4_successor_floor", 900, 9, 3600, ct);
            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsIntervalHourlyView, u.AddHours(-1), u, ct);

            var summary1 = await TimescaleSupport.RepairMaterializationHolesAsync(connection, null, u, ct);
            Assert.Equal(24, summary1.BucketsRepaired);

            DateTime floorAfterPass1;
            await using (var span = new NpgsqlCommand($"SELECT min(bucket) FROM collect.{TimescaleSupport.ProcedureStatsIntervalHourlyView}", connection))
            {
                floorAfterPass1 = (DateTime)(await span.ExecuteScalarAsync(ct))!;
            }

            var summary2 = await TimescaleSupport.RepairMaterializationHolesAsync(connection, null, u, ct);
            Assert.Equal(6, summary2.BucketsRepaired);

            DateTime floorAfterPass2;
            await using (var span = new NpgsqlCommand($"SELECT min(bucket) FROM collect.{TimescaleSupport.ProcedureStatsIntervalHourlyView}", connection))
            {
                floorAfterPass2 = (DateTime)(await span.ExecuteScalarAsync(ct))!;
            }

            /* No gap between the two passes' ranges: pass 2's floor is exactly one bucket below pass 1's. */
            Assert.Equal(floorAfterPass1.AddHours(-6), floorAfterPass2);
            Assert.Equal(b, floorAfterPass2);

            Assert.True(await TimescaleSupport.IsRawTierDropSafeAsync(connection, "procedure_stats", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }

    /// <summary>
    /// #4301, PIN 5 (an unknown source state holds): the gate's probe is made to fail by renaming the
    /// successor's underlying continuous aggregate mid-run, so <see cref="TimescaleSupport.RetentionArmSafetySql"/>
    /// names a relation that no longer exists. <see cref="TimescaleSupport.IsRawTierDropSafeAsync"/> must fail
    /// closed: an Unknown probe answers <c>false</c>, same as a measured Short, never <c>true</c>.
    /// </summary>
    [Fact]
    public async Task ProbeFailure_UnknownSourceState_ReportsRawPurgeNotSafe()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live A6 freeze test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live A6 freeze test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            var b = D0.AddDays(70);

            await InsertProcedureStatsAsync(connection, b, "pin5_h0", 900, 9, 3600, ct);
            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsIntervalHourlyView, b, b.AddHours(1), ct);

            /* Baseline: with the successor's own view present, the gate reads Covered. */
            Assert.True(await TimescaleSupport.IsRawTierDropSafeAsync(connection, "procedure_stats", ct));

            /* Rename the successor CAGG's view underneath the gate -- RetentionArmSafetySql still names the
               old view, so its probe query fails with an undefined-relation error, caught and turned into
               the Unknown verdict. */
            await using (var rename = new NpgsqlCommand(
                $"ALTER MATERIALIZED VIEW collect.{TimescaleSupport.ProcedureStatsIntervalHourlyView} RENAME TO pin5_renamed_away", connection))
            {
                await rename.ExecuteNonQueryAsync(ct);
            }

            Assert.False(await TimescaleSupport.IsRawTierDropSafeAsync(connection, "procedure_stats", ct));

            /* Restore the name so LiveStoreCleanup's own teardown (DROP DATABASE) does not trip over an
               unexpected shape while the scratch database is torn down. */
            await using (var restore = new NpgsqlCommand(
                $"ALTER MATERIALIZED VIEW collect.pin5_renamed_away RENAME TO {TimescaleSupport.ProcedureStatsIntervalHourlyView}", connection))
            {
                await restore.ExecuteNonQueryAsync(ct);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }

    private static async Task InsertQueryStatsAsync(
        NpgsqlConnection connection, DateTime at, string hash, long workerUs, long executions, int intervalSeconds, CancellationToken ct)
    {
        await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, $9, $10)", connection);
        insert.Parameters.AddWithValue(CollectionIdGenerator.Next());
        insert.Parameters.AddWithValue(DarlingMcpTestData.TruncateToSeconds(at));
        insert.Parameters.AddWithValue(ServerId);
        insert.Parameters.AddWithValue(ServerName);
        insert.Parameters.AddWithValue(Db);
        insert.Parameters.AddWithValue(hash);
        insert.Parameters.AddWithValue("0x" + hash);
        insert.Parameters.AddWithValue(workerUs);
        insert.Parameters.AddWithValue(executions);
        insert.Parameters.AddWithValue(intervalSeconds);
        await insert.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertProcedureStatsAsync(
        NpgsqlConnection connection, DateTime at, string objectName, long workerUs, long executions, int intervalSeconds, CancellationToken ct)
    {
        await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, 'dbo', $6, $7, $8, $8, $9, $10)", connection);
        insert.Parameters.AddWithValue(CollectionIdGenerator.Next());
        insert.Parameters.AddWithValue(DarlingMcpTestData.TruncateToSeconds(at));
        insert.Parameters.AddWithValue(ServerId);
        insert.Parameters.AddWithValue(ServerName);
        insert.Parameters.AddWithValue(Db);
        insert.Parameters.AddWithValue(objectName);
        insert.Parameters.AddWithValue("0x" + objectName);
        insert.Parameters.AddWithValue(workerUs);
        insert.Parameters.AddWithValue(executions);
        insert.Parameters.AddWithValue(intervalSeconds);
        await insert.ExecuteNonQueryAsync(ct);
    }

    private static async Task RefreshAsync(NpgsqlConnection connection, string view, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
        refresh.Parameters.AddWithValue(from);
        refresh.Parameters.AddWithValue(to);
        await refresh.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Sets ONE continuous aggregate's materialization chunk width directly — the frozen six take no
    /// width from <see cref="TimescaleSupport.EnsureMaterializationChunkIntervalAsync"/> (it walks only
    /// <see cref="TimescaleSupport.AggregateCompressionTargets"/>), so a live test that needs deterministic
    /// chunk boundaries on one of them has to set it here. Must run before the first refresh creates a chunk —
    /// <c>set_chunk_time_interval</c> governs only chunks created after the call.</summary>
    private static async Task SetMaterializationChunkIntervalAsync(NpgsqlConnection connection, string view, string interval, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($@"
SELECT set_chunk_time_interval(format('%I.%I', ca.materialization_hypertable_schema, ca.materialization_hypertable_name)::regclass, INTERVAL '{interval}')
FROM timescaledb_information.continuous_aggregates AS ca
WHERE ca.view_schema = 'collect' AND ca.view_name = '{view}'", connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<List<(DateTime Bucket, long WorkerSum, long ExecSum)>> ReadDailyTotalsAsync(
        NpgsqlConnection connection, string view, int serverId, DateTime start, DateTime end, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand(
            $"SELECT bucket, sum(worker_time_sum), sum(execution_count_sum) FROM collect.{view} " +
            "WHERE server_id = $1 AND bucket >= $2 AND bucket < $3 GROUP BY bucket ORDER BY bucket", connection);
        read.Parameters.AddWithValue(serverId);
        read.Parameters.AddWithValue(start);
        read.Parameters.AddWithValue(end);
        var rows = new List<(DateTime, long, long)>();
        await using var reader = await read.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add((reader.GetDateTime(0), reader.GetInt64(1), reader.GetInt64(2)));
        }
        return rows;
    }

    private static async Task<HashSet<string>> ReadScheduledRefreshViewsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand(
            $"SELECT hypertable_name FROM timescaledb_information.jobs WHERE proc_name = '{TimescaleSupport.RefreshPolicyProcName}' AND hypertable_schema = 'collect'",
            connection);
        var views = new HashSet<string>(StringComparer.Ordinal);
        await using var reader = await read.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            views.Add(reader.GetString(0));
        }
        return views;
    }

    private static async Task<long> CountRowsAsync(NpgsqlConnection connection, string relation, CancellationToken ct)
    {
        await using var count = new NpgsqlCommand($"SELECT count(*) FROM collect.{relation}", connection);
        return Convert.ToInt64(await count.ExecuteScalarAsync(ct));
    }

    /// <summary>
    /// #4299 (d′): query_stats and procedure_stats are raw relations — their armed verdict is
    /// config->>'darling_armed' through RawArmedStateSql, never 'scheduled' (which this build converges to
    /// false unconditionally for the three raw jobs). Every other relation keeps the original scheduled read.
    /// Mirrors the product's own branch in EnsureRetentionPoliciesAsync so the test and the product cannot
    /// disagree about which column answers "is this armed".
    /// </summary>
    private static async Task<bool?> IsRetentionScheduledAsync(NpgsqlConnection connection, string relation, CancellationToken ct)
    {
        var isRawRelation = TimescaleSupport.RawRelations.Any(r => r == relation);
        await using var read = new NpgsqlCommand(
            isRawRelation ? TimescaleSupport.RawArmedStateSql(relation) : TimescaleSupport.RetentionPolicyScheduledSql(relation), connection);
        var value = await read.ExecuteScalarAsync(ct);
        return value is bool b ? b : null;
    }

    private static async Task<List<string>> ReadMaterializationChunksAsync(NpgsqlConnection connection, string view, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand($@"
SELECT format('%I.%I', c.chunk_schema, c.chunk_name)
FROM timescaledb_information.chunks AS c
JOIN timescaledb_information.continuous_aggregates AS ca
  ON  c.hypertable_schema = ca.materialization_hypertable_schema
  AND c.hypertable_name = ca.materialization_hypertable_name
WHERE ca.view_schema = 'collect' AND ca.view_name = '{view}'
ORDER BY c.range_start", connection);
        var chunks = new List<string>();
        await using var reader = await read.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            chunks.Add(reader.GetString(0));
        }
        return chunks;
    }

    private static async Task CompressChunkAsync(NpgsqlConnection connection, string chunk, CancellationToken ct)
    {
        await using var compress = new NpgsqlCommand($"SELECT compress_chunk('{chunk}'::regclass, if_not_compressed => true)", connection);
        await compress.ExecuteNonQueryAsync(ct);
    }

    private static async Task<TimescaleSupport.FrozenDailyCompressionDrainState?> ReadDrainStateAsync(
        NpgsqlConnection connection, string view, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand(TimescaleSupport.FrozenDailyCompressionDrainStateSql, connection);
        await using var reader = await read.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            if (string.Equals(reader.GetString(0), view, StringComparison.Ordinal))
            {
                return new TimescaleSupport.FrozenDailyCompressionDrainState(
                    reader.GetString(0),
                    reader.IsDBNull(1) ? null : reader.GetInt32(1),
                    Convert.ToInt64(reader.GetValue(2)));
            }
        }
        return null;
    }

    private static async Task<int?> ReadCompressionJobIdAsync(NpgsqlConnection connection, string view, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand($@"
SELECT j.job_id
FROM timescaledb_information.jobs AS j
WHERE (j.proc_name LIKE '%compression%' OR j.proc_name LIKE '%columnstore%')
AND   j.hypertable_schema = 'collect'
AND   j.hypertable_name = '{view}'", connection);
        var value = await read.ExecuteScalarAsync(ct);
        return value is null or DBNull ? null : Convert.ToInt32(value);
    }
}
