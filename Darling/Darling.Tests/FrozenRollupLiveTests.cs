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

            /* The product's own start-path entry point (DarlingWorker calls this exact method). */
            var summary = await TimescaleSupport.RepairMaterializationHolesAsync(connection, null, u, ct);
            Assert.True(summary.BucketsRepaired >= 2, $"expected the 2-bucket seam tail to be repaired, got {summary.BucketsRepaired}");

            await using (var span = new NpgsqlCommand($"SELECT min(bucket) FROM collect.{TimescaleSupport.ProcedureStatsIntervalHourlyView}", connection))
            {
                var newFloor = (DateTime)(await span.ExecuteScalarAsync(ct))!;
                Assert.Equal(s.AddHours(-1), newFloor);
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
               forever without a manual --backfill-rollups. */
            var summary = await TimescaleSupport.RepairMaterializationHolesAsync(connection, null, u, ct);
            Assert.True(summary.BucketsRepaired >= 2, $"expected the 2-bucket seam tail to be repaired, got {summary.BucketsRepaired}");

            await using (var span = new NpgsqlCommand($"SELECT min(bucket) FROM collect.{TimescaleSupport.ProcedureStatsIntervalHourlyView}", connection))
            {
                var newFloor = (DateTime)(await span.ExecuteScalarAsync(ct))!;
                Assert.Equal(s.AddHours(-1), newFloor);
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

    private static async Task<bool?> IsRetentionScheduledAsync(NpgsqlConnection connection, string relation, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand(TimescaleSupport.RetentionPolicyScheduledSql(relation), connection);
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
