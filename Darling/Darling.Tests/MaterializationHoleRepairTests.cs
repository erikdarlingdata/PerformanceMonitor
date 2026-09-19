/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3653 (Q10, item 9's last clause): the materialization-hole repair —
/// <see cref="TimescaleSupport.RepairMaterializationHolesAsync"/> — pinned in its pure parts here and proven
/// on the outage shape itself in <see cref="MaterializationHoleRepairLiveTests"/>.
///
/// <para><b>The shape the pins hold.</b> The targets are every registered aggregate in dependency order with
/// the source, time column, width and CREATE the scan reads; the scan SQL is two index probes per bucket with
/// the aggregate's own filter on the source side; contiguous hole buckets fold into one refresh each; the cap is
/// one policy window per aggregate per start, oldest first, splitting a straddling range exactly; the horizon
/// is the source's own retention. Each is a function of the registry or of its arguments and nothing else, so
/// each is walked without a store.</para>
/// </summary>
public sealed class MaterializationHoleRepairTests
{
    private static readonly DateTime Hour = new(2026, 9, 18, 10, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public void Targets_AreEveryRegisteredAggregate_InDependencyOrder_WithTheirOwnCreate()
    {
        var targets = TimescaleSupport.MaterializationHoleTargets;
        var registered = TimescaleSupport.HourlyAggregates.Concat(TimescaleSupport.DailyAggregates).Concat(TimescaleSupport.BaselineAggregates).ToArray();

        Assert.Equal(registered.Length, targets.Count);
        Assert.Equal(23, targets.Count);
        Assert.Equal(registered.Select(a => a.View).OrderBy(v => v, StringComparer.Ordinal), targets.Select(t => t.View).OrderBy(v => v, StringComparer.Ordinal));

        /* The rollups come first in the backfill's dependency order — every raw-sourced rollup before the
           rollups that read it — and the baselines follow; each carries its own CREATE. */
        Assert.Equal(RollupBackfill.Targets.Select(t => t.View), targets.Take(RollupBackfill.Targets.Length).Select(t => t.View));
        Assert.Equal(TimescaleSupport.BaselineAggregates.Select(a => a.View), targets.Skip(RollupBackfill.Targets.Length).Select(t => t.View));
        foreach (var target in targets)
        {
            Assert.Equal(registered.Single(a => a.View == target.View).CreateSql, target.CreateSql);
            Assert.Contains($"FROM collect.{target.Source}", target.CreateSql, StringComparison.Ordinal);
            Assert.True(target.BucketWidth == TimescaleSupport.HourlyBucket || target.BucketWidth == TimescaleSupport.DailyBucket);
            Assert.Equal(target.Source.EndsWith("_hourly", StringComparison.Ordinal) || target.Source.EndsWith("_daily", StringComparison.Ordinal) ? "bucket" : "collection_time", target.SourceTimeColumn);
        }

        /* A daily is scanned after the hourly it reads, so its scan sees the rows the hourly's repair wrote. */
        foreach (var (legacy, _, dependentDaily) in TimescaleSupport.SupersededHourlyRollups)
        {
            Assert.True(
                targets.ToList().FindIndex(t => t.View == legacy) < targets.ToList().FindIndex(t => t.View == dependentDaily),
                $"{dependentDaily} must be scanned after {legacy}");
        }
    }

    [Fact]
    public void TheCap_IsOnePolicyWindowInBuckets_PerGrain()
    {
        Assert.Equal(24, TimescaleSupport.MaterializationHoleRepairCapBuckets(TimescaleSupport.HourlyBucket));
        Assert.Equal(3, TimescaleSupport.MaterializationHoleRepairCapBuckets(TimescaleSupport.DailyBucket));
        Assert.Equal((int)(TimescaleSupport.HourlyRefreshStartSpan.Ticks / TimescaleSupport.HourlyBucket.Ticks), TimescaleSupport.MaterializationHoleRepairCapBuckets(TimescaleSupport.HourlyBucket));
        Assert.Equal((int)(TimescaleSupport.DailyRefreshStartSpan.Ticks / TimescaleSupport.DailyBucket.Ticks), TimescaleSupport.MaterializationHoleRepairCapBuckets(TimescaleSupport.DailyBucket));
        Assert.Throws<ArgumentOutOfRangeException>(() => TimescaleSupport.MaterializationHoleRepairCapBuckets(TimeSpan.Zero));
    }

    [Fact]
    public void TheScanHorizon_IsTheSourcesOwnRetention_AndTheBaselineTierForCollectorPurgedSources()
    {
        Assert.Equal(TimescaleSupport.RawRetentionSpan, TimescaleSupport.MaterializationHoleScanSpanFor("query_stats"));
        Assert.Equal(TimescaleSupport.RawRetentionSpan, TimescaleSupport.MaterializationHoleScanSpanFor("procedure_stats"));
        Assert.Equal(TimescaleSupport.RawRetentionSpan, TimescaleSupport.MaterializationHoleScanSpanFor("query_store_stats"));
        Assert.Equal(TimescaleSupport.HourlyRetentionSpan, TimescaleSupport.MaterializationHoleScanSpanFor(TimescaleSupport.QueryStatsHourlyView));
        Assert.Equal(TimescaleSupport.IntervalRetentionSpan, TimescaleSupport.MaterializationHoleScanSpanFor(TimescaleSupport.QueryStoreStatsIntervalHourlyView));
        Assert.Equal(TimescaleSupport.IntervalDailyRetentionSpan, TimescaleSupport.MaterializationHoleScanSpanFor(TimescaleSupport.QueryStoreStatsIntervalDailyView));
        Assert.Equal(TimescaleSupport.BaselineRetentionSpan, TimescaleSupport.MaterializationHoleScanSpanFor("wait_stats"));
        Assert.Equal(TimescaleSupport.BaselineRetentionSpan, TimescaleSupport.MaterializationHoleScanSpanFor("perfmon_stats"));

        /* Every target's source resolves — a new tier with an unknown horizon string throws by design. */
        foreach (var target in TimescaleSupport.MaterializationHoleTargets)
        {
            Assert.True(TimescaleSupport.MaterializationHoleScanSpanFor(target.Source) > TimeSpan.Zero);
        }
    }

    [Fact]
    public void TheSourceFilter_IsTheAggregatesOwnWhere_Verbatim_OrEmpty()
    {
        Assert.Equal(string.Empty, TimescaleSupport.MaterializationHoleSourceFilterFor(TimescaleSupport.CreateQueryStatsHourlySql));
        Assert.Equal("sample_interval_seconds IS DISTINCT FROM 0", TimescaleSupport.MaterializationHoleSourceFilterFor(TimescaleSupport.CreateQueryStatsIntervalHourlySql));
        Assert.Equal("delta_worker_time IS NOT NULL AND sample_interval_seconds IS DISTINCT FROM 0", TimescaleSupport.MaterializationHoleSourceFilterFor(TimescaleSupport.CreateQueryStatsDbIntervalHourlySql));
        Assert.Equal("delta_worker_time IS NOT NULL", TimescaleSupport.MaterializationHoleSourceFilterFor(TimescaleSupport.CreateQueryStatsDbHourlySql));
        Assert.Equal(
            "counter_name = 'Batch Requests/sec' AND delta_cntr_value >= 0 AND sample_interval_seconds IS DISTINCT FROM 0",
            TimescaleSupport.MaterializationHoleSourceFilterFor(TimescaleSupport.CreatePerfmonIntervalBaselineSql));
        Assert.Throws<ArgumentNullException>(() => TimescaleSupport.MaterializationHoleSourceFilterFor(null!));

        /* And the scan SQL carries it on the SOURCE probe only, with both probes present and the width bound
           once as $3. */
        var target = TimescaleSupport.MaterializationHoleTargets.Single(t => t.View == TimescaleSupport.QueryStatsIntervalHourlyView);
        var sql = TimescaleSupport.MaterializationHoleScanSql(target, ("_timescaledb_internal", "_materialized_hypertable_42"));
        Assert.Contains("generate_series($1::timestamp, $2::timestamp, $3::interval)", sql, StringComparison.Ordinal);
        Assert.Contains("NOT EXISTS (SELECT 1 FROM \"_timescaledb_internal\".\"_materialized_hypertable_42\" AS m WHERE m.bucket = b.bucket)", sql, StringComparison.Ordinal);
        Assert.Contains("FROM collect.query_stats AS s", sql, StringComparison.Ordinal);
        Assert.Contains("s.collection_time >= b.bucket", sql, StringComparison.Ordinal);
        Assert.Contains("s.collection_time < b.bucket + $3::interval", sql, StringComparison.Ordinal);
        Assert.Contains("AND   sample_interval_seconds IS DISTINCT FROM 0)", sql, StringComparison.Ordinal);
        Assert.EndsWith("ORDER BY b.bucket", sql.TrimEnd(), StringComparison.Ordinal);

        var unfiltered = TimescaleSupport.MaterializationHoleScanSql(
            TimescaleSupport.MaterializationHoleTargets.Single(t => t.View == TimescaleSupport.QueryStatsDailyView), ("s", "m"));
        Assert.Contains("FROM collect.query_stats_hourly AS s", unfiltered, StringComparison.Ordinal);
        Assert.Contains("s.bucket >= b.bucket", unfiltered, StringComparison.Ordinal);
        /* No filter: the source probe closes straight after the width bound. */
        Assert.Contains("s.bucket < b.bucket + $3::interval)", unfiltered, StringComparison.Ordinal);
    }

    [Fact]
    public void ContiguousBuckets_FoldIntoOneRefresh_AndACoveredBucketBetweenTwoHolesKeepsThemApart()
    {
        var width = TimescaleSupport.HourlyBucket;

        Assert.Empty(TimescaleSupport.MergeContiguousBuckets(Array.Empty<DateTime>(), width));

        var one = TimescaleSupport.MergeContiguousBuckets(new[] { Hour }, width);
        Assert.Equal(new[] { (Hour, Hour.AddHours(1)) }, one);

        /* Unordered input, two runs: 10-12 (three buckets) and 14 alone; 13 is covered and stays covered. */
        var two = TimescaleSupport.MergeContiguousBuckets(new[] { Hour.AddHours(2), Hour, Hour.AddHours(4), Hour.AddHours(1) }, width);
        Assert.Equal(new[] { (Hour, Hour.AddHours(3)), (Hour.AddHours(4), Hour.AddHours(5)) }, two);

        /* Daily width folds days. */
        var days = TimescaleSupport.MergeContiguousBuckets(new[] { Hour.Date, Hour.Date.AddDays(1) }, TimescaleSupport.DailyBucket);
        Assert.Equal(new[] { (Hour.Date, Hour.Date.AddDays(2)) }, days);

        Assert.Throws<ArgumentOutOfRangeException>(() => TimescaleSupport.MergeContiguousBuckets(new[] { Hour }, TimeSpan.Zero));
        Assert.Throws<ArgumentNullException>(() => TimescaleSupport.MergeContiguousBuckets(null!, width));
    }

    [Fact]
    public void TheCap_TakesTheOldestFirst_SplitsAStraddlingRangeExactly_AndDefersTheRest()
    {
        var width = TimescaleSupport.HourlyBucket;
        var ranges = new List<(DateTime Start, DateTime End)>
        {
            (Hour.AddHours(30), Hour.AddHours(40)),   /* newest, 10 buckets */
            (Hour, Hour.AddHours(20)),                /* oldest, 20 buckets */
            (Hour.AddHours(22), Hour.AddHours(28)),   /* middle, 6 buckets */
        };

        var (repair, deferred) = TimescaleSupport.CapMaterializationHoleRepairs(ranges, 24, width);

        /* Oldest 20 whole, then 4 of the middle's 6 — split at the cap — the other 2 and the newest deferred. */
        Assert.Equal(new[] { (Hour, Hour.AddHours(20)), (Hour.AddHours(22), Hour.AddHours(26)) }, repair);
        Assert.Equal(new[] { (Hour.AddHours(26), Hour.AddHours(28)), (Hour.AddHours(30), Hour.AddHours(40)) }, deferred);
        Assert.Equal(24, repair.Sum(r => (int)((r.End - r.Start).Ticks / width.Ticks)));

        /* Under the cap: everything repaired, nothing deferred. Exactly at it: the same. */
        var small = new List<(DateTime Start, DateTime End)> { (Hour, Hour.AddHours(2)) };
        Assert.Equal(small, TimescaleSupport.CapMaterializationHoleRepairs(small, 24, width).Repair);
        Assert.Empty(TimescaleSupport.CapMaterializationHoleRepairs(small, 24, width).Deferred);
        Assert.Empty(TimescaleSupport.CapMaterializationHoleRepairs(new List<(DateTime, DateTime)> { (Hour, Hour.AddHours(24)) }, 24, width).Deferred);

        Assert.Throws<ArgumentOutOfRangeException>(() => TimescaleSupport.CapMaterializationHoleRepairs(small, 0, width));
        Assert.Throws<ArgumentNullException>(() => TimescaleSupport.CapMaterializationHoleRepairs(null!, 24, width));
    }

    [Fact]
    public void AlignDown_LandsOnABucketBoundary_ForBothWidths()
    {
        var instant = new DateTime(2026, 9, 18, 10, 47, 13, DateTimeKind.Unspecified);
        Assert.Equal(Hour, TimescaleSupport.AlignDown(instant, TimescaleSupport.HourlyBucket));
        Assert.Equal(Hour.Date, TimescaleSupport.AlignDown(instant, TimescaleSupport.DailyBucket));
        Assert.Equal(Hour, TimescaleSupport.AlignDown(Hour, TimescaleSupport.HourlyBucket));
        Assert.Equal(DateTimeKind.Unspecified, TimescaleSupport.AlignDown(instant, TimescaleSupport.HourlyBucket).Kind);
        Assert.Throws<ArgumentOutOfRangeException>(() => TimescaleSupport.AlignDown(instant, TimeSpan.Zero));
    }

    /// <summary>
    /// The start path: launched (not awaited) right after the ensure, on its own connection, inside the
    /// TimescaleDB block, before the compression and retention ensures; drained at shutdown beside the baseline
    /// backfill. Source-order pins, the RetiredBaselineAggregateTests shape.
    /// </summary>
    [Fact]
    public void Worker_LaunchesTheRepair_AfterTheEnsure_BeforeTheRetentionReArm_AndDrainsIt()
    {
        var worker = ReadWorkerSource();

        var ensureAt = worker.IndexOf("TimescaleSupport.EnsureContinuousAggregatesAsync(", StringComparison.Ordinal);
        var launchAt = worker.IndexOf("holeRepair = RunMaterializationHoleRepairAsync(postgres, stoppingToken);", StringComparison.Ordinal);
        var compressionAt = worker.IndexOf("TimescaleSupport.EnsureAggregateCompressionAsync(", StringComparison.Ordinal);
        var retentionAt = worker.IndexOf("TimescaleSupport.EnsureRetentionPoliciesAsync(", StringComparison.Ordinal);
        var plainModeAt = worker.IndexOf("continuing in plain-PostgreSQL mode", StringComparison.Ordinal);
        var drainAt = worker.IndexOf("await holeRepair;", StringComparison.Ordinal);
        var stoppedAt = worker.IndexOf("collection loop stopped", StringComparison.Ordinal);

        Assert.True(ensureAt > 0 && launchAt > 0 && compressionAt > 0 && retentionAt > 0 && plainModeAt > 0 && drainAt > 0 && stoppedAt > 0);
        Assert.True(ensureAt < launchAt, "the repair must be launched after the ensure that creates every aggregate");
        Assert.True(launchAt < compressionAt && launchAt < retentionAt, "the repair is launched before the compression and retention ensures");
        Assert.True(launchAt < plainModeAt, "the repair is launched inside the TimescaleDB block");
        Assert.True(drainAt < stoppedAt, "the repair is drained before the loop reports stopped");

        /* Launched, not awaited; its own connection; the pass itself is called once, in the runner. */
        Assert.DoesNotContain("await RunMaterializationHoleRepairAsync(", worker, StringComparison.Ordinal);
        Assert.Contains("await TimescaleSupport.RepairMaterializationHolesAsync(connection, _logger, DateTime.UtcNow, stoppingToken);", worker, StringComparison.Ordinal);
        Assert.Equal(1, CountOf(worker, "TimescaleSupport.RepairMaterializationHolesAsync("));
    }

    private static int CountOf(string text, string needle)
    {
        var count = 0;
        for (var at = text.IndexOf(needle, StringComparison.Ordinal); at >= 0; at = text.IndexOf(needle, at + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    private static string ReadWorkerSource([CallerFilePath] string thisFile = "")
    {
        var relative = Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.False(dir is null, "could not locate the repo root from the test source path");
        return File.ReadAllText(Path.Combine(dir!, relative));
    }
}

/// <summary>
/// The outage shape, planted for real on TimescaleDB and repaired with ONE targeted refresh (#3653, Q10).
///
/// <para><b>The sequence is the fleet's, step for step.</b> Collections land; a policy refresh materializes up
/// to its window's end; two more hours of collections land ABOVE the invalidation threshold that refresh set
/// (so they are never logged as invalidations — the mechanism, not a simulation of it); the service goes down
/// for hours; it comes back, collections resume, and the first refresh after resume covers only its own
/// trailing window. The aggregate then holds the pre-outage buckets and the post-outage ones and NOTHING for
/// the two-hour tail, while raw holds the tail's rows — measured here as the defect before it is repaired.
/// The repair finds exactly that tail, refreshes exactly its bounds, and the tail reads. The outage hours
/// themselves, which have no rows, are never treated as holes; the buckets below the aggregate's floor are
/// never touched. A second pass finds nothing.</para>
///
/// <para><b>Into a COMPRESSED chunk.</b> The materialization's chunk holding the hole is compressed before the
/// repair, because on a store the hole is typically past <c>compress_after</c> by the time a restart finds it,
/// and a refresh that could not write into a compressed materialization would make the repair a no-op exactly
/// where it is needed.</para>
///
/// <para><b>Both refresh paths, each on the shape that needs it.</b> The outage tail closes on the PLAIN refresh
/// (2.28.1 records the region a refresh skipped past as invalid when it advances the threshold — measured here,
/// after the first cut of the pass assumed the opposite and went straight to <c>force</c>). A hole with no
/// invalidation behind it — rows deleted from the materialization hypertable directly, the shape a refresh cut
/// short after consuming its entries leaves — does NOT close on the plain refresh and DOES on the forced one,
/// and the pass says which path it took.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class MaterializationHoleRepairLiveTests
{
    private const int ServerId = -936537;
    private const string ServerName = "hole-repair-e2e";

    [Fact]
    public async Task PreOutageTail_ReadsEmptyAfterResume_AndOneTargetedForcedRefreshClosesIt_AgainstDevPostgres()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live materialization-hole repair test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled,
            "The live hole-repair test needs TimescaleDB: a materialization hole exists only in a materialization.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var view = TimescaleSupport.QueryStatsHourlyView;
        var materialization = await TimescaleSupport.ResolveMaterializationAsync(connection, view, ct);
        Assert.NotNull(materialization);

        /* Twelve hours, all inside the raw horizon: H0-H2 collected and refreshed before the outage, H3-H4 the
           pre-outage TAIL (collected, never refreshed), H5-H8 the outage (nothing collected), H9-H10 collected
           after resume and refreshed by the first post-resume window. H11 is the still-filling current hour. */
        var h0 = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-1), DateTimeKind.Unspecified);
        DateTime H(int n) => h0.AddHours(n);

        await InsertHoursAsync(connection, new[] { 0, 1, 2 }, H, ct);
        await RefreshAsync(connection, view, H(0), H(3), ct);

        /* The tail: inserted ABOVE the threshold the refresh just set, so TimescaleDB does not log it. */
        await InsertHoursAsync(connection, new[] { 3, 4 }, H, ct);

        /* The outage: H5-H8 empty. Resume: H9, H10 collected; the first refresh after resume covers only its
           own trailing window. */
        await InsertHoursAsync(connection, new[] { 9, 10 }, H, ct);
        await RefreshAsync(connection, view, H(9), H(11), ct);

        /* THE DEFECT, MEASURED: raw holds H3 and H4, the aggregate holds nothing for them, and the aggregate's
           floor (H0) says the window is covered. */
        Assert.Equal(new[] { H(0), H(1), H(2), H(9), H(10) }, await MaterializedBucketsAsync(connection, view, ct));
        Assert.Equal(2, await RawHoursAsync(connection, H(3), H(5), ct));
        Assert.Equal(0, await RawHoursAsync(connection, H(5), H(9), ct));

        /* The scan alone, before the repair: exactly the tail — not the outage hours, not the live edge. */
        var target = TimescaleSupport.MaterializationHoleTargets.Single(t => t.View == view);
        Assert.Equal(new[] { H(3), H(4) }, await ScanAsync(connection, target, materialization.Value, H(0), H(10), ct));

        /* Into a compressed chunk: compression enabled on every materialization the product's own way (#3581's
           ensure), then the chunk(s) holding the tail compressed by hand — the nightly policy would do it two
           days on, which is exactly when a restart typically finds the hole. */
        await TimescaleSupport.EnsureAggregateCompressionAsync(connection, null, ct);
        await using (var compress = new NpgsqlCommand(
            $"SELECT count(compress_chunk(c)) FROM show_chunks('{materialization.Value.Schema}.{materialization.Value.Name}') AS c", connection))
        {
            var compressed = (long)(await compress.ExecuteScalarAsync(ct))!;
            Assert.True(compressed >= 1, "the materialization must have at least one chunk to compress for this leg to mean anything");
        }

        /* THE REPAIR: one forced refresh over [H3, H5) and nothing else. */
        var log = new CapturingTestLogger();
        var summary = await TimescaleSupport.RepairMaterializationHolesAsync(connection, log, DateTime.UtcNow, ct);

        Assert.Equal(1, summary.HolesRepaired);
        Assert.Equal(2, summary.BucketsRepaired);
        Assert.Equal(0, summary.HolesDeferred);
        Assert.Equal(0, summary.HolesRemaining);
        Assert.Equal(0, summary.Failures);
        Assert.True(summary.AggregatesScanned >= 1);

        /* THE OUTAGE SHAPE CLOSES ON THE PLAIN REFRESH — the engine logged the skipped region as invalid when
           the post-resume refresh advanced the threshold past it. Pinned, because the first cut of the pass
           assumed the opposite; if a TimescaleDB ever stops doing this the forced path still closes it and this
           assertion is what says the premise moved. */
        Assert.Equal(0, summary.HolesForced);
        Assert.Contains($"{view} had 2 bucket(s) in [{H(3):O}, {H(5):O})", log.Joined, StringComparison.Ordinal);
        Assert.Contains("one refresh over exactly those bounds closed it", log.Joined, StringComparison.Ordinal);
        Assert.DoesNotContain("a plain refresh left it standing", log.Joined, StringComparison.Ordinal);
        Assert.Contains("(0 needed the forced refresh)", log.Joined, StringComparison.Ordinal);
        Assert.DoesNotContain("still shows", log.Joined, StringComparison.Ordinal);
        Assert.DoesNotContain("left for the next start", log.Joined, StringComparison.Ordinal);
        Assert.DoesNotContain("could not scan or repair", log.Joined, StringComparison.Ordinal);

        /* The tail reads; the outage hours are still (correctly) absent; the floor did not move. */
        Assert.Equal(new[] { H(0), H(1), H(2), H(3), H(4), H(9), H(10) }, await MaterializedBucketsAsync(connection, view, ct));
        Assert.Empty(await ScanAsync(connection, target, materialization.Value, H(0), H(10), ct));

        /* A second pass finds nothing to do. */
        var again = await TimescaleSupport.RepairMaterializationHolesAsync(connection, null, DateTime.UtcNow, ct);
        Assert.Equal(0, again.HolesRepaired);
        Assert.Equal(0, again.Failures);

        /* THE HOLE WITH NO INVALIDATION BEHIND IT: H1's rows deleted from the materialization hypertable directly
           (a DELETE on the materialization logs nothing; it is the shape a refresh cut short after consuming its
           entries leaves). The plain refresh finds nothing to do and the hole stands; the pass measures that,
           escalates to the forced refresh, and the hole closes — the one case force is for, on the real engine. */
        await using (var lose = new NpgsqlCommand(
            $"DELETE FROM {materialization.Value.Schema}.{materialization.Value.Name} WHERE bucket = $1", connection))
        {
            lose.Parameters.AddWithValue(H(1));
            Assert.True(await lose.ExecuteNonQueryAsync(ct) >= 1, "the materialization must have held H1 for this leg to delete");
        }

        Assert.Equal(new[] { H(1) }, await ScanAsync(connection, target, materialization.Value, H(0), H(10), ct));
        await RefreshAsync(connection, view, H(1), H(2), ct);
        Assert.Equal(new[] { H(1) }, await ScanAsync(connection, target, materialization.Value, H(0), H(10), ct));

        var forcedLog = new CapturingTestLogger();
        var forcedSummary = await TimescaleSupport.RepairMaterializationHolesAsync(connection, forcedLog, DateTime.UtcNow, ct);
        Assert.Equal(1, forcedSummary.HolesRepaired);
        Assert.Equal(1, forcedSummary.HolesForced);
        Assert.Equal(0, forcedSummary.HolesRemaining);
        Assert.Equal(0, forcedSummary.Failures);
        Assert.Contains($"{view} had 1 bucket(s) in [{H(1):O}, {H(2):O})", forcedLog.Joined, StringComparison.Ordinal);
        Assert.Contains("a plain refresh left it standing (no invalidation behind it) and one forced refresh over exactly those bounds closed it", forcedLog.Joined, StringComparison.Ordinal);
        Assert.Empty(await ScanAsync(connection, target, materialization.Value, H(0), H(10), ct));
        Assert.Equal(new[] { H(0), H(1), H(2), H(3), H(4), H(9), H(10) }, await MaterializedBucketsAsync(connection, view, ct));

        /* THE CONTROL on the source filter: the interval-honest successor sees the same tail (its WHERE admits
           the planted rows), but an hour holding ONLY a restart row is not a hole for it — the scan applies the
           aggregate's own filter — while the legacy, which admits the row, would materialize it. Planted at
           H(12), refreshed on the legacy so its span reaches past it, and the successor's span made to reach
           past it too by refreshing H(9)-H(11) there. */
        var successor = TimescaleSupport.QueryStatsIntervalHourlyView;
        var successorMaterialization = await TimescaleSupport.ResolveMaterializationAsync(connection, successor, ct);
        Assert.NotNull(successorMaterialization);
        await RefreshAsync(connection, successor, H(0), H(3), ct);
        await using (var restartOnly = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES (99, $1, $2, $3, 'HoleDb', '0xHOLEHASH', '0xHOLEHANDLE', 0, 0, 0, 0)", connection))
        {
            restartOnly.Parameters.AddWithValue(H(12).AddMinutes(5));
            restartOnly.Parameters.AddWithValue(ServerId);
            restartOnly.Parameters.AddWithValue(ServerName);
            await restartOnly.ExecuteNonQueryAsync(ct);
        }

        await InsertHoursAsync(connection, new[] { 14 }, H, ct);
        await RefreshAsync(connection, successor, H(14), H(15), ct);

        var successorTarget = TimescaleSupport.MaterializationHoleTargets.Single(t => t.View == successor);
        var successorHoles = await ScanAsync(connection, successorTarget, successorMaterialization.Value, H(0), H(14), ct);
        Assert.DoesNotContain(H(12), successorHoles);
        Assert.Equal(new[] { H(3), H(4), H(9), H(10) }, successorHoles);

        await RefreshAsync(connection, view, H(12), H(13), ct);
        Assert.Contains(H(12), await MaterializedBucketsAsync(connection, view, ct));
    }

    private static async Task InsertHoursAsync(NpgsqlConnection connection, int[] hours, Func<int, DateTime> at, CancellationToken ct)
    {
        foreach (var hour in hours)
        {
            for (var minute = 0; minute < 60; minute += 20)
            {
                await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ($1, $2, $3, $4, 'HoleDb', '0xHOLEHASH', '0xHOLEHANDLE', 1000, 1000, 10, 1200)", connection);
                insert.Parameters.AddWithValue((long)(hour * 100 + minute));
                insert.Parameters.AddWithValue(at(hour).AddMinutes(minute));
                insert.Parameters.AddWithValue(ServerId);
                insert.Parameters.AddWithValue(ServerName);
                await insert.ExecuteNonQueryAsync(ct);
            }
        }
    }

    private static async Task RefreshAsync(NpgsqlConnection connection, string view, DateTime from, DateTime to, CancellationToken ct)
    {
        /* The plain, unforced form a policy runs — the CALL cannot be inside a transaction. */
        await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
        refresh.Parameters.AddWithValue(from);
        refresh.Parameters.AddWithValue(to);
        await refresh.ExecuteNonQueryAsync(ct);
    }

    private static async Task<DateTime[]> MaterializedBucketsAsync(NpgsqlConnection connection, string view, CancellationToken ct)
    {
        var buckets = new List<DateTime>();
        await using var read = new NpgsqlCommand($"SELECT DISTINCT bucket FROM collect.{view} WHERE server_id = $1 ORDER BY bucket", connection);
        read.Parameters.AddWithValue(ServerId);
        await using var reader = await read.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            buckets.Add(reader.GetDateTime(0));
        }

        return buckets.ToArray();
    }

    private static async Task<long> RawHoursAsync(NpgsqlConnection connection, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var count = new NpgsqlCommand(
            "SELECT count(DISTINCT time_bucket('1 hour', collection_time)) FROM collect.query_stats WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3", connection);
        count.Parameters.AddWithValue(ServerId);
        count.Parameters.AddWithValue(from);
        count.Parameters.AddWithValue(to);
        return (long)(await count.ExecuteScalarAsync(ct))!;
    }

    private static async Task<DateTime[]> ScanAsync(
        NpgsqlConnection connection, TimescaleSupport.MaterializationHoleTarget target, (string Schema, string Name) materialization,
        DateTime from, DateTime to, CancellationToken ct)
    {
        var holes = new List<DateTime>();
        await using var scan = new NpgsqlCommand(TimescaleSupport.MaterializationHoleScanSql(target, materialization), connection);
        scan.Parameters.AddWithValue(from);
        scan.Parameters.AddWithValue(to);
        scan.Parameters.AddWithValue(target.BucketWidth);
        await using var reader = await scan.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            holes.Add(reader.GetDateTime(0));
        }

        return holes.ToArray();
    }
}
