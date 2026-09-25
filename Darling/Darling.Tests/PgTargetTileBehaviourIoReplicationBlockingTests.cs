/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// issue-3653 A8 option B: behaviour tests for PR #4170 (the PG target's I/O latency, replay lag, and blocked
/// sessions detectors moving to per-hour tiles). These prove what the whole-window gate could not: a 2-hour
/// sustained shift inside a 4-hour (and a 24-hour, N-aware) window fires against its own hour-of-week baseline
/// where the whole-window mean stays under the cutoff; a lone spike does not fire; and the fallback path (every
/// tile under the minimum) still works through today's whole-window comparison.
///
/// <para>The I/O family gets all four scenarios (design's brief); replay lag and blocked sessions get scenario 1
/// only. Gated on <c>DARLING_TEST_PG</c>. Each scenario plants ONE continuous, set-based timeline per table
/// (<c>generate_series</c>) spanning the 21-day baseline plus the analysis window, so the detector's baseline
/// provider and window read see the SAME rows a real pass would.</para>
///
/// <para><b>Scenario 1's I/O test is also #4170's regression test</b> for commit 3a7e6cf3b, which bound
/// <c>$4..$6</c> (the tile key's clock parameters) on the three tiled window reads — before it, those binds
/// failed silently and every tile read the whole window as one bucket. A tile-vs-whole-window split that never
/// happened would sail through this test with <c>tiles_scored == 1</c>; the assertion pins <c>tiles_scored == 4</c>
/// specifically because the unbound version could not produce it.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetTileBehaviourIoReplicationBlockingTests
{
    private const int MinutesPerTick = 15;

    /* ── I/O latency (first family: all four scenarios) ───────────────────────────────────── */

    [Fact]
    public async Task Io_TwoHourShift_FiresOnTheTile_WhereTheWholeWindowMeanWouldNotHaveFired()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the I/O tile-behaviour test.");
        var ct = TestContext.Current.CancellationToken;

        const string serverName = "darling-pg-tile-io-shift-e2e";
        var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteIoRowsAsync(connection, serverId, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, serverId, serverName, "postgres", 17, ct);

            // T = a fixed Wednesday, 10:00 UTC. No server_properties row: keying is UTC.
            var windowStart = new DateTime(2026, 9, 30, 10, 0, 0, DateTimeKind.Unspecified);
            var windowEnd = windowStart.AddHours(4);
            var baselineStart = windowStart.AddDays(-21);
            await PlantDatabaseStatsSpanAsync(connection, serverId, serverName, baselineStart, windowEnd, ct);

            const double mu = 3.0;
            const double sigma = 0.5;
            const long readsPerTick = 1000;
            var totalTicks = (int)((windowEnd - baselineStart).TotalMinutes / MinutesPerTick);
            var shiftFromTick = (int)((windowStart.AddHours(2) - baselineStart).TotalMinutes / MinutesPerTick);

            /* One continuous cumulative timeline (client backend / relation / normal), 15-minute ticks from 21
               days before the window through its end: baseline noise μ + σ·sin(i) everywhere except the last
               two hours of the window, which run the WHOLE hour at μ + 6σ (the sustained shift). */
            await PlantIoTimelineAsync(connection, serverId, serverName, baselineStart, totalTicks, readsPerTick,
                $"CASE WHEN n >= {shiftFromTick} THEN {mu + 6 * sigma} ELSE {mu} + {sigma} * sin(n) END", ct);

            var context = Context(serverId, serverName, windowStart, windowEnd);
            var baselines = new PgTargetBaselineProvider(postgres);
            var detector = new PgTargetAnomalyDetector(postgres, baselines);
            var anomalies = await detector.DetectAnomaliesAsync(context);
            var anomaly = Assert.Single(anomalies, a => a.Key == PgTargetFactKeys.AnomalyIoLatency);

            Assert.True(anomaly.Metadata.ContainsKey("tile_local_hour"), "the tile path did not fire (fell back to whole-window)");
            var tileHour = anomaly.Metadata["tile_local_hour"];
            Assert.True(tileHour is 12 or 13, $"tile_local_hour was {tileHour}, expected 12 or 13");
            Assert.Equal(4, anomaly.Metadata["tiles_scored"]);
            Assert.Equal(2, anomaly.Metadata["tiles_fired"]);
            // The I/O family carries robust statistics: DecideRobustFirst grades tiles on the MODIFIED-z cutoff
            // (3.5, not the classical 2.0 default), at the 4h reference window (no N-aware raise here).
            var expectedTileCutoff = AnomalyThresholds.ModifiedZThresholdFor(MetricNames.PgIoReadLatency);
            Assert.Equal(expectedTileCutoff, anomaly.Metadata["fire_threshold"]);

            // dev's whole-window gate not firing is proven by the mutation check (EvaluateTiles → null), per #3653 B's test recipe.

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteIoRowsAsync(cleanup, serverId, cleanupCt));
        }
    }

    [Fact]
    public async Task Io_TwoHourShiftInATwentyFourHourWindow_Fires_WithTheNAwareRaisedCutoff()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the I/O tile-behaviour test.");
        var ct = TestContext.Current.CancellationToken;

        const string serverName = "darling-pg-tile-io-24h-e2e";
        var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteIoRowsAsync(connection, serverId, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, serverId, serverName, "postgres", 17, ct);

            // T = the same fixed Wednesday, 10:00 UTC; a 24h window anchored at T+2h: [T-22h, T+2h).
            var t = new DateTime(2026, 9, 30, 10, 0, 0, DateTimeKind.Unspecified);
            var windowStart = t.AddHours(-22);
            var windowEnd = t.AddHours(2);
            var baselineStart = windowStart.AddDays(-21);
            await PlantDatabaseStatsSpanAsync(connection, serverId, serverName, baselineStart, windowEnd, ct);

            const double mu = 3.0;
            const double sigma = 0.5;
            const long readsPerTick = 1000;
            var totalTicks = (int)((windowEnd - baselineStart).TotalMinutes / MinutesPerTick);
            // Hours 22-23 of the window are [T-2h, T) — the shift.
            var shiftFromTick = (int)((t.AddHours(-2) - baselineStart).TotalMinutes / MinutesPerTick);
            var shiftToTick = (int)((t - baselineStart).TotalMinutes / MinutesPerTick);

            await PlantIoTimelineAsync(connection, serverId, serverName, baselineStart, totalTicks, readsPerTick,
                $"CASE WHEN n >= {shiftFromTick} AND n < {shiftToTick} THEN {mu + 6 * sigma} ELSE {mu} + {sigma} * sin(n) END", ct);

            var context = Context(serverId, serverName, windowStart, windowEnd);
            var baselines = new PgTargetBaselineProvider(postgres);
            var detector = new PgTargetAnomalyDetector(postgres, baselines);
            var anomalies = await detector.DetectAnomaliesAsync(context);
            var anomaly = Assert.Single(anomalies, a => a.Key == PgTargetFactKeys.AnomalyIoLatency);

            Assert.True(anomaly.Metadata.ContainsKey("tile_local_hour"), "the tile path did not fire (fell back to whole-window)");
            // The I/O family grades tiles on the MODIFIED-z cutoff (robust statistics), not the classical default.
            var expectedCutoff = AnomalyThresholds.NAwarePeakCutoff(AnomalyThresholds.ModifiedZThresholdFor(MetricNames.PgIoReadLatency), TimeSpan.FromHours(24));
            Assert.Equal(expectedCutoff, anomaly.Metadata["fire_threshold"], precision: 2);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteIoRowsAsync(cleanup, serverId, cleanupCt));
        }
    }

    [Fact]
    public async Task Io_ALoneSpike_DoesNotFire()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the I/O tile-behaviour test.");
        var ct = TestContext.Current.CancellationToken;

        const string serverName = "darling-pg-tile-io-spike-e2e";
        var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteIoRowsAsync(connection, serverId, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, serverId, serverName, "postgres", 17, ct);

            var windowStart = new DateTime(2026, 9, 30, 10, 0, 0, DateTimeKind.Unspecified);
            var windowEnd = windowStart.AddHours(4);
            var baselineStart = windowStart.AddDays(-21);
            await PlantDatabaseStatsSpanAsync(connection, serverId, serverName, baselineStart, windowEnd, ct);

            const double mu = 3.0;
            const double sigma = 0.5;
            const long readsPerTick = 1000;
            // A finer grain than the shift scenarios (5 minutes, 12 ticks/hour instead of 4): with only 4 samples
            // in a tile, ONE 10σ sample also drags the tile MEAN past the mean clause's cutoff, so the pair gate
            // would fire on the mean alone — not the isolated-spike case this scenario is for. At 12 samples/hour
            // the same single spike still clears the PEAK clause (it must, to prove the gate looked at it) but
            // leaves the tile mean under the cutoff, so the AND of both clauses correctly does not fire.
            const int spikeMinutesPerTick = 5;
            var totalTicks = (int)((windowEnd - baselineStart).TotalMinutes / spikeMinutesPerTick);
            // One sample in hour 2 of the window (local hour 11): windowStart + 1h15m.
            var spikeTick = (int)((windowStart.AddHours(1).AddMinutes(15) - baselineStart).TotalMinutes / spikeMinutesPerTick);

            await PlantIoTimelineAsync(connection, serverId, serverName, baselineStart, totalTicks, readsPerTick,
                $"CASE WHEN n = {spikeTick} THEN {mu + 10 * sigma} ELSE {mu} + {sigma} * sin(n) END", ct, minutesPerTick: spikeMinutesPerTick);

            var context = Context(serverId, serverName, windowStart, windowEnd);
            var baselines = new PgTargetBaselineProvider(postgres);
            var detector = new PgTargetAnomalyDetector(postgres, baselines);
            var anomalies = await detector.DetectAnomaliesAsync(context);
            Assert.DoesNotContain(anomalies, a => a.Key == PgTargetFactKeys.AnomalyIoLatency);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteIoRowsAsync(cleanup, serverId, cleanupCt));
        }
    }

    [Fact]
    public async Task Io_FallbackPath_FiresThroughTheWholeWindow_WithNoTileKeysAtAll()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the I/O tile-behaviour test.");
        var ct = TestContext.Current.CancellationToken;

        const string serverName = "darling-pg-tile-io-fallback-e2e";
        var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteIoRowsAsync(connection, serverId, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, serverId, serverName, "postgres", 17, ct);

            var windowStart = new DateTime(2026, 9, 30, 10, 0, 0, DateTimeKind.Unspecified);
            var windowEnd = windowStart.AddHours(4);
            var baselineStart = windowStart.AddDays(-21);
            await PlantDatabaseStatsSpanAsync(connection, serverId, serverName, baselineStart, windowEnd, ct);

            const double mu = 3.0;
            const double sigma = 0.5;
            // Two samples per hour (under the design's minimum of 3 tile samples, and — since I/O counts
            // RATED samples — each tick must still clear the per-sample reads floor of its own).
            const int minutesPerFallbackTick = 30;
            const long readsPerTick = 1000;
            var totalBaselineTicks = (int)((windowStart - baselineStart).TotalMinutes / MinutesPerTick);
            var windowTicks = (int)((windowEnd - windowStart).TotalMinutes / minutesPerFallbackTick);

            /* Baseline: normal 15-minute cadence (plenty of tile samples there — this test is about the
               WINDOW's tiles, not the baseline's). Window: only 2 samples an hour, all at μ + 6σ. */
            await PlantIoTimelineAsync(connection, serverId, serverName, baselineStart, totalBaselineTicks, readsPerTick,
                $"{mu} + {sigma} * sin(n)", ct);
            await PlantIoTimelineAsync(connection, serverId, serverName, windowStart, windowTicks, readsPerTick,
                $"{mu + 6 * sigma}", ct, minutesPerTick: minutesPerFallbackTick);

            var context = Context(serverId, serverName, windowStart, windowEnd);
            var baselines = new PgTargetBaselineProvider(postgres);
            var detector = new PgTargetAnomalyDetector(postgres, baselines);
            var anomalies = await detector.DetectAnomaliesAsync(context);
            var anomaly = Assert.Single(anomalies, a => a.Key == PgTargetFactKeys.AnomalyIoLatency);

            Assert.False(anomaly.Metadata.ContainsKey("tile_local_hour"), "expected the never-blind fallback (no tile keys), not a tile fire");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteIoRowsAsync(cleanup, serverId, cleanupCt));
        }
    }

    /* ── replay lag (scenario 1 only) ──────────────────────────────────────────────────────── */

    [Fact]
    public async Task ReplayLag_TwoHourShift_FiresOnTheTile_WhereTheWholeWindowMeanWouldNotHaveFired()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the replay-lag tile-behaviour test.");
        var ct = TestContext.Current.CancellationToken;

        const string serverName = "darling-pg-tile-replay-shift-e2e";
        var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteReplicationRowsAsync(connection, serverId, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, serverId, serverName, "postgres", 17, ct);

            var windowStart = new DateTime(2026, 9, 30, 10, 0, 0, DateTimeKind.Unspecified);
            var windowEnd = windowStart.AddHours(4);
            var baselineStart = windowStart.AddDays(-21);
            await PlantDatabaseStatsSpanAsync(connection, serverId, serverName, baselineStart, windowEnd, ct);

            const double mu = 20_971_520.0; // 20 MiB, already above PgReplayLagBytesFloor (16 MiB is the noise floor).
            const double sigma = 2_097_152.0; // 2 MiB spread, so mu + 6*sigma clears the floor with margin.
            var totalTicks = (int)((windowEnd - baselineStart).TotalMinutes / MinutesPerTick);
            var shiftFromTick = (int)((windowStart.AddHours(2) - baselineStart).TotalMinutes / MinutesPerTick);

            await PlantReplicationTimelineAsync(connection, serverId, serverName, baselineStart, totalTicks,
                $"CASE WHEN n >= {shiftFromTick} THEN {mu + 6 * sigma} ELSE {mu} + {sigma} * sin(n) END", ct);

            var context = Context(serverId, serverName, windowStart, windowEnd);
            var baselines = new PgTargetBaselineProvider(postgres);
            var detector = new PgTargetAnomalyDetector(postgres, baselines);
            var anomalies = await detector.DetectAnomaliesAsync(context);
            var anomaly = Assert.Single(anomalies, a => a.Key == PgTargetFactKeys.AnomalyReplicationLag);

            Assert.True(anomaly.Metadata.ContainsKey("tile_local_hour"), "the tile path did not fire (fell back to whole-window)");
            var tileHour = anomaly.Metadata["tile_local_hour"];
            Assert.True(tileHour is 12 or 13, $"tile_local_hour was {tileHour}, expected 12 or 13");
            Assert.Equal(4, anomaly.Metadata["tiles_scored"]);
            Assert.Equal(2, anomaly.Metadata["tiles_fired"]);
            // The replay-lag bucket carries robust statistics too: the modified-z cutoff applies, not the classical default.
            var expectedReplayCutoff = AnomalyThresholds.ModifiedZThresholdFor(MetricNames.PgReplayLagBytes);
            Assert.Equal(expectedReplayCutoff, anomaly.Metadata["fire_threshold"]);

            // dev's whole-window gate not firing is proven by the mutation check (EvaluateTiles -> null), per #3653 B's test recipe.

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteReplicationRowsAsync(cleanup, serverId, cleanupCt));
        }
    }

    /* ── blocked sessions (scenario 1 only) ────────────────────────────────────────────────── */

    [Fact]
    public async Task Blocking_TwoHourShift_FiresOnTheTile_WhereTheWholeWindowMeanWouldNotHaveFired()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the blocking tile-behaviour test.");
        var ct = TestContext.Current.CancellationToken;

        const string serverName = "darling-pg-tile-blocking-shift-e2e";
        var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteBlockingRowsAsync(connection, serverId, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, serverId, serverName, "postgres", 18, ct);

            var windowStart = new DateTime(2026, 9, 30, 10, 0, 0, DateTimeKind.Unspecified);
            var windowEnd = windowStart.AddHours(4);
            var baselineStart = windowStart.AddDays(-21);
            await PlantDatabaseStatsSpanAsync(connection, serverId, serverName, baselineStart, windowEnd, ct);
            var totalMinutes = (int)(windowEnd - baselineStart).TotalMinutes;
            var shiftFromMinute = (int)(windowStart.AddHours(2) - baselineStart).TotalMinutes;

            const double mu = 2.0;
            const double sigma = 0.4;

            /* One SUCCESS collection_log row and one pg_blocking_edges row per minute (this family's own
               cadence — the window read pairs a distinct-minute log with a distinct-minute blocked count,
               never the 15-minute I/O grain), floored at 0 (blocked_sessions can't go negative), rounded so
               COUNT(DISTINCT blocked_pid) has an integer population to synthesize. */
            await PlantBlockingTimelineAsync(connection, serverId, serverName, baselineStart, totalMinutes,
                $"GREATEST(0, ROUND(CASE WHEN n >= {shiftFromMinute} THEN {mu + 6 * sigma} ELSE {mu} + {sigma} * sin(n) END))::int", ct);

            var context = Context(serverId, serverName, windowStart, windowEnd);
            var baselines = new PgTargetBaselineProvider(postgres);
            var detector = new PgTargetAnomalyDetector(postgres, baselines);
            var anomalies = await detector.DetectAnomaliesAsync(context);
            var anomaly = Assert.Single(anomalies, a => a.Key == PgTargetFactKeys.AnomalyBlocking);

            Assert.True(anomaly.Metadata.ContainsKey("tile_local_hour"), "the tile path did not fire (fell back to whole-window)");
            var tileHour = anomaly.Metadata["tile_local_hour"];
            Assert.True(tileHour is 12 or 13, $"tile_local_hour was {tileHour}, expected 12 or 13");
            Assert.Equal(4, anomaly.Metadata["tiles_scored"]);
            Assert.Equal(2, anomaly.Metadata["tiles_fired"]);
            // The blocked-sessions bucket carries robust statistics too (RobustTierScaffold): the modified-z cutoff
            // applies, not the classical default.
            var expectedBlockingCutoff = AnomalyThresholds.ModifiedZThresholdFor(MetricNames.PgBlockedSessions);
            Assert.Equal(expectedBlockingCutoff, anomaly.Metadata["fire_threshold"]);

            // dev's whole-window gate not firing is proven by the mutation check (EvaluateTiles → null), per #3653 B's test recipe.

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteBlockingRowsAsync(cleanup, serverId, cleanupCt));
        }
    }

    /* ───────────────────────── helpers ───────────────────────── */

    /// <summary>
    /// Plants one hourly <c>pg_database_stats</c> row per hour from <paramref name="start"/> through
    /// <paramref name="end"/> (inclusive) — the baseline-data gate's canary
    /// (<c>PgTargetAnomalyDetector.HasBaselineDataSql</c>), the one universal one-minute series every family's
    /// window read is gated behind. Without it <c>DetectAnomaliesAsync</c> returns no facts of ANY family,
    /// regardless of how the family's own table is planted.
    /// </summary>
    private static async Task PlantDatabaseStatsSpanAsync(
        NpgsqlConnection connection, int serverId, string serverName, DateTime start, DateTime end, CancellationToken ct)
    {
        var hours = (int)Math.Ceiling((end - start).TotalHours) + 1;
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_database_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset)
SELECT $1 + n, $2 + (n * interval '1 hour'), $3, $4, 'appdb', 1000 + n, 10, 100, 9000, 0, 0, 0, NULL
FROM generate_series(0, $5) AS n", connection) { CommandTimeout = 300 };
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(start);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(hours - 1);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static AnalysisContext Context(int serverId, string serverName, DateTime start, DateTime end) => new()
    {
        ServerId = serverId,
        ServerName = serverName,
        TimeRangeStart = start,
        TimeRangeEnd = end,
        ServerUtcOffset = TimeSpan.Zero,
        Coverage = new WindowCoverage { NominalMs = (long)(end - start).TotalMilliseconds, ObservedMs = (long)(end - start).TotalMilliseconds, SampleCount = (int)(end - start).TotalMinutes },
    };

    /// <summary>
    /// Plants one continuous, cumulative <c>pg_io_stats</c> timeline (one identity: client backend / relation /
    /// normal) from <paramref name="start"/> for <paramref name="ticks"/> ticks of <paramref name="minutesPerTick"/>
    /// minutes each. <paramref name="msPerReadExpr"/> is a SQL expression over the tick index <c>n</c> giving the
    /// TARGET ms-per-read at that tick; reads increment by <paramref name="readsPerTick"/> every tick (clearing
    /// the 250-read floor), and read_ms increments by <c>readsPerTick * msPerReadExpr</c> — so the detector's
    /// reset-aware per-sample differencing recovers exactly <paramref name="msPerReadExpr"/> at every tick.
    /// </summary>
    private static async Task PlantIoTimelineAsync(
        NpgsqlConnection connection, int serverId, string serverName, DateTime start, int ticks, long readsPerTick,
        string msPerReadExpr, CancellationToken ct, int minutesPerTick = MinutesPerTick)
    {
        using var command = new NpgsqlCommand($@"
WITH s AS (
    SELECT n, ({msPerReadExpr}) AS ms_per_read
    FROM generate_series(0, $5) AS n
),
c AS (
    SELECT n,
           $6::bigint * (n + 1)                                        AS reads,
           SUM($6::bigint * ms_per_read) OVER (ORDER BY n)              AS read_ms
    FROM s
)
INSERT INTO pg_io_stats
    (collection_id, collection_time, server_id, server_name, backend_type, object_type, context,
     reads, read_time_ms, writes, write_time_ms, stats_reset)
SELECT $1 + n, $2 + (n * $7 * interval '1 minute'), $3, $4, 'client backend', 'relation', 'normal',
       reads, read_ms, NULL::bigint, NULL::double precision, NULL
FROM c", connection) { CommandTimeout = 300 };
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(start);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(ticks - 1);
        command.Parameters.AddWithValue(readsPerTick);
        command.Parameters.AddWithValue(minutesPerTick);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteIoRowsAsync(NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_io_stats WHERE server_id = {serverId}; " +
            $"DELETE FROM pg_database_stats WHERE server_id = {serverId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {serverId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {serverId}; " +
            $"DELETE FROM servers WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Plants one continuous <c>pg_replication_stats</c> timeline (one standby, "replica-a") from
    /// <paramref name="start"/> for <paramref name="ticks"/> quarter-hour ticks. <paramref name="replayBytesExpr"/>
    /// is a SQL expression over the tick index <c>n</c> giving <c>replay_bytes_behind</c> directly — no
    /// differencing on this family's window read (<c>MAX(replay_bytes_behind)</c> per collection_time), so the
    /// planted value IS the read value.
    /// </summary>
    private static async Task PlantReplicationTimelineAsync(
        NpgsqlConnection connection, int serverId, string serverName, DateTime start, int ticks, string replayBytesExpr, CancellationToken ct)
    {
        using var command = new NpgsqlCommand($@"
INSERT INTO pg_replication_stats
    (collection_id, collection_time, server_id, server_name, application_name, client_addr, state, sync_state, sync_priority,
     sent_bytes_behind, write_bytes_behind, flush_bytes_behind, replay_bytes_behind, write_lag_ms, flush_lag_ms, replay_lag_ms, backend_start)
SELECT $1 + n, $2 + (n * interval '15 minutes'), $3, $4, 'replica-a', NULL, 'streaming', 'async', 0,
       65536, 65536, 65536, ({replayBytesExpr})::bigint, NULL, NULL, NULL, $2
FROM generate_series(0, $5) AS n", connection) { CommandTimeout = 300 };
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(start);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(ticks - 1);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteReplicationRowsAsync(NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_replication_stats WHERE server_id = {serverId}; " +
            $"DELETE FROM pg_database_stats WHERE server_id = {serverId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {serverId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {serverId}; " +
            $"DELETE FROM servers WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Plants one continuous minute-by-minute timeline for the blocked-sessions family: one SUCCESS
    /// <c>collection_log</c> row per minute (the "looked" side) and one <c>pg_blocking_edges</c> row per minute
    /// whose blocked_pid count is <paramref name="blockedCountExpr"/> — a SQL expression over the minute index
    /// <c>n</c> — synthesized as that many distinct blocked pids via a lateral <c>generate_series</c>.
    /// </summary>
    private static async Task PlantBlockingTimelineAsync(
        NpgsqlConnection connection, int serverId, string serverName, DateTime start, int minutes, string blockedCountExpr, CancellationToken ct)
    {
        using var logCommand = new NpgsqlCommand(@"
INSERT INTO collection_log (log_id, collection_time, server_id, server_name, collector_name, status, duration_ms, rows_collected, error_message)
SELECT $1 + n, $2 + (n * interval '1 minute'), $3, $4, 'pg_blocking', 'SUCCESS', 5, 0, NULL
FROM generate_series(0, $5) AS n", connection) { CommandTimeout = 300 };
        logCommand.Parameters.AddWithValue(CollectionIdGenerator.Next());
        logCommand.Parameters.AddWithValue(start);
        logCommand.Parameters.AddWithValue(serverId);
        logCommand.Parameters.AddWithValue(serverName);
        logCommand.Parameters.AddWithValue(minutes - 1);
        await logCommand.ExecuteNonQueryAsync(ct);

        using var edgeCommand = new NpgsqlCommand($@"
WITH minutes AS (
    SELECT n, ({blockedCountExpr}) AS blocked_count
    FROM generate_series(0, $5) AS n
)
INSERT INTO pg_blocking_edges
    (collection_id, collection_time, server_id, server_name, database_name,
     blocked_pid, blocked_application_name, blocked_username,
     blocking_pid, blocking_application_name, blocking_username, blocking_state)
SELECT $1 + m.n, $2 + (m.n * interval '1 minute'), $3, $4, 'appdb',
       9000 + k, 'app', 'app',
       9000, 'app', 'app', 'active'
FROM minutes AS m
CROSS JOIN LATERAL generate_series(1, m.blocked_count) AS k
WHERE m.blocked_count > 0", connection) { CommandTimeout = 300 };
        edgeCommand.Parameters.AddWithValue(CollectionIdGenerator.Next());
        edgeCommand.Parameters.AddWithValue(start);
        edgeCommand.Parameters.AddWithValue(serverId);
        edgeCommand.Parameters.AddWithValue(serverName);
        edgeCommand.Parameters.AddWithValue(minutes - 1);
        await edgeCommand.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteBlockingRowsAsync(NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_blocking_edges WHERE server_id = {serverId}; " +
            $"DELETE FROM pg_database_stats WHERE server_id = {serverId}; " +
            $"DELETE FROM collection_log WHERE server_id = {serverId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {serverId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {serverId}; " +
            $"DELETE FROM servers WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
