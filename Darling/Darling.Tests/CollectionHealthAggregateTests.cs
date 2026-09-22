/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The fleet collection-health rollup's off-grid registration (#3893 arm 2): what it must never take (a grid
/// minute, a compression target) and the one bound its cadence must never cross (the real-time tail reaching
/// compressed <c>collection_log</c>).
/// </summary>
public class CollectionHealthAggregateTests
{
    /// <summary>The tail a read serves from raw must stay well inside uncompressed data: if a cadence or offset
    /// change let its worst-case age reach <see cref="TimescaleSupport.CompressAfterDays"/>, every read would
    /// decompress again. "Well under" is held to a quarter of compress_after.</summary>
    [Fact]
    public void TailWorstCaseAge_StaysWellUnderCompressAfter()
    {
        var compressAfter = TimeSpan.FromDays(TimescaleSupport.CompressAfterDays);
        Assert.Equal(TimeSpan.FromHours(4), TimescaleSupport.CollectionHealthTailWorstCaseAge);
        Assert.True(TimescaleSupport.CollectionHealthTailWorstCaseAge * 4 <= compressAfter,
            $"worst-case tail {TimescaleSupport.CollectionHealthTailWorstCaseAge} is not well under compress_after {compressAfter}");
    }

    /// <summary>THE LOAD-BEARING GUARD AGAINST THE WATERMARK HOLE RETURNING (#3893): a refresh window that starts
    /// after the watermark jumps it past unrefreshed hours and strands them forever; the window must reach back
    /// over the whole consumer window plus the head bucket, so every run starts below the watermark.</summary>
    [Fact]
    public void RefreshStart_CoversTheConsumerWindowPlusABucket()
    {
        Assert.Equal(TimeSpan.FromDays(7), TimescaleSupport.CollectionHealthConsumerWindow);
        Assert.True(TimescaleSupport.CollectionHealthRefreshStartSpan >= TimescaleSupport.CollectionHealthConsumerWindow + TimescaleSupport.HourlyBucket,
            $"start_offset {TimescaleSupport.CollectionHealthRefreshStartSpan} is narrower than the consumer window + a bucket: the watermark hole returns");
    }

    [Fact]
    public void Twins_MatchTheirIntervalText()
    {
        Assert.Equal("8 days", TimescaleSupport.CollectionHealthRefreshStartOffset);
        Assert.Equal(TimeSpan.FromDays(8), TimescaleSupport.CollectionHealthRefreshStartSpan);
        Assert.Equal("1 hour", TimescaleSupport.CollectionHealthRefreshScheduleInterval);
        Assert.Equal(TimeSpan.FromHours(1), TimescaleSupport.CollectionHealthRefreshScheduleSpan);
        Assert.Equal("8 days", TimescaleSupport.CollectionHealthRetentionInterval);
        Assert.Equal(TimeSpan.FromDays(8), TimescaleSupport.CollectionHealthRetentionSpan);
        /* Retention covers the 7-day consumer + a bucket, and is never shorter than the refresh window. */
        Assert.True(TimescaleSupport.CollectionHealthRetentionSpan >= TimescaleSupport.CollectionHealthConsumerWindow + TimescaleSupport.HourlyBucket);
        Assert.True(TimescaleSupport.CollectionHealthRetentionSpan >= TimescaleSupport.CollectionHealthRefreshStartSpan);
    }

    /// <summary>Off the grid by construction: no initial_start (finish-to-start), and on none of the lists the
    /// grid, the converges and the compression band derive from.</summary>
    [Fact]
    public void Policy_IsOffGrid_AndOnNoRegistryList()
    {
        var sql = TimescaleSupport.AddCollectionHealthRefreshPolicySql();
        Assert.DoesNotContain("initial_start", sql, StringComparison.Ordinal);
        Assert.Contains("start_offset => INTERVAL '8 days', end_offset => INTERVAL '1 hour', schedule_interval => INTERVAL '1 hour'", sql, StringComparison.Ordinal);
        var view = TimescaleSupport.CollectionHealthHourlyView;
        Assert.DoesNotContain(view, TimescaleSupport.HourlyRefreshPhaseOrder);
        Assert.DoesNotContain(TimescaleSupport.HourlyAggregates, a => a.View == view);
        Assert.DoesNotContain(TimescaleSupport.DailyAggregates, a => a.View == view);
        Assert.DoesNotContain(TimescaleSupport.BaselineAggregates, a => a.View == view);
        Assert.DoesNotContain(TimescaleSupport.AggregateCompressionTargets, a => a.View == view);
        Assert.Single(TimescaleSupport.OffGridAggregates, a => a.View == view);
        Assert.Single(TimescaleSupport.RetentionPolicies, p => p.Relation == view);
    }

    /* ─────────── the watermark hole (#3893): live, through the PRODUCT's ensure path + a policy run ─────────── */

    /// <summary>Builds a scratch store the product's way: migrations, TimescaleDB, the collection_log
    /// hypertable, then <see cref="TimescaleSupport.EnsureContinuousAggregatesAsync"/> — which creates the
    /// aggregate WITH NO DATA and adds its policy. The policy's background schedule is switched off so the
    /// scheduler cannot race the test's own <c>CALL run_job</c>; returns the job id. NO hand
    /// <c>refresh_continuous_aggregate</c> backfill anywhere — a manual backfill is exactly what hid the
    /// hole.</summary>
    internal static async Task<(ScratchPostgres Scratch, NpgsqlConnection Connection, int JobId)?> OpenStoreAsync(CancellationToken ct)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        if (string.IsNullOrEmpty(baseConnectionString))
        {
            return null;
        }
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        if (!await TimescaleSupport.TryEnableAsync(connection, null, ct))
        {
            await connection.DisposeAsync();
            await scratch.DisposeAsync();
            return null;
        }
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        await using var job = new NpgsqlCommand(
            "SELECT job_id FROM timescaledb_information.jobs WHERE hypertable_schema = 'collect' AND hypertable_name = '" +
            TimescaleSupport.CollectionHealthHourlyView + "' AND proc_name = 'policy_refresh_continuous_aggregate'", connection);
        var jobId = Convert.ToInt32(await job.ExecuteScalarAsync(ct));
        await using (var off = new NpgsqlCommand($"SELECT alter_job({jobId}, scheduled => false)", connection))
        {
            await off.ExecuteNonQueryAsync(ct);
        }
        return (scratch, connection, jobId);
    }

    /// <summary>Plants one run every <paramref name="everyMinutes"/> for 2 servers × 3 collectors over
    /// [<paramref name="fromAgo"/>, <paramref name="toAgo"/>) before now, set-based.</summary>
    internal static async Task PlantAsync(NpgsqlConnection connection, TimeSpan fromAgo, TimeSpan toAgo, int everyMinutes, long idBase, CancellationToken ct)
    {
        await using var plant = new NpgsqlCommand($@"
INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT {idBase} + row_number() OVER (), s, 'srv-' || s, 'collector_' || c, t, 1, 'SUCCESS', 5
FROM generate_series(1, 2) s
CROSS JOIN generate_series(1, 3) c
CROSS JOIN generate_series((now() AT TIME ZONE 'UTC') - INTERVAL '{(long)fromAgo.TotalMinutes} minutes',
                           (now() AT TIME ZONE 'UTC') - INTERVAL '{(long)toAgo.TotalMinutes} minutes' - INTERVAL '1 second',
                           INTERVAL '{everyMinutes} minutes') t", connection);
        await plant.ExecuteNonQueryAsync(ct);
    }

    internal static async Task RunPolicyAsync(NpgsqlConnection connection, int jobId, CancellationToken ct)
    {
        await using var run = new NpgsqlCommand($"CALL run_job({jobId})", connection);
        await run.ExecuteNonQueryAsync(ct);
    }

    /// <summary>(runs the aggregate serves over the 7-day consumer window, runs raw holds there).</summary>
    private static async Task<(long Aggregate, long Raw)> WindowRunsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand($@"
SELECT (SELECT COALESCE(SUM(total_runs), 0) FROM collect.{TimescaleSupport.CollectionHealthHourlyView}
        WHERE bucket >= date_trunc('hour', (now() AT TIME ZONE 'UTC') - INTERVAL '7 days')),
       (SELECT count(*) FROM collect.collection_log
        WHERE collection_time >= date_trunc('hour', (now() AT TIME ZONE 'UTC') - INTERVAL '7 days') AND server_id <> 0)", connection);
        await using var reader = await read.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return (Convert.ToInt64(reader.GetValue(0)), Convert.ToInt64(reader.GetValue(1)));
    }

    /// <summary>
    /// THE INSTALL HOLE. Before the first refresh the aggregate is EXACT, not holed: nothing is materialized,
    /// so the whole window sits above the watermark and is served real-time from raw. The defect was the
    /// FIRST policy run: a narrow window [now-3h, now-1h] moved the watermark to ~now-1h and stranded three
    /// days of history below it, never materialized. With start_offset ≥ the consumer window, that first run
    /// IS the backfill and the aggregate stays exact.
    /// </summary>
    [Fact]
    public async Task InstallHole_FirstPolicyRunAfterEnsure_StrandsNoHistory_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = await OpenStoreAsync(ct);
        Assert.SkipWhen(store is null, "Set DARLING_TEST_PG to a Postgres connection string with TimescaleDB to run the live watermark-hole test.");
        var (scratch, connection, jobId) = store!.Value;
        await using var _s = scratch;
        await using var _c = connection;

        await PlantAsync(connection, TimeSpan.FromDays(3), TimeSpan.FromMinutes(20), 20, 1_000_000, ct);
        var before = await WindowRunsAsync(connection, ct);
        Assert.True(before.Raw > 1000);
        Assert.Equal(before.Raw, before.Aggregate); // empty aggregate = all real-time = exact

        await RunPolicyAsync(connection, jobId, ct);
        var after = await WindowRunsAsync(connection, ct);
        Assert.Equal(after.Raw, after.Aggregate);
    }

    /// <summary>
    /// THE OUTAGE HOLE. The aggregate is current to T0 = now-8h (the last run before the outage, stood in for
    /// by one bounded refresh of the narrow policy's own window shape at T0); rows keep landing for seven
    /// hours with no run; then the policy runs. A window that starts AFTER the watermark jumps it past the
    /// unrefreshed gap and those hours are never materialized. A window that starts at or before the
    /// watermark cannot.
    /// </summary>
    [Fact]
    public async Task OutageHole_PolicyRunAfterMissedRuns_JumpsNoGap_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = await OpenStoreAsync(ct);
        Assert.SkipWhen(store is null, "Set DARLING_TEST_PG to a Postgres connection string with TimescaleDB to run the live watermark-hole test.");
        var (scratch, connection, jobId) = store!.Value;
        await using var _s = scratch;
        await using var _c = connection;

        await PlantAsync(connection, TimeSpan.FromHours(10), TimeSpan.FromHours(8), 10, 2_000_000, ct);
        await using (var preOutage = new NpgsqlCommand($@"
CALL refresh_continuous_aggregate('collect.{TimescaleSupport.CollectionHealthHourlyView}',
    date_trunc('hour', (now() AT TIME ZONE 'UTC') - INTERVAL '11 hours'),
    date_trunc('hour', (now() AT TIME ZONE 'UTC') - INTERVAL '8 hours'))", connection))
        {
            await preOutage.ExecuteNonQueryAsync(ct);
        }
        await PlantAsync(connection, TimeSpan.FromHours(8), TimeSpan.FromMinutes(5), 10, 3_000_000, ct);
        var before = await WindowRunsAsync(connection, ct);
        Assert.Equal(before.Raw, before.Aggregate); // gap is above the watermark: served real-time, exact

        await RunPolicyAsync(connection, jobId, ct);
        var after = await WindowRunsAsync(connection, ct);
        Assert.Equal(after.Raw, after.Aggregate);
    }
}
