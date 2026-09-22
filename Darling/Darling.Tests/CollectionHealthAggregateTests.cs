/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
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

    /* ─────────── the composed reader (#3893 arm 2): live parity, the two guards, the late row ─────────── */

    /// <summary>Plants every CASE arm the eleven aggregates branch on, for 2 servers × 3 collectors every
    /// 17 minutes from eight days + 13 minutes ago to now (so rows straddle the seven-day head boundary at
    /// every minute offset and land in the last, never-materialized hour), plus two productive-then-stopped
    /// collectors (a zero-row SUCCESS streak; a flip to a named skip) and fleet-maintenance sentinel rows the
    /// read must exclude. The arm per row rotates through thirteen shapes: productive SUCCESS, empty SUCCESS,
    /// ERROR, SKIPPED, the three named skips, ABANDONED, the pre-#2803 abandoned-by-note SUCCESS at both
    /// shipped budgets, a SUCCESS with NULL rows_collected (the COALESCE path; <c>status</c> is NOT NULL, so
    /// the aggregates' <c>status IS NULL</c> arm is unreachable by any write), the empty-enumeration SUCCESS (NOT an abandonment) and YIELDED.</summary>
    private static async Task PlantEveryArmAsync(NpgsqlConnection connection, long idBase, CancellationToken ct)
    {
        await using var plant = new NpgsqlCommand($@"
WITH n AS (SELECT (now() AT TIME ZONE 'UTC') AS u),
arms (k, status, rows_collected, error_message) AS
(
    VALUES (0, 'SUCCESS'::text, 5, NULL::text),
           (1, 'SUCCESS', 0, NULL),
           (2, 'ERROR', NULL, 'boom'),
           (3, 'SKIPPED', 0, NULL),
           (4, '{CollectorRuntimePrecondition.DegradedStatus}', 0, 'denied'),
           (5, '{CollectorRuntimePrecondition.ExtensionMissingStatus}', 0, 'no extension'),
           (6, '{CollectorRuntimePrecondition.CaptureSessionMissingStatus}', 0, 'no session'),
           (7, '{EnumeratedCollectorDriver.AbandonedStatus}', 0, '{EnumeratedCollectorDriver.WholeCycleBudgetNote(120)}'),
           (8, 'SUCCESS', 0, '{EnumeratedCollectorDriver.WholeCycleBudgetNote(120)}'),
           (9, 'SUCCESS', 0, '{EnumeratedCollectorDriver.WholeCycleBudgetNote(600)}'),
           (10, 'SUCCESS', NULL, NULL),
           (11, 'SUCCESS', 0, '{EnumeratedCollectorDriver.EmptyEnumerationMessage}'),
           (12, 'YIELDED', 0, NULL)
)
INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected, error_message)
SELECT {idBase} + row_number() OVER (), p.server_id, 'srv-' || p.server_id, p.collector_name, p.t, 1, p.status, p.rows_collected, p.error_message
FROM
(
    SELECT s AS server_id, 'collector_' || c AS collector_name, n.u - INTERVAL '8 days 13 minutes' + g * INTERVAL '17 minutes' AS t,
           a.status, a.rows_collected, a.error_message
    FROM n
    CROSS JOIN generate_series(1, 2) s
    CROSS JOIN generate_series(1, 3) c
    CROSS JOIN generate_series(0, 700) g
    JOIN arms a ON a.k = (g + s + c) % 13
    UNION ALL
    SELECT s, x.collector, t,
           CASE WHEN x.collector = 'collector_streak' OR t < n.u - INTERVAL '1 day' THEN 'SUCCESS' ELSE '{CollectorRuntimePrecondition.CaptureSessionMissingStatus}' END,
           CASE WHEN x.collector = 'collector_streak' THEN CASE WHEN t < n.u - INTERVAL '2 days' THEN 9 ELSE 0 END
                WHEN t < n.u - INTERVAL '1 day' THEN 4 ELSE 0 END,
           NULL
    FROM n
    CROSS JOIN generate_series(1, 2) s
    CROSS JOIN (VALUES ('collector_streak'), ('collector_skipflip')) x (collector)
    CROSS JOIN LATERAL generate_series(n.u - INTERVAL '8 days', n.u - INTERVAL '1 minute', INTERVAL '20 minutes') t
    UNION ALL
    SELECT 0, 'fleet_maintenance', t, 'ERROR', 0, NULL
    FROM n CROSS JOIN LATERAL generate_series(n.u - INTERVAL '6 days', n.u, INTERVAL '6 hours') t
) p
WHERE p.t < (SELECT u FROM n)", connection);
        await plant.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Rows that land AFTER the policy run, above its watermark (served real-time): an existing pair
    /// and a brand-new collector.</summary>
    private static async Task PlantAboveWatermarkAsync(NpgsqlConnection connection, long idBase, CancellationToken ct)
    {
        await using var plant = new NpgsqlCommand($@"
INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
VALUES ({idBase}, 1, 'srv-1', 'collector_1', (now() AT TIME ZONE 'UTC') - INTERVAL '5 seconds', 1, 'ERROR', NULL),
       ({idBase + 1}, 2, 'srv-2', 'collector_late', (now() AT TIME ZONE 'UTC') - INTERVAL '1 second', 1, 'SUCCESS', 3)", connection);
        await plant.ExecuteNonQueryAsync(ct);
    }

    /// <summary>One statement's result, keyed (server, collector), every one of the thirteen ordinals rendered
    /// invariantly (timestamps to the tick, so a MAX that loses a microsecond cannot compare equal).</summary>
    private static async Task<SortedDictionary<string, string>> ReadRowsAsync(
        NpgsqlDataSource postgres, string sql, DateTime windowStart, DateTime? headEnd, CancellationToken ct)
    {
        await using var command = postgres.CreateCommand(sql);
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = windowStart });
        if (headEnd is { } h)
        {
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = h });
        }
        var rows = new SortedDictionary<string, string>(StringComparer.Ordinal);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.Equal(13, reader.FieldCount);
        while (await reader.ReadAsync(ct))
        {
            var fields = Enumerable.Range(0, 13).Select(i => reader.GetName(i) + "=" + (reader.IsDBNull(i)
                ? "NULL"
                : reader.GetValue(i) switch
                {
                    DateTime d => d.Ticks.ToString(CultureInfo.InvariantCulture),
                    var v => Convert.ToString(v, CultureInfo.InvariantCulture),
                }));
            rows.Add(reader.GetInt32(0) + "|" + reader.GetString(1), string.Join(" ", fields));
        }
        return rows;
    }

    /// <summary>The product's banded fleet payload, through the product's own chooser
    /// (<c>ReadFailingCollectorCountsAsync</c> is private; reached by reflection so the test takes no seam).</summary>
    private static async Task<Dictionary<int, DarlingFleetReader.CollectorCounts>> ReadBandedAsync(
        NpgsqlDataSource postgres, DateTime now, CancellationToken ct)
    {
        var method = typeof(DarlingFleetReader).GetMethod("ReadFailingCollectorCountsAsync", BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException("ReadFailingCollectorCountsAsync not found");
        return await (Task<Dictionary<int, DarlingFleetReader.CollectorCounts>>)method.Invoke(null, [postgres, now, ct])!;
    }

    private static void AssertSameRows(SortedDictionary<string, string> raw, SortedDictionary<string, string> composed)
    {
        var onlyRaw = raw.Where(r => !composed.TryGetValue(r.Key, out var c) || c != r.Value).Select(r => "raw  " + r.Key + " " + r.Value);
        var onlyComposed = composed.Where(c => !raw.TryGetValue(c.Key, out var r) || r != c.Value).Select(c => "comp " + c.Key + " " + c.Value);
        var diff = onlyRaw.Concat(onlyComposed).ToList();
        Assert.True(diff.Count == 0, $"composed read differs from the raw scan in {diff.Count} row(s):\n" + string.Join("\n", diff));
    }

    private static DateTime NaiveUtcNow() => DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

    private static async Task DropAggregateAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var drop = new NpgsqlCommand($"DROP MATERIALIZED VIEW collect.{TimescaleSupport.CollectionHealthHourlyView}", connection);
        await drop.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// THE PARITY PROOF. Eight days of every CASE arm, materialized by the PRODUCT's first policy run (no hand
    /// backfill), then rows landing above the watermark: the composed read (whole buckets + raw head slice)
    /// EQUALS the raw scan per (server, collector) across all thirteen ordinals, the guard says composed, and
    /// the banded fleet payload the product computes through the composed path is identical to the one it
    /// computes through the raw path (the aggregate dropped, so the chooser falls back).
    /// </summary>
    [Fact]
    public async Task ComposedRead_EqualsRawScan_AcrossEveryCaseArm_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = await OpenStoreAsync(ct);
        Assert.SkipWhen(store is null, "Set DARLING_TEST_PG to a Postgres connection string with TimescaleDB to run the live parity test.");
        var (scratch, connection, jobId) = store!.Value;
        await using var _s = scratch;
        await using var _c = connection;
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        await PlantEveryArmAsync(connection, 10_000_000, ct);
        await RunPolicyAsync(connection, jobId, ct);
        await PlantAboveWatermarkAsync(connection, 19_000_000, ct);

        var now = NaiveUtcNow();
        var windowStart = now.AddDays(-7);
        var headEnd = DarlingFleetReader.CeilingHour(windowStart);
        await using (var mark = new NpgsqlCommand(DarlingFleetReader.CollectionHealthWatermarkSql, connection))
        {
            /* The first run materialized the window: the watermark is finite and well past the head hour, so
               the composed read below really is served from buckets, not all real-time. */
            var watermark = Assert.IsType<DateTime>(await mark.ExecuteScalarAsync(ct));
            Assert.True(watermark > headEnd.AddDays(6), $"watermark {watermark:O} is not past the materialized week");
        }
        Assert.True(await DarlingFleetReader.CollectionHealthRollupUsableAsync(postgres, headEnd, ct));

        var raw = await ReadRowsAsync(postgres, DarlingFleetReader.FleetCollectionHealthSql, windowStart, null, ct);
        var composed = await ReadRowsAsync(postgres, DarlingFleetReader.FleetCollectionHealthComposedSql, windowStart, headEnd, ct);
        Assert.Equal(2 * 5 + 1, raw.Count); // 2 servers × (3 rotating + streak + skipflip) + collector_late; sentinel excluded
        AssertSameRows(raw, composed);

        /* Every arm is actually exercised (a parity over zeros proves nothing). */
        var all = string.Join(" ", raw.Values);
        foreach (var column in new[] { "error_count", "permission_denied_count", "abandoned_count", "extension_missing_count" })
        {
            Assert.Contains(column + "=", all, StringComparison.Ordinal);
            Assert.DoesNotContain(raw.Values, v => v.Contains("collector_1 ", StringComparison.Ordinal) && v.Contains(column + "=0 ", StringComparison.Ordinal));
        }

        var bandedComposed = await ReadBandedAsync(postgres, now, ct);
        await DropAggregateAsync(connection, ct);
        Assert.False(await DarlingFleetReader.CollectionHealthRollupUsableAsync(postgres, headEnd, ct));
        var bandedRaw = await ReadBandedAsync(postgres, now, ct);
        Assert.Equal(bandedRaw.OrderBy(kv => kv.Key), bandedComposed.OrderBy(kv => kv.Key));
        Assert.Equal(2, bandedRaw.Count);
    }

    /// <summary>
    /// THE CONTINUITY GUARD. A whole hour missing below the watermark — here engineered by deleting one
    /// materialized bucket's rows from the materialization hypertable, the shape an interrupted or hand-run
    /// narrow refresh leaves — is a HOLE the composed read would serve short (asserted: forced, it differs
    /// from raw). The guard sees the missing bucket, the chooser falls back to the raw scan, and the product's
    /// banded payload equals the raw path's.
    /// </summary>
    [Fact]
    public async Task ContinuityGuard_HourMissingBelowWatermark_FallsBackToRaw_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = await OpenStoreAsync(ct);
        Assert.SkipWhen(store is null, "Set DARLING_TEST_PG to a Postgres connection string with TimescaleDB to run the live continuity-guard test.");
        var (scratch, connection, jobId) = store!.Value;
        await using var _s = scratch;
        await using var _c = connection;
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        await PlantEveryArmAsync(connection, 20_000_000, ct);
        await RunPolicyAsync(connection, jobId, ct);

        string materialization;
        await using (var find = new NpgsqlCommand(
            "SELECT format('%I.%I', materialization_hypertable_schema, materialization_hypertable_name) " +
            "FROM timescaledb_information.continuous_aggregates WHERE view_schema = 'collect' AND view_name = '" +
            TimescaleSupport.CollectionHealthHourlyView + "'", connection))
        {
            materialization = (string)(await find.ExecuteScalarAsync(ct))!;
        }
        await using (var hole = new NpgsqlCommand(
            $"DELETE FROM {materialization} WHERE bucket = date_trunc('hour', (now() AT TIME ZONE 'UTC') - INTERVAL '3 days')", connection))
        {
            Assert.True(await hole.ExecuteNonQueryAsync(ct) > 0);
        }

        var now = NaiveUtcNow();
        var windowStart = now.AddDays(-7);
        var headEnd = DarlingFleetReader.CeilingHour(windowStart);
        var raw = await ReadRowsAsync(postgres, DarlingFleetReader.FleetCollectionHealthSql, windowStart, null, ct);
        var forced = await ReadRowsAsync(postgres, DarlingFleetReader.FleetCollectionHealthComposedSql, windowStart, headEnd, ct);
        Assert.NotEqual(raw, forced); // the hole is real: served from buckets, the read is short

        Assert.False(await DarlingFleetReader.CollectionHealthRollupUsableAsync(postgres, headEnd, ct));
        var banded = await ReadBandedAsync(postgres, now, ct);
        await DropAggregateAsync(connection, ct);
        var bandedRaw = await ReadBandedAsync(postgres, now, ct);
        Assert.Equal(bandedRaw.OrderBy(kv => kv.Key), banded.OrderBy(kv => kv.Key));
    }

    /// <summary>THE ABSENT GUARD. No aggregate (plain PostgreSQL, or not yet created): the probe says so, the
    /// chooser never names the view (a relation in a statement resolves at parse time), and the raw scan
    /// serves the read.</summary>
    [Fact]
    public async Task AbsentAggregate_ReadsRaw_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = await OpenStoreAsync(ct);
        Assert.SkipWhen(store is null, "Set DARLING_TEST_PG to a Postgres connection string with TimescaleDB to run the live absent-aggregate test.");
        var (scratch, connection, jobId) = store!.Value;
        await using var _s = scratch;
        await using var _c = connection;
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        await PlantEveryArmAsync(connection, 30_000_000, ct);
        await DropAggregateAsync(connection, ct);

        var now = NaiveUtcNow();
        var headEnd = DarlingFleetReader.CeilingHour(now.AddDays(-7));
        await using (var probe = new NpgsqlCommand(DarlingFleetReader.CollectionHealthRollupProbeSql, connection))
        {
            Assert.False(Assert.IsType<bool>(await probe.ExecuteScalarAsync(ct)));
        }
        Assert.False(await DarlingFleetReader.CollectionHealthRollupUsableAsync(postgres, headEnd, ct));
        var banded = await ReadBandedAsync(postgres, now, ct);
        Assert.Equal(2, banded.Count);
        Assert.All(banded.Values, c => Assert.Equal(5, c.Total));
    }

    /// <summary>
    /// A LATE ROW BELOW THE WATERMARK — the one staleness the design accepts, stated here as behaviour. A row
    /// written after a refresh with a collection_time below the watermark lands in a bucket that already
    /// exists, so the continuity guard (which counts buckets, not rows) does NOT see it: until the next policy
    /// run the composed read UNDERCOUNTS that pair by the late row (and the guard still says composed). The
    /// insert logged an invalidation for its bucket; the next run re-materializes exactly that range and the
    /// read is exact again. Bound: one refresh interval (an hour) of staleness, for rows the product never
    /// routinely writes (every <c>collection_log</c> writer stamps the insert-time clock).
    /// </summary>
    [Fact]
    public async Task LateRowBelowWatermark_UndercountsUntilTheNextRun_ThenHeals_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = await OpenStoreAsync(ct);
        Assert.SkipWhen(store is null, "Set DARLING_TEST_PG to a Postgres connection string with TimescaleDB to run the live late-row test.");
        var (scratch, connection, jobId) = store!.Value;
        await using var _s = scratch;
        await using var _c = connection;
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        await PlantEveryArmAsync(connection, 40_000_000, ct);
        await RunPolicyAsync(connection, jobId, ct);
        await using (var late = new NpgsqlCommand(@"
INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected, error_message)
VALUES (49000000, 1, 'srv-1', 'collector_2', (now() AT TIME ZONE 'UTC') - INTERVAL '2 days', 1, 'ERROR', NULL, 'late')", connection))
        {
            await late.ExecuteNonQueryAsync(ct);
        }

        var now = NaiveUtcNow();
        var windowStart = now.AddDays(-7);
        var headEnd = DarlingFleetReader.CeilingHour(windowStart);
        var raw = await ReadRowsAsync(postgres, DarlingFleetReader.FleetCollectionHealthSql, windowStart, null, ct);
        var before = await ReadRowsAsync(postgres, DarlingFleetReader.FleetCollectionHealthComposedSql, windowStart, headEnd, ct);
        Assert.True(await DarlingFleetReader.CollectionHealthRollupUsableAsync(postgres, headEnd, ct)); // guard is blind to it, by design
        var stale = raw.Keys.Where(k => raw[k] != before[k]).ToList();
        Assert.Equal(new[] { "1|collector_2" }, stale);
        Assert.Contains("total_runs=", before[stale[0]], StringComparison.Ordinal);
        Assert.Equal(TotalRuns(raw[stale[0]]) - 1, TotalRuns(before[stale[0]]));

        await RunPolicyAsync(connection, jobId, ct);
        var after = await ReadRowsAsync(postgres, DarlingFleetReader.FleetCollectionHealthComposedSql, windowStart, headEnd, ct);
        AssertSameRows(raw, after);
    }

    private static long TotalRuns(string row) =>
        long.Parse(row.Split(' ').Single(f => f.StartsWith("total_runs=", StringComparison.Ordinal))["total_runs=".Length..], CultureInfo.InvariantCulture);
}
