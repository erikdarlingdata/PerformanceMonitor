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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3953's live pass for the Queries slicer (lane B3b), mirroring <see cref="QueryStoreIntervalWideGridLiveTests"/>'s
/// shape: <see cref="ViewerDataService.QueryStoreSlicerTableSql"/>, the slicer's own gate call convention
/// (<see cref="QueryStoreIntervalWide.ReadsTableAsync"/> with <c>windowStart - 1 hour</c> and <c>minWindow:
/// TimeSpan.Zero</c>, since the slicer enforces its own threshold — <see cref="ViewerDataService.QueryStoreSlicerMinWindow"/>
/// — separately, on the unwidened window), and the defensive legacy-row clause
/// (<see cref="ViewerDataService.QueryStoreSlicerHasLegacyRowSql"/>, private but exercised here through the base
/// seed, which leaves <c>interval_start_time_utc</c> null throughout).
///
/// Reuses <see cref="QueryStoreIntervalWideGridLiveTests.SeedGridAsync"/> for the base seed and adds two rows of
/// its own for the slicer-specific edge cases the #3953 slicer lane brief calls out: an interval that starts
/// before the window but is collected inside it, and one that starts inside the window but is last collected
/// after it (ruling issuecomment-5836972848).
/// </summary>
/* #1776 own-store: see QueryStoreIntervalWideGridLiveTests for why this is deliberately not [Collection("live-postgres")]. */
public sealed class QueryStoreIntervalWideSlicerLiveTests
{
    private const int ServerId = -3953931;

    private static readonly DateTime WindowStart = new(2026, 9, 15, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WindowEnd = WindowStart.AddDays(3);

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheTableRead_EqualsRaw_OpenEndAndLiteralEndAtAppliedThrough_EndToEnd()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #3953 slicer live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());

        await QueryStoreIntervalWideGridLiveTests.SeedGridAsync(runner, ServerId, WindowStart, ct);
        await SeedSlicerEdgeCasesAsync(runner, ServerId, ct);
        await ForceFilledSinceAsync(connection, WindowStart.AddDays(-1), ct);

        var appliedThrough = await ScalarDateTimeAsync(connection,
            "SELECT applied_through FROM collect.query_store_interval_wide_coverage WHERE server_id = @server_id", ct);

        var (useTableOpen, clampOpen) = await QueryStoreIntervalWide.ReadsTableAsync(
            connection, ServerId, WindowStart - TimeSpan.FromHours(1), WindowEnd, null, TimeSpan.Zero, 30, null, ct);
        Assert.True(useTableOpen, "expected the gate to pick the table for the seeded, forced-covered window");
        await AssertRawEqualsTableAsync(connection, WindowStart, WindowEnd, clampOpen, null, ct);

        var (useTableLiteral, clampLiteral) = await QueryStoreIntervalWide.ReadsTableAsync(
            connection, ServerId, WindowStart - TimeSpan.FromHours(1), WindowEnd, appliedThrough, TimeSpan.Zero, 30, null, ct);
        Assert.True(useTableLiteral, "expected the gate to pick the table when the literal end exactly matches applied_through");
        await AssertRawEqualsTableAsync(connection, WindowStart, WindowEnd, clampLiteral, appliedThrough, ct);

        /* End to end: ViewerDataService.GetQueryStoreSlicerDataAsync, through its own schema-version probe and
           the legacy-row clause (the base seed's rows are all null-interval, so this end-to-end call proves the
           defensive clause does not break correctness even while it is choosing raw), returns exactly raw's own
           bucket set. */
        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        var endToEndBuckets = await viewer.GetQueryStoreSlicerDataAsync(ServerId, WindowStart, WindowEnd);
        var rawBuckets = await RawSlicerBucketsAsync(connection, WindowStart, WindowEnd, ct);
        Assert.True(rawBuckets.Count > 0, "the seed produced no raw buckets; the end-to-end comparison would be vacuous");
        Assert.Equal(rawBuckets, endToEndBuckets
            .Select(b => (b.BucketTime, b.SessionCount, b.TotalCpu, b.TotalElapsed, b.TotalReads, b.TotalWrites, b.TotalPhysicalReads))
            .OrderBy(b => b.BucketTime)
            .ToList());
    }

    /// <summary>
    /// The three refusal clauses the shared gate owns (pending, literal end before applied_through), called
    /// exactly as <see cref="ViewerDataService"/>'s private slicer table method calls them (<c>windowStart - 1
    /// hour</c>, <c>minWindow: TimeSpan.Zero</c>), then the slicer's OWN threshold clause — which lives outside
    /// <see cref="QueryStoreIntervalWide.ReadsTableAsync"/> and so cannot be proven by calling it directly — via a
    /// row written straight into the wide table for an identity raw does not have at all: if the table is read,
    /// it shows up; if raw is read (the sub-threshold window must force that), it does not.
    /// </summary>
    [Fact]
    public async Task Gate_RefusesPendingAndLiteralEndBeforeAppliedThrough_AndEnforcesItsOwnThresholdSeparately()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #3953 slicer gate test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());

        await SeedSlicerEdgeCasesAsync(runner, ServerId, ct);
        await ForceFilledSinceAsync(connection, WindowStart.AddDays(-1), ct);
        var appliedThrough = await ScalarDateTimeAsync(connection,
            "SELECT applied_through FROM collect.query_store_interval_wide_coverage WHERE server_id = @server_id", ct);

        Assert.False((await QueryStoreIntervalWide.ReadsTableAsync(
            connection, ServerId, WindowStart - TimeSpan.FromHours(1), WindowEnd, appliedThrough.AddMinutes(-1), TimeSpan.Zero, 30, null, ct)).UseTable,
            "a literal end before applied_through must read raw");

        await ExecAsync(connection,
            "INSERT INTO collect.query_store_interval_wide_pending (server_id, collection_time, database_name, recorded_at) VALUES (@server_id, @cutoff, 'qsA', @cutoff)",
            WindowStart, ct);
        Assert.False((await QueryStoreIntervalWide.ReadsTableAsync(
            connection, ServerId, WindowStart - TimeSpan.FromHours(1), WindowEnd, null, TimeSpan.Zero, 30, null, ct)).UseTable,
            "a pending batch must read raw regardless of coverage");
        await ExecAsync(connection, "DELETE FROM collect.query_store_interval_wide_pending WHERE server_id = @server_id", ct);

        Assert.True((await QueryStoreIntervalWide.ReadsTableAsync(
            connection, ServerId, WindowStart - TimeSpan.FromHours(1), WindowEnd, null, TimeSpan.Zero, 30, null, ct)).UseTable,
            "the passing case, restored, must read the table again");

        /* The sinkhole row: an identity that exists ONLY in the wide table, never in raw. */
        var sinkholeBucket = WindowEnd.AddHours(-2);
        await ExecAsync(connection, @"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time,
 last_execution_time, execution_count, avg_duration_us, avg_cpu_time_us, runtime_stats_interval_id, interval_start_time_utc)
VALUES
(@cutoff, @server_id, 'qsSinkhole', 999999, 999999, 'Regular', @cutoff, @cutoff, 1, 1000, 1000, 999999, @cutoff);",
            sinkholeBucket, ct);

        await using var viewer = new ViewerDataService(scratch.ConnectionString);

        /* Sub-threshold window (well under QueryStoreSlicerMinWindow): every OTHER gate clause here would pass
           (coverage forced, no pending row, open end), so if the slicer's own threshold were not enforced
           separately, the sinkhole row would leak into this result. It must not. */
        var underThreshold = await viewer.GetQueryStoreSlicerDataAsync(ServerId, sinkholeBucket.AddHours(-1), WindowEnd);
        Assert.DoesNotContain(underThreshold, b => b.SessionCount > 0 && b.BucketTime == new DateTime(sinkholeBucket.Year, sinkholeBucket.Month, sinkholeBucket.Day, sinkholeBucket.Hour, 0, 0));
        Assert.True(WindowEnd - sinkholeBucket.AddHours(-1) < ViewerDataService.QueryStoreSlicerMinWindow, "sanity: the probe window must actually be under the slicer's own threshold");

        /* At/above the threshold, with the sinkhole's own bucket covered: the table is read and the sinkhole row
           shows up, proving the gate positively picks the table once every clause (including this one) holds. */
        var atThreshold = await viewer.GetQueryStoreSlicerDataAsync(ServerId, sinkholeBucket.Add(-ViewerDataService.QueryStoreSlicerMinWindow), WindowEnd);
        Assert.Contains(atThreshold, b => b.BucketTime == new DateTime(sinkholeBucket.Year, sinkholeBucket.Month, sinkholeBucket.Day, sinkholeBucket.Hour, 0, 0));
    }

    /// <summary>
    /// The clamp (review D4R H3, restated for the slicer's own $4): once raw's oldest chunk is dropped, the wide
    /// table still holds those rows (its own retention is independent of raw's), so an unclamped table read's
    /// collection_time floor would let them through where raw no longer can. Bounding it at
    /// <see cref="QueryStoreIntervalWide.ClampedStart"/> (against the slicer's own 1-hour-widened start) instead
    /// keeps the two equal over the span both can still answer.
    /// </summary>
    [Fact]
    public async Task Clamp_MatchesRawOnceRawsOldestChunkIsDropped_AndFailsWithoutTheClamp()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #3953 slicer clamp test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct), "TimescaleDB must be enabled on the test cluster for the clamp test's chunk drop");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await ScalarBoolAsync(connection,
            "SELECT EXISTS (SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_schema = 'collect' AND hypertable_name = 'query_store_stats')", ct),
            "query_store_stats did not convert to a hypertable; the chunk-drop below cannot run");

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());

        await QueryStoreIntervalWideGridLiveTests.SeedGridAsync(runner, ServerId, WindowStart, ct);
        await ForceFilledSinceAsync(connection, WindowStart.AddDays(-1), ct);

        var widenedStart = WindowStart - TimeSpan.FromHours(1);
        var rawFloorBefore = await ScalarDateTimeOrNullAsync(connection, ChunkFloorSql, ct);
        Assert.Equal(widenedStart, QueryStoreIntervalWide.ClampedStart(rawFloorBefore, widenedStart));

        /* Drop every chunk wholly before window day 1, exactly as the grid's own clamp test does — the anchor's
           chunk (45 days back) and day 0's own chunk. Day 1 and day 2 survive. */
        await ExecAsync(connection, "SELECT drop_chunks('collect.query_store_stats', older_than => @cutoff)", WindowStart.AddDays(1), ct);

        var rawFloorAfter = await ScalarDateTimeOrNullAsync(connection, ChunkFloorSql, ct);
        Assert.True(rawFloorAfter is DateTime f && f > widenedStart, $"expected the drop to move raw's floor past {widenedStart:o}; got {rawFloorAfter:o}");
        var clampedStart = QueryStoreIntervalWide.ClampedStart(rawFloorAfter, widenedStart);
        Assert.True(clampedStart > widenedStart, "expected the clamp to move forward once raw's floor rose above the widened start");

        /* Without the clamp -- reading the table's $4 from the widened (pre-drop) start -- the table still shows
           day 0's rows and raw does not any more: they differ. The one place this file proves a mismatch on
           purpose. */
        var (_, unclampedTableOnly, _, _) = await CompareSlicerAsync(connection, WindowStart, WindowEnd, widenedStart, null, ct);
        Assert.True(unclampedTableOnly > 0, "expected the unclamped table read to show rows raw no longer has, proving the clamp matters");

        /* With the clamp restored: equal again, over the shrunken span raw and the table can both still answer. */
        await AssertRawEqualsTableAsync(connection, WindowStart, WindowEnd, clampedStart, null, ct);
    }

    private const string ChunkFloorSql = @"
SELECT MIN(range_start) AT TIME ZONE 'UTC'
FROM timescaledb_information.chunks
WHERE hypertable_schema = 'collect'
AND   hypertable_name = 'query_store_stats';";

    /* ---- the slicer's own edge-case seed (#3953 slicer lane brief) ------------------------------------------ */

    /// <summary>
    /// Two rows the shared <see cref="QueryStoreIntervalWideGridLiveTests.SeedGridAsync"/> seed does not cover,
    /// both with <c>interval_start_time_utc</c> explicitly set (the shared seed's own rows leave it null
    /// throughout, which is a valid but different regime -- the null-fallback path, exercised implicitly by
    /// every equality check that also seeds the shared base).
    /// </summary>
    private static async Task SeedSlicerEdgeCasesAsync(DarlingCollectorRunner runner, int serverId, CancellationToken ct)
    {
        var context = new CollectorContext
        {
            ServerId = serverId,
            ServerName = "qsiw-slicer-host",
            CollectionTime = DateTime.UtcNow,
            Deltas = new CollectorDeltaCalculator(),
        };
        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = "qsiw-slicer", Host = "qsiw-slicer-host" },
            ConnectionString = "Server=qsiw-slicer-host",
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = "qsiw-slicer-host",
            ServerId = serverId,
            EngineEdition = 3,
        };

        async Task WriteAsync(DateTime collectionTime, params QueryStoreCollector.Row[] rows)
        {
            foreach (var batch in rows.GroupBy(r => r.DatabaseName))
            {
                await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, batch.ToList(), server, collectionTime, context, ct);
            }
        }

        /* An anchor 46 days before WindowStart (own identity, not shared with QueryStoreIntervalWideGridLiveTests'
           own anchor, and WITH interval_start_time_utc set -- unlike that anchor -- so a test that seeds only
           this helper never trips ViewerDataService.QueryStoreSlicerHasLegacyRowSql): pushes the wide table's
           own floor comfortably past every clause-3 margin this file's gate test needs, the same role
           SeedGridAsync's own anchor plays there. Outside every window this file uses, so it never appears in
           an equality comparison's output. */
        var anchor = WindowStart.AddDays(-46);
        await WriteAsync(anchor.AddMinutes(10), Row("qsA", 903, 9103, 9102, anchor, anchor.AddMinutes(5), 1, 50, intervalStartUtc: anchor));

        /* Starts before the window (interval_start_time_utc = WindowStart - 2h) but is COLLECTED inside it: the
           1-hour floor slack does not save it. Both raw and the table must exclude it from every bucket -- the
           content filter keys on interval_start_time_utc via COALESCE, not on collection_time, and a bug that
           used the wrong column for the content bound would leak this row into a bar it does not belong in. */
        await WriteAsync(WindowStart.AddHours(1), Row(
            "qsA", 901, 9101, 9100, WindowStart.AddHours(-2), WindowStart.AddHours(-2).AddMinutes(5), 7, 600,
            intervalStartUtc: WindowStart.AddHours(-2)));

        /* Starts inside the window (interval_start_time_utc = WindowStart + 5h) but is re-collected (the open
           interval's second fetch) past WindowEnd. Raw's own ROW_NUMBER dedupe keys on the fixed
           interval_start_time_utc, so it ALSO picks this final, post-window snapshot; the table's single stored
           row is the same running-max snapshot. Proves the $3 + 30 day ceiling does not wrongly exclude a row
           whose only surviving collection instance lands after the requested window. */
        var lateStart = WindowStart.AddHours(5);
        await WriteAsync(lateStart.AddMinutes(10), Row("qsB", 902, 9102, 9101, lateStart, lateStart.AddMinutes(9), 3, 300, intervalStartUtc: lateStart));
        await WriteAsync(WindowEnd.AddHours(2), Row("qsB", 902, 9102, 9101, lateStart, WindowEnd.AddHours(1).AddMinutes(50), 9, 320, intervalStartUtc: lateStart));
    }

    private static QueryStoreCollector.Row Row(
        string database, long queryId, long planId, long intervalId, DateTime first, DateTime last, long executions,
        long cpuUs, DateTime? intervalStartUtc, string type = "Regular") => new()
        {
            DatabaseName = database,
            QueryId = queryId,
            PlanId = planId,
            ExecutionTypeDesc = type,
            FirstExecutionTime = first,
            LastExecutionTime = last,
            QueryHash = "0x" + queryId.ToString("X8", System.Globalization.CultureInfo.InvariantCulture),
            QueryPlanHash = "0x" + planId.ToString("X8", System.Globalization.CultureInfo.InvariantCulture),
            ExecutionCount = executions,
            AvgCpuTimeUs = cpuUs,
            AvgDurationUs = cpuUs * 2,
            IsForcedPlan = false,
            ForceFailureCount = 0,
            RuntimeStatsIntervalId = intervalId,
            IntervalStartTimeUtc = intervalStartUtc,
        };

    /* ---- raw vs. table, over every returned column ------------------------------------------------------- */

    private static async Task AssertRawEqualsTableAsync(
        NpgsqlConnection connection, DateTime windowStart, DateTime windowEnd, DateTime tableStart, DateTime? literalEndUtc, CancellationToken ct)
    {
        var (rawOnly, tableOnly, rawCount, tableCount) = await CompareSlicerAsync(connection, windowStart, windowEnd, tableStart, literalEndUtc, ct);
        Assert.True(rawCount > 0, "the seed produced no raw rows; the comparison would be vacuous");
        Assert.Equal(rawCount, tableCount);
        Assert.Equal(0, rawOnly);
        Assert.Equal(0, tableOnly);
    }

    /// <summary>
    /// Materialises <see cref="ViewerDataService.QueryStoreSlicerSql"/> and <see cref="ViewerDataService.QueryStoreSlicerTableSql"/>
    /// into temp tables and diffs them both ways with <c>EXCEPT ALL</c> over every returned column.
    /// <paramref name="tableStart"/> is deliberately caller-supplied (the table SQL's own $4) rather than always
    /// <see cref="QueryStoreIntervalWide.ClampedStart"/>, so the clamp test can pass the unclamped value and
    /// observe the mismatch it causes.
    /// </summary>
    private static async Task<(long RawOnly, long TableOnly, long RawCount, long TableCount)> CompareSlicerAsync(
        NpgsqlConnection connection, DateTime windowStart, DateTime windowEnd, DateTime tableStart, DateTime? literalEndUtc, CancellationToken ct)
    {
        await ExecAsync(connection, "DROP TABLE IF EXISTS tmp_raw_slicer, tmp_table_slicer", ct);

        await using (var raw = new NpgsqlCommand("CREATE TEMP TABLE tmp_raw_slicer AS " + ViewerDataService.QueryStoreSlicerSql, connection))
        {
            ViewerDataService.AddServerWindowParameters(raw, ServerId, windowStart, windowEnd);
            raw.Parameters.Add(ViewerDataService.DatabaseFilterParameter(null));
            await raw.ExecuteNonQueryAsync(ct);
        }

        await using (var table = new NpgsqlCommand("CREATE TEMP TABLE tmp_table_slicer AS " + ViewerDataService.QueryStoreSlicerTableSql, connection))
        {
            table.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
            table.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(windowStart, DateTimeKind.Unspecified) });
            table.Parameters.Add(new NpgsqlParameter
            {
                NpgsqlDbType = NpgsqlDbType.Timestamp,
                Value = literalEndUtc.HasValue ? DateTime.SpecifyKind(literalEndUtc.Value, DateTimeKind.Unspecified) : DBNull.Value,
            });
            table.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(tableStart, DateTimeKind.Unspecified) });
            table.Parameters.Add(ViewerDataService.DatabaseFilterParameter(null));
            await table.ExecuteNonQueryAsync(ct);
        }

        var rawCount = await ScalarLongAsync(connection, "SELECT COUNT(*) FROM tmp_raw_slicer", ct);
        var tableCount = await ScalarLongAsync(connection, "SELECT COUNT(*) FROM tmp_table_slicer", ct);
        var rawOnly = await ScalarLongAsync(connection, "SELECT COUNT(*) FROM ((SELECT * FROM tmp_raw_slicer) EXCEPT ALL (SELECT * FROM tmp_table_slicer)) AS d", ct);
        var tableOnly = await ScalarLongAsync(connection, "SELECT COUNT(*) FROM ((SELECT * FROM tmp_table_slicer) EXCEPT ALL (SELECT * FROM tmp_raw_slicer)) AS d", ct);
        return (rawOnly, tableOnly, rawCount, tableCount);
    }

    /// <summary>The end-to-end oracle: <see cref="ViewerDataService.QueryStoreSlicerSql"/> itself (raw,
    /// unchanged), read directly and shaped to match <c>TimeSliceBucket</c>'s own field tuple.</summary>
    private static async Task<List<(DateTime BucketTime, long SessionCount, double TotalCpu, double TotalElapsed, double TotalReads, double TotalWrites, double TotalPhysicalReads)>> RawSlicerBucketsAsync(
        NpgsqlConnection connection, DateTime windowStart, DateTime windowEnd, CancellationToken ct)
    {
        var buckets = new List<(DateTime, long, double, double, double, double, double)>();
        await using var command = new NpgsqlCommand(ViewerDataService.QueryStoreSlicerSql, connection);
        ViewerDataService.AddServerWindowParameters(command, ServerId, windowStart, windowEnd);
        command.Parameters.Add(ViewerDataService.DatabaseFilterParameter(null));
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            buckets.Add((
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1)),
                reader.IsDBNull(2) ? 0 : Convert.ToDouble(reader.GetValue(2)),
                reader.IsDBNull(3) ? 0 : Convert.ToDouble(reader.GetValue(3)),
                reader.IsDBNull(4) ? 0 : Convert.ToDouble(reader.GetValue(4)),
                reader.IsDBNull(5) ? 0 : Convert.ToDouble(reader.GetValue(5)),
                reader.IsDBNull(6) ? 0 : Convert.ToDouble(reader.GetValue(6))));
        }

        return buckets.OrderBy(b => b.Item1).ToList();
    }

    /* ---- helpers ------------------------------------------------------------------------------------------ */

    private static async Task<NpgsqlConnection> OpenMigratedAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return connection;
    }

    private static async Task ForceFilledSinceAsync(NpgsqlConnection connection, DateTime value, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "UPDATE collect.query_store_interval_wide_coverage SET filled_since = @value WHERE server_id = @server_id", connection);
        command.Parameters.AddWithValue("server_id", ServerId);
        command.Parameters.AddWithValue("value", DateTime.SpecifyKind(value, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        if (sql.Contains("@server_id", StringComparison.Ordinal))
        {
            command.Parameters.AddWithValue("server_id", ServerId);
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, DateTime cutoff, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        if (sql.Contains("@server_id", StringComparison.Ordinal))
        {
            command.Parameters.AddWithValue("server_id", ServerId);
        }

        command.Parameters.AddWithValue("cutoff", DateTime.SpecifyKind(cutoff, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> ScalarLongAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task<bool> ScalarBoolAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<DateTime> ScalarDateTimeAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("server_id", ServerId);
        return (DateTime)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<DateTime?> ScalarDateTimeOrNullAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var result = await command.ExecuteScalarAsync(ct);
        return result is DateTime dt ? dt : null;
    }
}
