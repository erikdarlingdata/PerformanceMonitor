/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4310 site 4's live pass for the slicer overlay's item timeline:
/// <see cref="QueryStoreIntervalWide.ReadsTableAsync"/>'s gate and
/// <see cref="ViewerDataService.QueryStoreItemTimelineTableSql"/>, seeded through the real write path (reusing
/// <see cref="QueryStoreIntervalWideGridLiveTests.SeedGridAsync"/>, #4341's own seed) on a real PostgreSQL store.
/// Mirrors <c>QueryStoreIntervalWideGridLiveTests</c>' own equivalence/gate coverage for the grid read.
/// </summary>
/* #1776 own-store: not [Collection("live-postgres")], for the same reason as the grid's own file — every test
   here creates and drops its own scratch database and never touches live collection. */
public sealed class QueryStoreItemTimelineWideLiveTests
{
    private const int ServerId = -4310930;

    private static readonly DateTime WindowStart = new(2026, 9, 15, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WindowEnd = WindowStart.AddDays(3);

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheTableRead_EqualsRaw_OnTheDay2ReFetchedInterval_AndTheLiveGateRoutesCorrectly()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4310 item-timeline live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());

        // Reuse #4341's grid seed: day2's qsB/query 4/plan 41 interval is re-fetched across three collections
        // while open (execution_count 5 -> 12 -> 20), the same shape #1841's dedup exists to collapse.
        await QueryStoreIntervalWideGridLiveTests.SeedGridAsync(runner, ServerId, WindowStart, ct);
        await ForceFilledSinceAsync(connection, ServerId, WindowStart.AddDays(-1), ct);

        // ---- below the grid's 12h minimum window: must route to raw (clause 5) ----
        await using (var viewer = new ViewerDataService(scratch.ConnectionString))
        {
            var shortEnd = WindowStart.AddHours(6);
            var shortTimeline = await viewer.GetQueryStoreItemTimelineAsync(ServerId, "qsB", queryId: 4, planId: 41, WindowStart, shortEnd);
            // The seed's day0 interval sits within [WindowStart, WindowStart+6h]? No: day0 is at
            // WindowStart+1h..+2h, unrelated identity (query 1/2). query 4/plan 41 lives on day2, outside this
            // 6h short window, so raw legitimately returns nothing here — the point of this arm is only that
            // routing happens (proven directly below via the gate itself), not this list's contents.
            Assert.NotNull(shortTimeline);
        }

        var (useTableShort, _) = await QueryStoreIntervalWide.ReadsTableAsync(
            connection, ServerId, WindowStart, WindowStart.AddHours(6), null, QueryStoreIntervalWide.GridWideMinWindow, 30, null, ct);
        Assert.False(useTableShort, "a 6h window is under the item-timeline gate's 12h minimum and must read raw");

        // ---- at/above the minimum window, on the seeded/forced-covered span: the gate picks the table ----
        var (useTableOpen, clampOpen) = await QueryStoreIntervalWide.ReadsTableAsync(
            connection, ServerId, WindowStart, WindowEnd, null, QueryStoreIntervalWide.GridWideMinWindow, 30, null, ct);
        Assert.True(useTableOpen, "expected the gate to pick the table for the seeded, forced-covered window");

        // ---- raw vs. table equality on query 4 / plan 41 (the re-fetched, open interval) ----
        var rawPoints = await ReadRawAsync(connection, WindowStart, WindowEnd, ct);
        var tablePoints = await ReadTableAsync(connection, clampOpen, null, ct);
        Assert.True(rawPoints.Count > 0, "the seed produced no raw rows for query 4/plan 41; the comparison would be vacuous");
        Assert.Equal(rawPoints.Count, tablePoints.Count);
        for (var i = 0; i < rawPoints.Count; i++)
        {
            Assert.Equal(rawPoints[i].PointTime, tablePoints[i].PointTime);
            Assert.Equal(rawPoints[i].CpuMs, tablePoints[i].CpuMs, 3);
            Assert.Equal(rawPoints[i].ElapsedMs, tablePoints[i].ElapsedMs, 3);
            Assert.Equal(rawPoints[i].Reads, tablePoints[i].Reads, 3);
            Assert.Equal(rawPoints[i].Writes, tablePoints[i].Writes, 3);
            Assert.Equal(rawPoints[i].PhysicalReads, tablePoints[i].PhysicalReads, 3);
        }

        // End to end through the viewer: lands on the table (schema 145 present) and returns the raw statement's
        // own final running-max execution count for the re-fetched interval (20, after three re-fetches — same
        // fact #4341's grid pins for this identity).
        await using var viewer2 = new ViewerDataService(scratch.ConnectionString);
        var endToEnd = await viewer2.GetQueryStoreItemTimelineAsync(ServerId, "qsB", queryId: 4, planId: 41, WindowStart, WindowEnd);
        var point = Assert.Single(endToEnd);
        Assert.Equal(5.0, point.CpuMs, 3); // avg_cpu_time_us(250) x execution_count(20) / 1000 -> ms, the seed's final re-fetch

        // ---- coverage-miss falls back to raw: force filled_since AFTER the window start ----
        await ForceFilledSinceAsync(connection, ServerId, WindowEnd.AddDays(1), ct);
        var (useTableMiss, _) = await QueryStoreIntervalWide.ReadsTableAsync(
            connection, ServerId, WindowStart, WindowEnd, null, QueryStoreIntervalWide.GridWideMinWindow, 30, null, ct);
        Assert.False(useTableMiss, "coverage starting after the window must fall back to raw");

        await using var viewer3 = new ViewerDataService(scratch.ConnectionString);
        var missTimeline = await viewer3.GetQueryStoreItemTimelineAsync(ServerId, "qsB", queryId: 4, planId: 41, WindowStart, WindowEnd);
        var missPoint = Assert.Single(missTimeline);
        Assert.Equal(point.PointTime, missPoint.PointTime);
        Assert.Equal(point.ElapsedMs, missPoint.ElapsedMs, 3); // raw fallback reports the identical answer

        // restore coverage
        await ForceFilledSinceAsync(connection, ServerId, WindowStart.AddDays(-1), ct);

        // ---- a pending batch must read raw regardless of coverage (same clause the grid pins) ----
        await using (var pending = new NpgsqlCommand(
            "INSERT INTO collect.query_store_interval_wide_pending (server_id, collection_time, database_name, recorded_at) VALUES (@server_id, @cutoff, 'qsB', @cutoff)",
            connection))
        {
            pending.Parameters.AddWithValue("server_id", ServerId);
            pending.Parameters.AddWithValue("cutoff", DateTime.SpecifyKind(WindowStart, DateTimeKind.Unspecified));
            await pending.ExecuteNonQueryAsync(ct);
        }

        var (useTablePending, _) = await QueryStoreIntervalWide.ReadsTableAsync(
            connection, ServerId, WindowStart, WindowEnd, null, QueryStoreIntervalWide.GridWideMinWindow, 30, null, ct);
        Assert.False(useTablePending, "a pending batch must read raw regardless of coverage");

        await using (var clearPending = new NpgsqlCommand(
            "DELETE FROM collect.query_store_interval_wide_pending WHERE server_id = @server_id", connection))
        {
            clearPending.Parameters.AddWithValue("server_id", ServerId);
            await clearPending.ExecuteNonQueryAsync(ct);
        }

        Assert.True((await QueryStoreIntervalWide.ReadsTableAsync(
            connection, ServerId, WindowStart, WindowEnd, null, QueryStoreIntervalWide.GridWideMinWindow, 30, null, ct)).UseTable,
            "the passing case, restored, must read the table again");
    }

    /// <summary>Raw oracle: <see cref="ViewerDataService.QueryStoreItemTimelineSql"/> itself, for query 4 / plan 41
    /// (qsB) over [<paramref name="windowStart"/>, <paramref name="windowEnd"/>].</summary>
    private static async Task<System.Collections.Generic.List<(DateTime PointTime, double CpuMs, double ElapsedMs, double Reads, double Writes, double PhysicalReads)>> ReadRawAsync(
        NpgsqlConnection connection, DateTime windowStart, DateTime windowEnd, System.Threading.CancellationToken ct)
    {
        var points = new System.Collections.Generic.List<(DateTime, double, double, double, double, double)>();
        await using var command = new NpgsqlCommand(ViewerDataService.QueryStoreItemTimelineSql, connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = "qsB" });
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = 4L });
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = 41L });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(windowStart, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(windowEnd, DateTimeKind.Unspecified) });
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            points.Add((
                reader.GetDateTime(0),
                Convert.ToDouble(reader.GetValue(1)),
                Convert.ToDouble(reader.GetValue(2)),
                Convert.ToDouble(reader.GetValue(3)),
                Convert.ToDouble(reader.GetValue(4)),
                Convert.ToDouble(reader.GetValue(5))));
        }

        return points;
    }

    /// <summary>Table read: <see cref="ViewerDataService.QueryStoreItemTimelineTableSql"/>, for the same identity.</summary>
    private static async Task<System.Collections.Generic.List<(DateTime PointTime, double CpuMs, double ElapsedMs, double Reads, double Writes, double PhysicalReads)>> ReadTableAsync(
        NpgsqlConnection connection, DateTime tableStart, DateTime? literalEndUtc, System.Threading.CancellationToken ct)
    {
        var points = new System.Collections.Generic.List<(DateTime, double, double, double, double, double)>();
        await using var command = new NpgsqlCommand(ViewerDataService.QueryStoreItemTimelineTableSql, connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = "qsB" });
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = 4L });
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = 41L });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(tableStart, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp,
            Value = literalEndUtc.HasValue ? DateTime.SpecifyKind(literalEndUtc.Value, DateTimeKind.Unspecified) : DBNull.Value,
        });
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            points.Add((
                reader.GetDateTime(0),
                Convert.ToDouble(reader.GetValue(1)),
                Convert.ToDouble(reader.GetValue(2)),
                Convert.ToDouble(reader.GetValue(3)),
                Convert.ToDouble(reader.GetValue(4)),
                Convert.ToDouble(reader.GetValue(5))));
        }

        return points;
    }

    private static async Task ForceFilledSinceAsync(NpgsqlConnection connection, int serverId, DateTime value, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "UPDATE collect.query_store_interval_wide_coverage SET filled_since = @value WHERE server_id = @server_id", connection);
        command.Parameters.AddWithValue("server_id", serverId);
        command.Parameters.AddWithValue("value", DateTime.SpecifyKind(value, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }
}
