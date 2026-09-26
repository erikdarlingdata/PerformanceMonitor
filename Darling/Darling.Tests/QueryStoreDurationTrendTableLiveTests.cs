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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4310 site 3's live pass for the Performance-Trends chart's Query Store duration series
/// (<see cref="ViewerDataService.GetQueryStoreDurationTrendAsync"/>'s raw arm, and its new table twin
/// <see cref="ViewerDataService.QueryStoreDurationTrendTableSql"/>): routing above/below
/// <see cref="QueryStoreIntervalWide.GridWideMinWindow"/>, raw vs. table point equality on a covered window,
/// and the coverage-miss/fault fallback to raw. Reuses <see cref="QueryStoreIntervalWideGridLiveTests.SeedGridAsync"/>
/// so this file's seed cannot drift from the grid's own #3953 seed.
/// </summary>
/* #1776 own-store: not [Collection("live-postgres")] — every test here works entirely inside its own
   ScratchPostgres-created database, so it cannot race live collection. */
public sealed class QueryStoreDurationTrendTableLiveTests
{
    private const int ServerId = -4310301;

    private static readonly DateTime WindowStart = new(2026, 9, 15, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WindowEnd = WindowStart.AddDays(3);

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TableRead_EqualsRaw_OnACoveredWindow_AndRoutesBelowTheMinimumWindow()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4310 duration-trend table live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());

        await QueryStoreIntervalWideGridLiveTests.SeedGridAsync(runner, ServerId, WindowStart, ct);
        await ForceFilledSinceAsync(connection, WindowStart.AddDays(-1), ct);

        /* At or above the 12h minimum window, on a store the seed forced long-covered: the table read must
           equal raw's own duration-trend points exactly (collection_time, duration_ms_per_second,
           executions_per_second — every row, in order). */
        var rawPoints = await ReadPointsAsync(postgres, ViewerDataService.QueryStoreDurationTrendSql, WindowStart, WindowEnd, ct);
        var tablePoints = await ReadPointsAsync(postgres, ViewerDataService.QueryStoreDurationTrendTableSql, WindowStart, WindowEnd, ct, isTableSql: true);
        Assert.True(rawPoints.Count > 0, "the seed produced no raw points; the comparison would be vacuous");
        Assert.Equal(rawPoints, tablePoints);

        /* End to end: GetQueryStoreDurationTrendAsync at or above the minimum window lands on the table (V145
           is present, the store is long-covered) and returns exactly raw's own points. */
        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        var longWindowSeries = await viewer.GetQueryStoreDurationTrendAsync(ServerId, WindowStart, WindowEnd);
        Assert.Equal(rawPoints.Select(p => (p.CollectionTime, p.Value, p.ExecutionCount)).ToList(),
            longWindowSeries.Points.Select(p => (p.CollectionTime, p.Value, p.ExecutionCount)).ToList());

        /* Below the minimum window: GetQueryStoreDurationTrendAsync must return exactly what the raw-only read
           returns for the same short span — the gate never gets a chance to route it to the table. */
        var shortWindowEnd = WindowStart.AddHours(6);
        var shortRawPoints = await ReadPointsAsync(postgres, ViewerDataService.QueryStoreDurationTrendSql, WindowStart, shortWindowEnd, ct);
        var shortWindowSeries = await viewer.GetQueryStoreDurationTrendAsync(ServerId, WindowStart, shortWindowEnd);
        Assert.Equal(shortRawPoints.Select(p => (p.CollectionTime, p.Value, p.ExecutionCount)).ToList(),
            shortWindowSeries.Points.Select(p => (p.CollectionTime, p.Value, p.ExecutionCount)).ToList());
    }

    /// <summary>
    /// Coverage-miss falls back to raw: a window at or above the minimum that the gate refuses (here, by never
    /// forcing <c>filled_since</c> back — the real apply's own coverage bookkeeping lands it near "now", far
    /// past this historical seed's window) must still return raw's own points, unchanged, rather than an empty
    /// or wrong series.
    /// </summary>
    [Fact]
    public async Task UnforcedCoverage_FallsBackToRaw_AndStillMatchesRawExactly()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4310 coverage-miss fallback live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());

        await QueryStoreIntervalWideGridLiveTests.SeedGridAsync(runner, ServerId, WindowStart, ct);
        /* Deliberately NOT forcing filled_since back: the real apply's own coverage claim sits near "now",
           which does not cover this historical window, so clause 2 must refuse the table. */

        var rawPoints = await ReadPointsAsync(postgres, ViewerDataService.QueryStoreDurationTrendSql, WindowStart, WindowEnd, ct);
        Assert.True(rawPoints.Count > 0, "the seed produced no raw points; the comparison would be vacuous");

        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        var series = await viewer.GetQueryStoreDurationTrendAsync(ServerId, WindowStart, WindowEnd);
        Assert.Equal(rawPoints.Select(p => (p.CollectionTime, p.Value, p.ExecutionCount)).ToList(),
            series.Points.Select(p => (p.CollectionTime, p.Value, p.ExecutionCount)).ToList());
    }

    private static async Task<List<(DateTime CollectionTime, double Value, long ExecutionCount)>> ReadPointsAsync(
        NpgsqlDataSource postgres, string sql, DateTime windowStart, DateTime windowEnd, CancellationToken ct, bool isTableSql = false)
    {
        var items = new List<(DateTime, double, long)>();
        await using var command = postgres.CreateCommand(sql);
        if (isTableSql)
        {
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(windowStart, DateTimeKind.Unspecified) });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(windowEnd, DateTimeKind.Unspecified) });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(windowEnd, DateTimeKind.Unspecified) });
            command.Parameters.Add(ViewerDataService.DatabaseFilterParameter(null));
        }
        else
        {
            ViewerDataService.AddServerWindowParameters(command, ServerId, windowStart, windowEnd);
            command.Parameters.Add(ViewerDataService.DatabaseFilterParameter(null));
        }

        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            if (reader.IsDBNull(1))
            {
                continue;
            }

            items.Add((
                reader.GetDateTime(0),
                Convert.ToDouble(reader.GetValue(1)),
                reader.IsDBNull(2) ? 0 : (long)Convert.ToDouble(reader.GetValue(2))));
        }

        return items;
    }

    private static async Task ForceFilledSinceAsync(NpgsqlConnection connection, DateTime value, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "UPDATE collect.query_store_interval_wide_coverage SET filled_since = @value WHERE server_id = @server_id", connection);
        command.Parameters.AddWithValue("server_id", ServerId);
        command.Parameters.AddWithValue("value", DateTime.SpecifyKind(value, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }
}
