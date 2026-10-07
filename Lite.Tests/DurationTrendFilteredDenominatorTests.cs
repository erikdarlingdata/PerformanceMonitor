/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5414 M1 (#5244 PR4): a filtered duration-trend bucket is read over EVERY collection in it. The query-stats collector
/// keeps only plans that ran in the last ten minutes, so a database that ran nothing recently has no row in most
/// collections; dividing by only the collections it appeared in read its rate high (one 6 s query in a 60-minute bucket
/// of four 15-minute collections read 6000 ms / 900 s, four times the true 6000 ms / 3600 s) and left [A] + [B] != [A, B].
/// A is busy in the first collection only, B in the third only, C (a small steady load) in all four, so [A] and [B]
/// each miss three of the four collections. Covers the three DuckDB statements that bucket the duration shape: the MCP
/// read (query and procedure) and the two chart reads (duration, execution count).
/// </summary>
public sealed class DurationTrendFilteredDenominatorTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "TrendDenominatorSrv";
    private const double BucketSeconds = 4 * 900;

    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _service;
    private readonly int _serverId;
    private readonly DateTime _hour;
    private DuckDBConnection? _seedConn;
    private long _nextId = 950000;

    public DurationTrendFilteredDenominatorTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _service = new LocalDataService(_duckDb);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(ServerName);

        var now = DateTime.UtcNow;
        _hour = new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Utc).AddHours(-3);
    }

    public void Dispose() => _seedConn?.Dispose();

    private async Task SeedAsync(bool procedures)
    {
        for (var i = 0; i < 4; i++)
        {
            var at = _hour.AddMinutes(15 * i);
            if (i == 0) { await InsertAsync(procedures, at, "DbA", 6_000_000); }
            if (i == 2) { await InsertAsync(procedures, at, "DbB", 3_000_000); }
            await InsertAsync(procedures, at, "DbC", 1_000);
        }
    }

    private async Task InsertAsync(bool procedure, DateTime collectionTimeUtc, string database, long elapsedUs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        _seedConn ??= _duckDb.CreateConnection();
        if (_seedConn.State != System.Data.ConnectionState.Open)
        {
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = procedure
            ? @"INSERT INTO procedure_stats
                    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name,
                     delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, sample_interval_seconds)
                VALUES ($1, $2, $3, $4, $5, 'dbo', 'usp_Trend', 10, $6, $6, 0, 900)"
            : @"INSERT INTO query_stats
                    (collection_id, collection_time, server_id, server_name, database_name,
                     query_hash, sql_handle, last_execution_time, delta_execution_count,
                     delta_worker_time, delta_elapsed_time, query_text, sample_interval_seconds)
                VALUES ($1, $2, $3, $4, $5, $7, '0xSQLH', $2, 10, $6, $6, 'SELECT 1', 900)";
        var naive = DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified);
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = naive });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = database });
        cmd.Parameters.Add(new DuckDBParameter { Value = elapsedUs });
        if (!procedure)
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = "0xTRENDDEN" + database });
        }

        await cmd.ExecuteNonQueryAsync();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task McpBucket_AFilteredBucket_IsReadOverEveryCollectionInIt_SoTheDatabasesAddUp(bool procedures)
    {
        await SeedAsync(procedures);
        var asOf = DateTime.UtcNow;

        async Task<(double Elapsed, double Executions)> Read(params string[]? names)
        {
            var points = procedures
                ? await _service.GetBucketedProcedureDurationTrendAsync(_serverId, 24, asOf, 60, names)
                : await _service.GetBucketedQueryDurationTrendAsync(_serverId, 24, asOf, 60, names);
            var point = points.Single();
            return (point.Value!.Value, point.ExecutionsPerSecond!.Value);
        }

        var a = await Read("DbA");
        var b = await Read("DbB");
        var both = await Read("DbA", "DbB");
        Assert.Equal(6_000 / BucketSeconds, a.Elapsed, 9);
        Assert.Equal(3_000 / BucketSeconds, b.Elapsed, 9);
        Assert.Equal(a.Elapsed + b.Elapsed, both.Elapsed, 9);
        /* Ten executions per row: A and B each ran in one collection of four. */
        Assert.Equal(10 / BucketSeconds, a.Executions, 9);
        Assert.Equal(a.Executions + b.Executions, both.Executions, 9);
        Assert.Equal((9_000 + 4) / BucketSeconds, (await Read()).Elapsed, 9);
        Assert.Equal(4 / BucketSeconds, (await Read("DbC")).Elapsed, 9);
    }

    /// <summary>
    /// [#5414 round 2, per-bucket HAVING] A bucket the chosen databases had no rows in is a measured 0 and a bucket the
    /// store covered, not a missing one. Collections every 15 minutes for four hours, DbC in every one, DbA in the third
    /// hour's first collection only. [DbA] returns all four hour buckets (zero before and after its one busy hour), and
    /// the first one's first_collection_time is the store's own first collection, so the window is not read as cut short
    /// (Darling's DescribeCoverage and Lite's window notice both read that instant). The chart reads follow the rule.
    /// </summary>
    [Fact]
    public async Task AFilteredWindow_KeepsEveryBucketTheStoreCovered_AsZero()
    {
        var first = _hour.AddHours(-3);
        for (var i = 0; i < 16; i++)
        {
            var at = first.AddMinutes(15 * i);
            if (i == 8) { await InsertAsync(procedure: false, at, "DbA", 6_000_000); }
            await InsertAsync(procedure: false, at, "DbC", 1_000);
        }

        var asOf = DateTime.UtcNow;
        var points = await _service.GetBucketedQueryDurationTrendAsync(_serverId, 24, asOf, 60, new[] { "DbA" });
        Assert.Equal(4, points.Count);
        var expected = new[] { 0.0, 0.0, 6_000 / BucketSeconds, 0.0 };
        for (var i = 0; i < expected.Length; i++) { Assert.Equal(expected[i], points[i].Value!.Value, 9); }
        Assert.Equal(DateTime.SpecifyKind(first, DateTimeKind.Unspecified), DateTime.SpecifyKind(points[0].FirstCollectionTime!.Value, DateTimeKind.Unspecified));

        var chart = await _service.GetQueryDurationTrendAsync(_serverId, 12, null, null, new[] { "DbA" }, asOf);
        Assert.Contains(chart, p => p.Value == 0.0 && p.CollectionTime < first.AddHours(2));
        var executions = await _service.GetExecutionCountTrendAsync(_serverId, 12, null, null, new[] { "DbA" });
        Assert.Contains(executions, p => p.Value == 0.0 && p.CollectionTime < first.AddHours(2));

        Assert.Empty(await _service.GetBucketedQueryDurationTrendAsync(_serverId, 24, asOf, 60, new[] { "NoSuchDb" }));
        Assert.Empty(await _service.GetQueryDurationTrendAsync(_serverId, 12, null, null, new[] { "NoSuchDb" }, asOf));
        Assert.Empty(await _service.GetExecutionCountTrendAsync(_serverId, 12, null, null, new[] { "NoSuchDb" }));
    }

    [Fact]
    public async Task McpBucket_AListThatMatchesNothing_IsEmpty_NotAZeroSeries()
    {
        await SeedAsync(procedures: false);

        var points = await _service.GetBucketedQueryDurationTrendAsync(_serverId, 24, DateTime.UtcNow, 60, new[] { "NoSuchDb" });

        Assert.Empty(points);
    }

    [Fact]
    public async Task Chart_AFilteredBucket_IsReadOverEveryCollectionInIt_SoTheDatabasesAddUp()
    {
        await SeedAsync(procedures: false);

        /* The chart's bucket width is the window's own; ninety days is wide enough for a whole number of hours, so every collection
           planted lies in one bucket. */
        const int hoursBack = 90 * 24;
        Assert.Equal(0, TrendBuckets.AutoMinutes(hoursBack * 60, 1, TrendBudget.Chart.AutoPoints) % 60);
        var asOf = DateTime.UtcNow;

        async Task<double> Duration(params string[]? names) =>
            (await _service.GetQueryDurationTrendAsync(_serverId, hoursBack, null, null, names, asOf)).Single().Value!.Value;

        var a = await Duration("DbA");
        var b = await Duration("DbB");
        Assert.Equal(6_000 / BucketSeconds, a, 9);
        Assert.Equal(3_000 / BucketSeconds, b, 9);
        Assert.Equal(a + b, await Duration("DbA", "DbB"), 9);
        Assert.Equal((9_000 + 4) / BucketSeconds, await Duration(), 9);

        async Task<double> Executions(params string[]? names) =>
            (await _service.GetExecutionCountTrendAsync(_serverId, hoursBack, null, null, names)).Single().Value!.Value;

        var executionsA = await Executions("DbA");
        Assert.Equal(10 / BucketSeconds, executionsA, 9);
        Assert.Equal(executionsA + await Executions("DbB"), await Executions("DbA", "DbB"), 9);
        Assert.Equal(40 / BucketSeconds + 20 / BucketSeconds, await Executions(), 9);
    }
}
