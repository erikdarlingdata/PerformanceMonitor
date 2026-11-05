/*
 * Performance Monitor Lite
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7682: SQL Server on Linux before 2025 CU1 cannot report host CPU, so the collector stores NULL for
/// other-process CPU (#1048). The reads turned that NULL into 0, so the CPU chart and the get_cpu_utilization reply
/// said the host was 100% idle when nothing had measured it. A bucket whose samples all lack the figure returns null
/// for other, total and idle; a bucket that mixes both kinds averages only the samples that have it.
/// </summary>
public sealed class LinuxNullOtherProcessCpuTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -7682;
    private const string ServerName = "linux-null-other-cpu";

    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public LinuxNullOtherProcessCpuTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);
    }

    public void Dispose() => _seedConn?.Dispose();

    private static DateTime Hour() => new(DateTime.UtcNow.Year, DateTime.UtcNow.Month, DateTime.UtcNow.Day, DateTime.UtcNow.Hour, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public async Task ChartRead_AllNullOtherProcessCpu_ReturnsNullOtherAndTotal()
    {
        var t1 = Hour().AddHours(-2);
        await SeedCpuAsync(t1, 40, null);
        await SeedCpuAsync(t1.AddMinutes(5), 50, null);
        await SeedCpuAsync(t1.AddMinutes(10), 60, null);

        var rows = await _dataService.GetCpuUtilizationAsync(ServerId, hoursBack: 4, serverClock: ServerClock.Utc);

        Assert.Equal(new[] { 40, 50, 60 }, rows.Select(r => r.SqlServerCpu).ToArray());
        Assert.All(rows, r =>
        {
            Assert.Null(r.OtherProcessCpu);
            Assert.Null(r.TotalCpu);
            Assert.Null(r.IdleCpu);
        });
    }

    [Fact]
    public async Task ChartRead_MixedBucket_AveragesOnlyTheMeasuredSamples()
    {
        var t1 = Hour().AddHours(-2);
        /* One 60 minute window is wider than the sample spacing, so all three land in one bucket when the
           window is wide enough; hoursBack 24 * 30 forces a wide bucket. */
        await SeedCpuAsync(t1, 20, null);
        await SeedCpuAsync(t1.AddMinutes(1), 40, 10);
        await SeedCpuAsync(t1.AddMinutes(2), 60, 20);

        var rows = await _dataService.GetCpuUtilizationAsync(ServerId, hoursBack: 24 * 30, serverClock: ServerClock.Utc);

        var row = Assert.Single(rows);
        Assert.Equal(40, row.SqlServerCpu);
        Assert.Equal(15, row.OtherProcessCpu);
        Assert.Equal(65, row.TotalCpu);
        Assert.Equal(35, row.IdleCpu);
    }

    /* The app reads the UTC frame only (ServerTab.Refresh), whose statement carries the total as a seventh column. */
    [Fact]
    public async Task ChartRead_UtcFrame_AllNullOtherProcessCpu_ReturnsNullOtherAndTotal()
    {
        var t1 = Hour().AddHours(-2);
        await SeedCpuAsync(t1, 40, null, withUtcInstant: true);
        await SeedCpuAsync(t1.AddMinutes(5), 50, null, withUtcInstant: true);

        var rows = await _dataService.GetCpuUtilizationAsync(
            ServerId, hoursBack: 4, serverClock: ServerClock.Utc, frame: CpuTimeFrame.Utc);

        Assert.Equal(new[] { 40, 50 }, rows.Select(r => r.SqlServerCpu).ToArray());
        Assert.All(rows, r =>
        {
            Assert.Null(r.OtherProcessCpu);
            Assert.Null(r.TotalCpu);
            Assert.Null(r.IdleCpu);
        });
    }

    [Fact]
    public async Task ChartRead_UtcFrame_MixedBucket_AveragesOnlyTheMeasuredSamples()
    {
        var t1 = Hour().AddHours(-2);
        await SeedCpuAsync(t1, 20, null, withUtcInstant: true);
        await SeedCpuAsync(t1.AddMinutes(1), 40, 10, withUtcInstant: true);
        await SeedCpuAsync(t1.AddMinutes(2), 60, 20, withUtcInstant: true);

        var rows = await _dataService.GetCpuUtilizationAsync(
            ServerId, hoursBack: 24 * 30, serverClock: ServerClock.Utc, frame: CpuTimeFrame.Utc);

        var row = Assert.Single(rows);
        Assert.Equal(40, row.SqlServerCpu);
        Assert.Equal(15, row.OtherProcessCpu);
        Assert.Equal(65, row.TotalCpu);
        Assert.Equal(35, row.IdleCpu);
    }

    [Fact]
    public async Task McpBucketRead_AllNullBucket_ReturnsNullHostFigures_AndMixedBucketAveragesMeasuredRows()
    {
        var hour = Hour().AddHours(-3);
        /* Bucket one: every sample lacks the figure. Bucket two: a mix. */
        await SeedCpuAsync(hour, 30, null);
        await SeedCpuAsync(hour.AddMinutes(10), 50, null);
        await SeedCpuAsync(hour.AddHours(1), 20, null);
        await SeedCpuAsync(hour.AddHours(1).AddMinutes(10), 40, 10);
        await SeedCpuAsync(hour.AddHours(1).AddMinutes(20), 60, 20);

        var points = await _dataService.GetCpuBucketsAsync(ServerId, 6, DateTime.UtcNow, ServerClock.Utc, bucketMinutes: 60);

        Assert.Equal(2, points.Count);
        var linux = points[0];
        Assert.Equal(40, linux.SqlServerCpu);
        Assert.Null(linux.OtherProcessCpu);
        Assert.Null(linux.TotalCpu);
        Assert.Null(linux.IdleCpu);
        Assert.Null(linux.PeakTotalCpu);
        Assert.Equal(50, linux.PeakSqlServerCpu);

        var mixed = points[1];
        Assert.Equal(40, mixed.SqlServerCpu);
        Assert.Equal(15, mixed.OtherProcessCpu);
        Assert.Equal(65, mixed.TotalCpu);
        Assert.Equal(35, mixed.IdleCpu);
        Assert.Equal(80, mixed.PeakTotalCpu);
        Assert.Equal(3, mixed.Samples);
    }

    [Fact]
    public void Headline_WithoutOtherProcessCpu_IsLabelledSqlCpuAndHasNoTotal()
    {
        var card = new ServerSummaryItem { CpuPercent = 12 };

        Assert.Null(card.TotalCpuPercent);
        Assert.Equal("SQL CPU 12%", card.CpuDisplay);

        card.OtherProcessCpuPercent = 8;
        Assert.Equal(20, card.TotalCpuPercent);
        Assert.Equal("20% (SQL 12%)", card.CpuDisplay);
    }

    [Fact]
    public void StatusReason_WithoutOtherProcessCpu_SaysSqlCpuOnce()
    {
        var card = new ServerSummaryItem { CpuPercent = 85 };

        Assert.Contains("SQL CPU 85%", card.StatusReason, StringComparison.Ordinal);
        Assert.DoesNotContain("CPU SQL CPU", card.StatusReason, StringComparison.Ordinal);
    }

    private async Task SeedCpuAsync(DateTime sampleTime, int sqlCpu, int? otherCpu, bool withUtcInstant = false)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"INSERT INTO cpu_utilization_stats
            (collection_id, collection_time, server_id, server_name, sample_time,
             sqlserver_cpu_utilization, other_process_cpu_utilization, sample_time_utc)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)";
        foreach (var v in new object?[] { _nextId--, DateTime.UtcNow, ServerId, ServerName, sampleTime, sqlCpu, otherCpu, withUtcInstant ? sampleTime : null })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
        }

        await cmd.ExecuteNonQueryAsync();
    }
}
