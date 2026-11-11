/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8548: the 7-day provisioning trend lists seven calendar days, the six whole days before today and today so far. The read is
/// run over a real store that holds a CPU sample on each of the last nine days of the display zone, so the rows it returns are
/// the ones the rule keeps: none from the day seven days back (a part day when the window began at this hour a week ago), and
/// only today's row marked as a day still running.
/// </summary>
public sealed class FinOpsProvisioningTrendSevenDaysTests : IDisposable
{
    private const int ServerId = 8548;
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "FinOpsTrendSeven_" + Guid.NewGuid().ToString("N")[..8]);
    private long _nextId = 1;

    public FinOpsProvisioningTrendSevenDaysTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task TheTrend_ListsTheSixWholeDaysBeforeToday_AndTodaySoFar()
    {
        using var duckDb = new DuckDbInitializer(Path.Combine(_dir, "trend.duckdb"));
        await duckDb.InitializeFromTemplateAsync();

        /* A zone in which it is noon now, so an hour either side of each sample stays on its own day whatever the hour of the run. */
        var nowUtc = DateTime.UtcNow;
        var zone = NoonZone(nowUtc);
        var today = TimeZoneInfo.ConvertTimeFromUtc(DateTime.SpecifyKind(nowUtc, DateTimeKind.Utc), zone).Date;

        /* One sample an hour ago on each of the last nine days, and one on the day a week back at an hour from now: inside a
           window that begins at this hour seven days ago, and on a day that is not one of the seven. */
        for (var daysBack = 0; daysBack <= 8; daysBack++)
        {
            await SeedCpuAsync(duckDb, nowUtc.AddDays(-daysBack).AddHours(-1));
        }
        await SeedCpuAsync(duckDb, nowUtc.AddDays(-7).AddHours(1));

        var rows = await new LocalDataService(duckDb).GetProvisioningTrendAsync(ServerId, zone);

        Assert.Equal(
            Enumerable.Range(0, 7).Select(i => today.AddDays(i - 6)).ToArray(),
            rows.Select(r => r.Day).ToArray());

        /* Today is still running, and its Day cell says so; the six days before it are whole and say nothing more than the date. */
        Assert.EndsWith(", so far", rows[^1].DayDisplay, StringComparison.Ordinal);
        Assert.All(rows.Take(6), r => Assert.DoesNotContain("so far", r.DayDisplay, StringComparison.Ordinal));
    }

    /// <summary>A fixed-offset zone whose wall clock reads about noon at <paramref name="nowUtc"/>.</summary>
    private static TimeZoneInfo NoonZone(DateTime nowUtc)
    {
        var offset = TimeSpan.FromHours(12 - nowUtc.Hour);
        return TimeZoneInfo.CreateCustomTimeZone("trend-noon", offset, "trend-noon", "trend-noon");
    }

    private async Task SeedCpuAsync(DuckDbInitializer duckDb, DateTime at)
    {
        using var connection = duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO cpu_utilization_stats
    (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES ($1, $2, $3, 'trend-seven-days', $2, 40, 5)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(at, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        await cmd.ExecuteNonQueryAsync();
    }
}
