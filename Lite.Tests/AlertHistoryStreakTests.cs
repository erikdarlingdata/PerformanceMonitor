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
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests.Helpers;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6261: <c>get_alert_history</c> with <c>include_streak</c> adds <c>streak_days</c> to each row (Darling's twin, same planted
/// shape); without it the rows carry no streak member. The streak is counted from fires outside the page too, a muted fire
/// counts, and a recovery row has none.
/// </summary>
public sealed class AlertHistoryStreakTests : IDisposable
{
    private const string Metric = "Blocking Detected";

    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly TestAlertDataHelper _helper;
    private readonly DuckDbInitializer _initializer;

    public AlertHistoryStreakTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _helper = new TestAlertDataHelper(_dbPath);
        _initializer = new DuckDbInitializer(_dbPath);
    }

    public void Dispose()
    {
        _initializer.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* Best-effort cleanup */ }
    }

    [Fact]
    public async Task IncludeStreak_AddsTheStreakToEachRow_AndTheDefaultReadCarriesNone()
    {
        await _initializer.InitializeFromTemplateAsync();
        var today = DateTime.UtcNow.Date;
        DateTime At(int daysBack) => today.AddDays(-daysBack).AddHours(2).AddMinutes(10);

        /* The metric fired at 02:10 on days 1, 2, 4 and 6 back and only muted on day 3; day 5 is the gap, and a fire at 14:10 on
           it is another time of day. A recovery row sits under its own title. */
        using (var connection = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            foreach (var daysBack in new[] { 1, 2, 4, 6 })
                await _helper.InsertLiveAlertAsync(connection, At(daysBack), 1, "srv", Metric);
            await _helper.InsertLiveAlertAsync(connection, At(3), 1, "srv", Metric, muted: true);
            await _helper.InsertLiveAlertAsync(connection, At(5).AddHours(12), 1, "srv", Metric);
            await _helper.InsertLiveAlertAsync(connection, At(1).AddMinutes(5), 1, "srv", Metric + " Resolved");
            /* A 23:50 alert: its day-1 fire is the 00:10 of its own date; a count by calendar date finds no streak. */
            await _helper.InsertLiveAlertAsync(connection, today.AddDays(-1).AddHours(23).AddMinutes(50), 1, "srv", "Midnight");
            await _helper.InsertLiveAlertAsync(connection, today.AddDays(-1).AddMinutes(10), 1, "srv", "Midnight");
            await _helper.InsertLiveAlertAsync(connection, today.AddDays(-2).AddMinutes(10), 1, "srv", "Midnight", muted: true);
        }

        var service = new LocalDataService(_initializer);

        var plain = await McpAlertTools.GetAlertHistory(service, 168, 50, metric_name: Metric);
        Assert.Contains("alerts_returned", plain, StringComparison.Ordinal);
        Assert.DoesNotContain("streak", plain, StringComparison.Ordinal);

        var raw = await McpAlertTools.GetAlertHistory(service, 168, 50, include_streak: true);
        var rows = JsonDocument.Parse(raw)
            .RootElement.GetProperty("alerts").EnumerateArray().ToList();
        JsonElement Row(DateTime at, string metric) => rows.Single(a =>
            a.GetProperty("metric_name").GetString() == metric
            && DateTime.Parse(a.GetProperty("alert_time").GetString()!, null, System.Globalization.DateTimeStyles.RoundtripKind) == at);
        int Streak(DateTime at) => Row(at, Metric).GetProperty("streak_days").GetInt32();

        Assert.Equal(3, Streak(At(1)));   // days 2, 3 (muted only) and 4 back from it, then the gap at day 5
        Assert.Equal(2, Streak(At(2)));
        Assert.Equal(1, Streak(At(3)));
        Assert.Equal(0, Streak(At(4)));
        Assert.Equal(0, Streak(At(6)));
        Assert.False(Row(At(1), Metric).GetProperty("streak_at_least_days_window").GetBoolean());
        Assert.Equal(2, Row(today.AddDays(-1).AddHours(23).AddMinutes(50), "Midnight").GetProperty("streak_days").GetInt32());
        Assert.Equal(JsonValueKind.Null, Row(At(1).AddMinutes(5), Metric + " Resolved").GetProperty("streak_days").ValueKind);

        /* The page is bounded by limit, the streak is not. */
        var narrow = JsonDocument.Parse(await McpAlertTools.GetAlertHistory(service, 168, 1, metric_name: Metric, include_streak: true))
            .RootElement.GetProperty("alerts");
        Assert.Equal(1, narrow.GetArrayLength());
        Assert.Equal(3, narrow[0].GetProperty("streak_days").GetInt32());
    }
}
