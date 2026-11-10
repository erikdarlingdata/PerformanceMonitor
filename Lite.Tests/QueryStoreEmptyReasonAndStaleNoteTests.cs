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
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8541 on Lite, against a real DuckDB store. Two answers about one <c>query_store_health</c> capture:
/// <c>get_query_store_health</c>'s staleness note, which names the hourly cadence only where the collector's schedule runs it, and
/// what the Query Store panel says over a window with no rows. The panel shows its empty-window sentence and then runs
/// <see cref="ServerTab.ShowQueryStoreEmptyReasonAsync"/>, which asks the store why; each panel case runs those two steps on a real
/// slicer whose message host is a real text block (the pair the tab's XAML wires) and reads what the host shows.
/// </summary>
public sealed class QueryStoreEmptyReasonAndStaleNoteTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "QsEmptyReasonSrv";

    /// <summary>A capture time far older than the staleness threshold on any day the test runs.</summary>
    private static readonly DateTime StaleCapture = new(2026, 9, 20, 18, 0, 0, DateTimeKind.Utc);

    private readonly string _configDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;

    public QueryStoreEmptyReasonAndStaleNoteTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-qsemptyreason-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);
        var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = ServerName, DisplayName = ServerName, IsEnabled = true };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    /// <summary>One database of a <c>query_store_health</c> capture taken at <paramref name="capturedUtc"/>; the readable-secondary bit is <c>readonly_reason</c> 8.</summary>
    private async Task SeedCaptureAsync(string actualState, int readonlyReason, DateTime capturedUtc)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var conn = _duckDb.CreateConnection();
        await conn.OpenAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText =
            "INSERT INTO query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason, current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days, max_plans_per_query, interval_length_minutes, query_capture_mode, wait_stats_capture_mode) "
            + $"VALUES (-854101, TIMESTAMP '{capturedUtc.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)}', {_serverId.ToString(CultureInfo.InvariantCulture)}, '{ServerName}', 'db1', '{actualState}', 'READ_WRITE', {readonlyReason.ToString(CultureInfo.InvariantCulture)}, 100, 1000, 'AUTO', 30, 200, 60, 'AUTO', 'ON')";
        await cmd.ExecuteNonQueryAsync();
    }

    private McpCollectorRunTimes HealthSchedule(bool enabled)
    {
        var schedules = new ScheduleManager(_configDir);
        schedules.SetScheduleForServer(_serverManager.GetAllServers().Single().Id, new List<CollectorSchedule>
        {
            new() { Name = "query_store_health", Enabled = enabled, FrequencyMinutes = 60, RetentionDays = 30 },
        });
        return new McpCollectorRunTimes(schedules, _serverManager);
    }

    private async Task<string?> HealthNoteAsync(McpCollectorRunTimes runTimes)
    {
        using var answer = JsonDocument.Parse(await McpConfigTools.GetQueryStoreHealth(_dataService, _serverManager, runTimes: runTimes));
        return answer.RootElement.GetProperty("note").GetString();
    }

    [Fact]
    public async Task GetQueryStoreHealth_OverAStaleCapture_SaysTheCollectorIsDisabled_WhereItsScheduleIsOff_AndNamesTheHourlyCadenceOnlyWhereItRuns()
    {
        await SeedCaptureAsync("READ_WRITE", 0, StaleCapture);

        var disabledNote = await HealthNoteAsync(HealthSchedule(enabled: false));
        Assert.NotNull(disabledNote);
        Assert.DoesNotContain("runs hourly", disabledNote, StringComparison.Ordinal);
        Assert.Contains("the query_store_health collector is disabled", disabledNote, StringComparison.Ordinal);

        var enabledNote = await HealthNoteAsync(HealthSchedule(enabled: true));
        Assert.NotNull(enabledNote);
        Assert.Contains("the query_store_health collector runs hourly", enabledNote, StringComparison.Ordinal);
    }

    /// <summary>
    /// What the Query Store panel's message host shows once the panel has said its window is empty and run its reason step: the
    /// host's text, or null when the host is hidden.
    /// </summary>
    private string? Shown(bool healthCollectorEnabled)
    {
        string? shown = null;
        StaTestThread.Run(async () =>
        {
            var host = new TextBlock { Visibility = Visibility.Collapsed };
            var slicer = new TimeRangeSlicerControl { EmptyMessage = host };
            slicer.ShowEmpty(ServerTab.QueryStoreEmptyWindowText);
            await ServerTab.ShowQueryStoreEmptyReasonAsync(slicer, _dataService, _serverId, ServerName, healthCollectorEnabled);
            shown = host.Visibility == Visibility.Visible ? host.Text : null;
        });
        return shown;
    }

    [Fact]
    public async Task OnAReadableSecondary_ThePanelGivesThatReason_InPlaceOfTheEmptyWindowSentence()
    {
        /* The newest capture's one database is READ_ONLY with the readable-secondary bit, so Lite collects no Query Store from it. */
        await SeedCaptureAsync("READ_ONLY", 8, StaleCapture);

        var shown = Shown(healthCollectorEnabled: true);

        Assert.NotNull(shown);
        Assert.Contains("Query Store is not collected for " + ServerName, shown, StringComparison.Ordinal);
        Assert.Contains("readable secondary", shown, StringComparison.Ordinal);
        Assert.DoesNotContain(ServerTab.QueryStoreEmptyWindowText, shown, StringComparison.Ordinal);
    }

    [Fact]
    public async Task OnAServerWhoseHealthCaptureIsStale_AndItsCollectorDisabled_ThePanelSaysNoNewerCaptureIsTaken_AfterTheEmptyWindowSentence()
    {
        await SeedCaptureAsync("READ_WRITE", 0, StaleCapture);

        var shown = Shown(healthCollectorEnabled: false);

        Assert.NotNull(shown);
        Assert.StartsWith(ServerTab.QueryStoreEmptyWindowText, shown, StringComparison.Ordinal);
        Assert.Contains("the query_store_health collector is disabled for this server", shown, StringComparison.Ordinal);
        Assert.DoesNotContain("runs hourly", shown, StringComparison.Ordinal);
    }

    [Fact]
    public async Task OnAServerWhoseHealthCaptureIsFresh_ThePanelKeepsTheEmptyWindowSentence()
    {
        await SeedCaptureAsync("READ_WRITE", 0, DateTime.UtcNow.AddMinutes(-10));

        Assert.Equal(ServerTab.QueryStoreEmptyWindowText, Shown(healthCollectorEnabled: true));
    }
}
