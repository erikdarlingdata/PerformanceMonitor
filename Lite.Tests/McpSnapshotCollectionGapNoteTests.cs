/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9339, Lite's twin of Darling's live test: an empty answer from <c>get_waiting_tasks</c>, <c>get_active_queries</c> or
/// <c>get_load_by_caller</c> for a server that is online says so when the window holds skipped collection slots or failed runs of
/// the snapshot collector, through <see cref="CollectionGapNote"/>. A gap outside the window, a failed run of another collector and
/// a collector run that ended SKIPPED add nothing. An offline server keeps its offline answer.
/// </summary>
public sealed class McpSnapshotCollectionGapNoteTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private long _nextId = 1;

    public McpSnapshotCollectionGapNoteTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _tempDir = Path.Combine(Path.GetTempPath(), "McpGapNote_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(Path.Combine(_tempDir, "config"));

        _serverManager = new ServerManager(Path.Combine(_tempDir, "config"));
        var server = new ServerConnection { ServerName = "TestServer", DisplayName = "TestServer" };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    public static TheoryData<string> Families() => new() { "get_waiting_tasks", "get_active_queries", "get_load_by_caller" };

    private static string CollectorOf(string family) => family == "get_waiting_tasks" ? "waiting_tasks" : "query_snapshots";

    private async Task<JsonElement> ReadAsync(string family)
    {
        var dataService = new LocalDataService(_duckDb);
        var answer = family switch
        {
            "get_waiting_tasks" => await McpWaitTools.GetWaitingTasks(dataService, _serverManager, "TestServer", hours_back: 1),
            "get_active_queries" => await McpSessionTools.GetActiveQueries(dataService, _serverManager, "TestServer", hours_back: 1),
            "get_load_by_caller" => await McpWhoDroveItTools.GetLoadByCaller(dataService, _serverManager, "TestServer", hours_back: 1),
            _ => throw new ArgumentOutOfRangeException(nameof(family), family, null),
        };
        return JsonDocument.Parse(answer).RootElement;
    }

    private static bool HasGapHint(JsonElement root) =>
        root.TryGetProperty("hints", out var hints) && hints.ValueKind == JsonValueKind.Object && hints.TryGetProperty(CollectionGapNote.HintKey, out _);

    [Theory]
    [MemberData(nameof(Families))]
    public async Task EmptyAnswer_NamesSkippedSlotsAndFailedRuns_InTheWindow(string family)
    {
        var now = DateTime.UtcNow;
        await SeedRunAsync(CollectorOf(family), now.AddMinutes(-1), "SUCCESS", null);
        await SeedRunAsync(CollectionCycleSkip.CollectorName, now.AddMinutes(-40), CollectionCycleSkip.Status, 60);
        await SeedRunAsync(CollectionCycleSkip.CollectorName, now.AddMinutes(-30), CollectionCycleSkip.Status, 45);
        foreach (var minutes in new[] { 35, 33, 31 })
        {
            await SeedRunAsync(CollectorOf(family), now.AddMinutes(-minutes), "ERROR", null);
        }

        var root = await ReadAsync(family);

        Assert.Equal("empty", root.GetProperty("status").GetString());
        var gap = root.GetProperty("hints").GetProperty(CollectionGapNote.HintKey);
        Assert.Equal(105, gap.GetProperty("skipped_slots").GetInt64());
        Assert.Equal(2, gap.GetProperty("skipped_records").GetInt64());
        Assert.Equal(3, gap.GetProperty("failed_runs").GetInt64());
        Assert.Equal(CollectorOf(family), gap.GetProperty("collector").GetString());
        Assert.Contains("105 collection slots were skipped", root.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Contains("3 runs of the " + CollectorOf(family) + " collector did not succeed", root.GetProperty("message").GetString(), StringComparison.Ordinal);
    }

    [Theory]
    [MemberData(nameof(Families))]
    public async Task EmptyAnswer_IgnoresGapsOutsideTheWindow_AndOtherCollectors(string family)
    {
        var now = DateTime.UtcNow;
        await SeedRunAsync(CollectorOf(family), now.AddMinutes(-1), "SUCCESS", null);
        await SeedRunAsync(CollectionCycleSkip.CollectorName, now.AddHours(-5), CollectionCycleSkip.Status, 60);
        await SeedRunAsync(CollectorOf(family), now.AddHours(-5), "ERROR", null);
        /* A failed run of ANOTHER collector inside the window is not this collector's gap. */
        await SeedRunAsync("wait_stats", now.AddMinutes(-20), "ERROR", null);

        var root = await ReadAsync(family);

        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.False(HasGapHint(root));
        Assert.DoesNotContain("did not run cleanly", root.GetProperty("message").GetString(), StringComparison.Ordinal);
    }

    [Theory]
    [MemberData(nameof(Families))]
    public async Task EmptyAnswer_CountsASkippedRunAsHealthy_AndAWarningRunAsFailed(string family)
    {
        var now = DateTime.UtcNow;
        await SeedRunAsync(CollectorOf(family), now.AddMinutes(-1), "SUCCESS", null);
        /* SKIPPED is healthy everywhere else (EnumeratedCollectorDriver.FreshnessSuccessStatuses), so two of them are no failed runs. */
        await SeedRunAsync(CollectorOf(family), now.AddMinutes(-20), "SKIPPED", null);
        await SeedRunAsync(CollectorOf(family), now.AddMinutes(-10), "SKIPPED", null);

        var healthy = await ReadAsync(family);

        Assert.Equal("empty", healthy.GetProperty("status").GetString());
        Assert.False(HasGapHint(healthy));
        Assert.DoesNotContain("did not succeed", healthy.GetProperty("message").GetString(), StringComparison.Ordinal);

        /* A WARNING run partly failed, so it still counts. */
        await SeedRunAsync(CollectorOf(family), now.AddMinutes(-5), "WARNING", null);

        var warned = await ReadAsync(family);

        Assert.Equal(1, warned.GetProperty("hints").GetProperty(CollectionGapNote.HintKey).GetProperty("failed_runs").GetInt64());
    }

    [Theory]
    [MemberData(nameof(Families))]
    public async Task OfflineServer_KeepsItsOfflineAnswer(string family)
    {
        var now = DateTime.UtcNow;
        /* The newest successful run is twelve days old: offline. Skipped slots in the window do not change that answer. */
        await SeedRunAsync(CollectorOf(family), now.AddDays(-12), "SUCCESS", null);
        await SeedRunAsync(CollectionCycleSkip.CollectorName, now.AddMinutes(-30), CollectionCycleSkip.Status, 45);

        var root = await ReadAsync(family);

        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        Assert.False(HasGapHint(root));
    }

    private async Task SeedRunAsync(string collector, DateTime atUtc, string status, long? fanoutItems)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status, rows_collected, fanout_item_count)
VALUES ($1, $2, $3, $4, $5, $6, 0, $7)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestServer" });
        cmd.Parameters.Add(new DuckDBParameter { Value = collector });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(atUtc, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = status });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)fanoutItems ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}
