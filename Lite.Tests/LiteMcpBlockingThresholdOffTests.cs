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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9518: get_blocked_process_reports on a server whose <c>blocked process threshold (s)</c> is 0. The report collector reads
/// nothing there and says so with <c>blocked_process_threshold_off=1</c> on its SUCCESS run. An empty answer stays <c>empty</c>
/// and adds <see cref="BlockedProcessThresholdOffNote.Hint"/> under <c>hints</c> when the newest report run in the window
/// carries that count and a dmv_blocking_snapshot run succeeded in the window. Darling twin:
/// <c>DarlingMcpBlockingThresholdOffTests</c>. Own <see cref="DuckDbInitializer"/> per test, anchored at a fixed <c>as_of</c>.
/// </summary>
[Trait("Cost", "Slow")]
public sealed class LiteMcpBlockingThresholdOffTests : IDisposable
{
    private const string ServerName = "ThresholdOffServer";
    private const string AsOf = "2026-09-10T12:00:00Z";
    private const string OffNote = "blocked_process_threshold_off=1 events_read=0 report_xml_empty=0 report_xml_unparsed=0 events_stored=0";
    private const string OnNote = "shred_gated=1 events_read=0 report_xml_empty=0 report_xml_unparsed=0 events_stored=0";
    private const string LossNote = "ring_buffer_evicted=12 ring_buffer_loss_window_s=60 ring_buffer_events_limit=1000";
    private static readonly DateTime Anchor = new(2026, 9, 10, 12, 0, 0, DateTimeKind.Utc);
    private static readonly string ReportCollector = BlockedProcessReportCollector.Instance.Name;
    private static readonly string SnapshotCollector = DmvBlockingSnapshotCollector.Instance.Name;

    private readonly int _serverId;
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly PendingSeedSession _seed;
    private readonly ServerManager _serverManager;
    private long _nextId = 1;

    public LiteMcpBlockingThresholdOffTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteMcpThresholdOff_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(Path.Combine(_tempDir, "config"));
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        _seed = new PendingSeedSession(_duckDb);

        _serverManager = new ServerManager(Path.Combine(_tempDir, "config"));
        var server = new ServerConnection { ServerName = ServerName, DisplayName = ServerName };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seed.Dispose();
        _duckDb.Dispose();
        if (Directory.Exists(_tempDir))
        {
            try
            {
                Directory.Delete(_tempDir, recursive: true);
            }
            catch (IOException)
            {
                /* The temp folder is the OS's to reclaim; a file still held by the engine must not fail a passing test. */
                return;
            }
        }
    }

    private static DateTime Naive(DateTime instant) => DateTime.SpecifyKind(instant, DateTimeKind.Unspecified);

    private static JsonElement Root(string json) => JsonDocument.Parse(json).RootElement.Clone();

    private LocalDataService Service()
    {
        _seed.Flush();
        return new(_duckDb);
    }

    private Task SeedRunAsync(string collector, DateTime at, string status, string? note) => _seed.ExecuteAsync(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected, error_message)
VALUES ($1, $2, $3, $4, $5, 12, $6, 0, $7)",
        _nextId++, _serverId, ServerName, collector, Naive(at), status, note);

    private Task SeedBlockedReportAsync(DateTime at) => _seed.ExecuteAsync(
        "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, event_time, server_id, server_name, wait_time_ms) VALUES ($1, $2, $2, $3, $4, 12000)",
        _nextId++, Naive(at), _serverId, ServerName);

    private async Task<JsonElement> GetReportsAsync(string asOf = AsOf) =>
        Root(await McpBlockingTools.GetBlockedProcessReports(Service(), _serverManager, ServerName, 24, 15, as_of: asOf));

    private static bool HasThresholdNote(JsonElement root) =>
        root.TryGetProperty("hints", out var hints)
        && hints.ValueKind == JsonValueKind.Object
        && hints.TryGetProperty(BlockedProcessThresholdOffNote.HintKey, out _);

    [Fact]
    public async Task GetBlockedProcessReports_ReportsOff_NamesTheThresholdOnceTheDmvSnapshotsCoveredTheWindow()
    {
        await _duckDb.InitializeFromTemplateAsync();
        /* The report collector's older run lost events, its newest found the threshold at 0. */
        await SeedRunAsync(ReportCollector, Anchor.AddHours(-3), "SUCCESS", LossNote);
        await SeedRunAsync(ReportCollector, Anchor.AddMinutes(-2), "SUCCESS", OffNote);

        /* The DMV snapshot did not cover the window: its one good run is before the window and the run inside it failed. The
           answer is what it is on any server, with no word about the threshold. */
        await SeedRunAsync(SnapshotCollector, Anchor.AddHours(-25), "SUCCESS", null);
        await SeedRunAsync(SnapshotCollector, Anchor.AddMinutes(-5), "ERROR", "Timeout expired.");

        var uncovered = await GetReportsAsync();
        Assert.Equal("empty", uncovered.GetProperty("status").GetString());
        Assert.False(HasThresholdNote(uncovered));

        /* One successful snapshot run inside the window: still empty, and now the hint says why the reports half is silent. */
        await SeedRunAsync(SnapshotCollector, Anchor.AddMinutes(-1), "SUCCESS", null);

        var covered = await GetReportsAsync();
        Assert.Equal("empty", covered.GetProperty("status").GetString());
        Assert.Equal(uncovered.GetProperty("message").GetString(), covered.GetProperty("message").GetString());
        var hints = covered.GetProperty("hints");
        Assert.Equal(BlockedProcessThresholdOffNote.Hint, hints.GetProperty(BlockedProcessThresholdOffNote.HintKey).GetString());

        /* Beside what the hints already carried, not instead of it: every key the uncovered answer had is still there with the same
           value, the #8146 loss among them. */
        foreach (var before in uncovered.GetProperty("hints").EnumerateObject())
        {
            Assert.Equal(before.Value.GetRawText(), hints.GetProperty(before.Name).GetRawText());
        }

        Assert.Equal(12, hints.GetProperty("ring_buffer_loss").GetProperty("evicted_events").GetInt64());
    }

    [Fact]
    public async Task GetBlockedProcessReports_TheNewestReportRunInTheWindowDecides()
    {
        await _duckDb.InitializeFromTemplateAsync();
        await SeedRunAsync(SnapshotCollector, Anchor.AddMinutes(-90), "SUCCESS", null);
        await SeedRunAsync(SnapshotCollector, Anchor.AddMinutes(-1), "SUCCESS", null);

        /* Threshold at 0 two hours ago. A failed run after it reads nothing about the setting, so the off run is still the newest that did. */
        await SeedRunAsync(ReportCollector, Anchor.AddHours(-2), "SUCCESS", OffNote);
        await SeedRunAsync(ReportCollector, Anchor.AddMinutes(-80), "ERROR", "Timeout expired.");

        Assert.True(HasThresholdNote(await GetReportsAsync()));

        /* Somebody set the threshold: the next run reads the session and carries no off count, and the hint goes with it. */
        await SeedRunAsync(ReportCollector, Anchor.AddMinutes(-10), "SUCCESS", OnNote);

        var now = await GetReportsAsync();
        Assert.Equal("empty", now.GetProperty("status").GetString());
        Assert.False(HasThresholdNote(now));

        /* A window that ends before that run still reads the reports as off: the run at the window's end is in, a later one is not. */
        Assert.True(HasThresholdNote(await GetReportsAsync(Anchor.AddMinutes(-80).ToString("o"))));
        Assert.False(HasThresholdNote(await GetReportsAsync(Anchor.AddMinutes(-10).ToString("o"))));
    }

    [Fact]
    public async Task GetBlockedProcessReports_RunsOutsideTheWindow_AddNoHint()
    {
        await _duckDb.InitializeFromTemplateAsync();
        /* Both collectors ran before the 24-hour window began. */
        await SeedRunAsync(ReportCollector, Anchor.AddHours(-30), "SUCCESS", OffNote);
        await SeedRunAsync(SnapshotCollector, Anchor.AddHours(-30), "SUCCESS", null);
        /* Another collector's recent run keeps the server reading as online, so the answer is the empty one and not the offline one. */
        await SeedRunAsync(DeadlocksCollector.Instance.Name, Anchor.AddMinutes(-2), "SUCCESS", null);

        var root = await GetReportsAsync();

        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.False(HasThresholdNote(root));
    }

    [Fact]
    public async Task GetBlockedProcessReports_WithReports_AnswerCarriesNoThresholdHint()
    {
        await _duckDb.InitializeFromTemplateAsync();
        await SeedBlockedReportAsync(Anchor.AddHours(-2));
        await SeedRunAsync(ReportCollector, Anchor.AddMinutes(-2), "SUCCESS", OffNote);
        await SeedRunAsync(SnapshotCollector, Anchor.AddMinutes(-1), "SUCCESS", null);

        var json = await McpBlockingTools.GetBlockedProcessReports(Service(), _serverManager, ServerName, 24, 15, as_of: AsOf);
        var root = Root(json);

        Assert.False(root.TryGetProperty("status", out var status) && status.GetString() == "empty");
        Assert.False(HasThresholdNote(root));
        Assert.DoesNotContain(BlockedProcessThresholdOffNote.HintKey, json, StringComparison.Ordinal);
    }
}
