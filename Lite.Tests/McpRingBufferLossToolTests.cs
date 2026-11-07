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
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8146: the MCP tools that read the deadlock, blocked-process-report and long-query data sets say when the window lost ring-buffer
/// events. The loss comes from the notes the collectors leave on their SUCCESS runs (<c>RingBufferLossNote</c>); a window's loss is the
/// runs inside it plus the first SUCCESS run after its end, of the one collector the tool reads. A data answer carries
/// <c>ring_buffer_loss</c> at the top level, an empty one under <c>hints</c>, and a clean window carries no such key. Darling twin:
/// <c>DarlingMcpRingBufferLossTests</c>. Own <see cref="DuckDbInitializer"/> per test, anchored at a fixed <c>as_of</c>.
/// </summary>
[Trait("Cost", "Slow")]
public sealed class McpRingBufferLossToolTests : IDisposable
{
    private const string ServerName = "RingBufferLossServer";
    private const string AsOf = "2026-09-10T12:00:00Z";
    private const string LossNote = "ring_buffer_evicted=12 ring_buffer_loss_window_s=60 ring_buffer_events_limit=1000";
    private static readonly DateTime Anchor = new(2026, 9, 10, 12, 0, 0, DateTimeKind.Utc);

    private readonly int _serverId;
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly PendingSeedSession _seed;
    private readonly ServerManager _serverManager;
    private long _nextId = 1;

    public McpRingBufferLossToolTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "McpRingBufferLoss_" + Guid.NewGuid().ToString("N")[..8]);
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

    private Task ExecuteAsync(string sql, params object?[] values) => _seed.ExecuteAsync(sql, values);

    private Task SeedRunAsync(string collector, DateTime at, string? note) => ExecuteAsync(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected, error_message)
VALUES ($1, $2, $3, $4, $5, 12, 'SUCCESS', 0, $6)",
        _nextId++, _serverId, ServerName, collector, Naive(at), note);

    private Task SeedDeadlockAsync(DateTime at) => ExecuteAsync(
        "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time) VALUES ($1, $2, $3, $4, $2)",
        _nextId++, Naive(at), _serverId, ServerName);

    private Task SeedBlockedReportAsync(DateTime at) => ExecuteAsync(
        "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, event_time, server_id, server_name, wait_time_ms) VALUES ($1, $2, $2, $3, $4, 12000)",
        _nextId++, Naive(at), _serverId, ServerName);

    private Task SeedLongQueryAsync(DateTime at) => ExecuteAsync(
        @"INSERT INTO long_query_completions (long_query_completion_id, collection_time, server_id, server_name, event_time, event_type, database_name, statement_text, duration_microseconds)
VALUES ($1, $2, $3, $4, $2, 'rpc_completed', 'testdb', 'EXEC p', 5000000)",
        _nextId++, Naive(at), _serverId, ServerName);

    private Task<string> DeadlocksAsync(string asOf = AsOf) =>
        McpBlockingTools.GetDeadlocks(Service(), _serverManager, ServerName, 24, 15, asOf);

    private Task<string> LongQueriesAsync() =>
        McpLongQueryTools.GetLongQueryCompletions(Service(), _serverManager, ServerName, 24, 15, AsOf);

    /* Review Focus 5: an empty answer over a lossy window is not an all-clear, so the loss rides the hints the #4966 window keys ride in. */
    [Fact]
    public async Task GetBlockedProcessReports_EmptyWindowWithLoss_CarriesRingBufferLoss()
    {
        await _duckDb.InitializeFromTemplateAsync();
        await SeedRunAsync(BlockedProcessReportCollector.Instance.Name, Anchor.AddHours(-3), LossNote);
        await SeedRunAsync(BlockedProcessReportCollector.Instance.Name, Anchor.AddMinutes(-2), null);

        var root = Root(await McpBlockingTools.GetBlockedProcessReports(Service(), _serverManager, ServerName, 24, 15, as_of: AsOf));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        var loss = root.GetProperty("hints").GetProperty("ring_buffer_loss");
        Assert.Equal(12, loss.GetProperty("evicted_events").GetInt64());
        Assert.Equal(0, loss.GetProperty("dropped_events").GetInt64());
    }

    [Fact]
    public async Task GetDeadlocks_RowsAndLoss_TopLevelKey()
    {
        await _duckDb.InitializeFromTemplateAsync();
        await SeedDeadlockAsync(Anchor.AddHours(-2));
        await SeedRunAsync(DeadlocksCollector.Instance.Name, Anchor.AddHours(-4), LossNote);
        await SeedRunAsync(DeadlocksCollector.Instance.Name, Anchor.AddHours(-1), "xe_session_dropped=4 ring_buffer_loss_unknown=1");

        var root = Root(await DeadlocksAsync());

        Assert.Equal(1, root.GetProperty("deadlocks_returned").GetInt32());
        var loss = root.GetProperty("ring_buffer_loss");
        Assert.Equal(12, loss.GetProperty("evicted_events").GetInt64());
        Assert.Equal(4, loss.GetProperty("dropped_events").GetInt64());
        Assert.Equal(1, loss.GetProperty("unknown_spans").GetInt32());
    }

    [Fact]
    public async Task GetLongQueryCompletions_CleanWindow_NoKey()
    {
        await _duckDb.InitializeFromTemplateAsync();
        await SeedLongQueryAsync(Anchor.AddHours(-2));
        await SeedRunAsync(LongQueryCompletionsCollector.Instance.Name, Anchor.AddHours(-3), "ring_buffer_evicted=0 ring_buffer_loss_window_s=60 ring_buffer_events_limit=1000");
        await SeedRunAsync(LongQueryCompletionsCollector.Instance.Name, Anchor.AddHours(-1), null);

        var json = await LongQueriesAsync();

        var root = Root(json);
        Assert.Equal(1, root.GetProperty("completions_returned").GetInt32());
        Assert.False(root.TryGetProperty("ring_buffer_loss", out _));
        Assert.DoesNotContain("ring_buffer_loss", json, StringComparison.Ordinal);
    }

    /* The events a run reads can sit in the window's last minutes while the run that lost them is the first one after its end. */
    [Fact]
    public async Task LossRunAfterWindowEnd_Counts()
    {
        await _duckDb.InitializeFromTemplateAsync();
        var pastEnd = Anchor.AddDays(-2);
        await SeedDeadlockAsync(pastEnd.AddHours(-1));
        await SeedRunAsync(DeadlocksCollector.Instance.Name, pastEnd.AddMinutes(10), "ring_buffer_evicted=7 ring_buffer_loss_window_s=60 ring_buffer_events_limit=1000");
        await SeedRunAsync(DeadlocksCollector.Instance.Name, pastEnd.AddMinutes(40), "ring_buffer_evicted=900 ring_buffer_loss_window_s=60 ring_buffer_events_limit=1000");

        var root = Root(await DeadlocksAsync("2026-09-08T12:00:00Z"));

        Assert.Equal(7, root.GetProperty("ring_buffer_loss").GetProperty("evicted_events").GetInt64());
    }

    [Fact]
    public async Task OtherCollectorsLoss_NotCounted()
    {
        await _duckDb.InitializeFromTemplateAsync();
        await SeedLongQueryAsync(Anchor.AddHours(-2));
        await SeedRunAsync(DeadlocksCollector.Instance.Name, Anchor.AddHours(-3), LossNote);
        await SeedRunAsync(BlockedProcessReportCollector.Instance.Name, Anchor.AddHours(-3), LossNote);
        await SeedRunAsync(LongQueryCompletionsCollector.Instance.Name, Anchor.AddHours(-1), null);

        var root = Root(await LongQueriesAsync());

        Assert.False(root.TryGetProperty("ring_buffer_loss", out _));
    }

    /* get_blocking_stats reads the blocked-process reports and the deadlocks: the loss of both counts, summed. */
    [Fact]
    public async Task GetBlockingStats_EmptyWindow_SumsTheLossOfBothCollectors()
    {
        await _duckDb.InitializeFromTemplateAsync();
        await SeedRunAsync(BlockedProcessReportCollector.Instance.Name, Anchor.AddHours(-3), LossNote);
        await SeedRunAsync(DeadlocksCollector.Instance.Name, Anchor.AddHours(-2), "ring_buffer_evicted=30 ring_buffer_loss_window_s=60 ring_buffer_events_limit=1000");
        await SeedRunAsync(DeadlocksCollector.Instance.Name, Anchor.AddMinutes(-2), null);

        var root = Root(await McpHealthTools.GetBlockingStats(Service(), _serverManager, ServerName, 24, AsOf));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.Equal(42, root.GetProperty("hints").GetProperty("ring_buffer_loss").GetProperty("evicted_events").GetInt64());
    }

    [Fact]
    public async Task GetDeadlockTrend_EmptyWindowWithLoss_CarriesRingBufferLoss()
    {
        await _duckDb.InitializeFromTemplateAsync();
        await SeedRunAsync(DeadlocksCollector.Instance.Name, Anchor.AddHours(-3), LossNote);
        await SeedRunAsync(DeadlocksCollector.Instance.Name, Anchor.AddMinutes(-2), null);

        var root = Root(await McpBlockingTools.GetDeadlockTrend(Service(), _serverManager, ServerName, 24, AsOf));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.Equal(12, root.GetProperty("hints").GetProperty("ring_buffer_loss").GetProperty("evicted_events").GetInt64());
    }

    [Fact]
    public async Task GetBlockingTrend_WithRowsAndLoss_TopLevelKey()
    {
        await _duckDb.InitializeFromTemplateAsync();
        await SeedBlockedReportAsync(Anchor.AddHours(-2));
        await SeedRunAsync(BlockedProcessReportCollector.Instance.Name, Anchor.AddHours(-1), LossNote);

        var root = Root(await McpBlockingTools.GetBlockingTrend(Service(), _serverManager, ServerName, 24, AsOf));

        Assert.Equal(12, root.GetProperty("ring_buffer_loss").GetProperty("evicted_events").GetInt64());
    }
}
