/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8146: the MCP tools that read the deadlock, blocked-process-report and long-query data sets say when the window lost ring-buffer
/// events. The loss comes from the notes the collectors leave on their SUCCESS runs (<see cref="RingBufferLossNote"/>); a window's loss
/// is the runs inside it plus the first SUCCESS run after its end, of the one collector the tool reads. A data answer carries
/// <c>ring_buffer_loss</c> at the top level, an empty one under <c>hints</c>, and a clean window carries no such key. Lite twin:
/// <c>McpRingBufferLossToolTests</c>.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpRingBufferLossTests
{
    private const string LossNote = "ring_buffer_evicted=12 ring_buffer_loss_window_s=60 ring_buffer_events_limit=1000";
    private static readonly string[] Tables = ["deadlocks", "blocked_process_reports", "long_query_completions"];

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string Name(string scenario) => "darling-mcp-rbloss-" + scenario;

    private static Task RunAsync(string scenario, Func<NpgsqlConnection, NpgsqlDataSource, string, DateTime, Task> body) =>
        WindowFloorLiveHarness.RunAsync(ConnectionString, [], [Name(scenario)], Tables, async (c, ds, end) =>
        {
            var name = Name(scenario);
            await WindowFloorLiveHarness.SeedServerAsync(c, name, end.AddDays(-30), "wait_stats", null, 30, end, Tables, TestContext.Current.CancellationToken);
            await body(c, ds, name, end);
        });

    private static Task SeedRunAsync(string collector, DateTime at, string? note, NpgsqlConnection c, string name) =>
        DarlingMcpTestData.ExecAsync(c, TestContext.Current.CancellationToken,
            @"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected, error_message)
SELECT COALESCE(MAX(log_id), 0) + 1, $1, $2, $3, $4, 12, 'SUCCESS', 0, $5 FROM collection_log",
            ServerIdHelper.GetDeterministicHashCode(name), name, collector, DarlingMcpTestData.Naive(at), note);

    private static Task SeedDeadlockAsync(NpgsqlConnection c, string name, DateTime at) =>
        DarlingMcpTestData.ExecAsync(c, TestContext.Current.CancellationToken,
            @"INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml, database_name)
VALUES ($1,$2,$3,$4,$2,'process1','select 1','<deadlock/>','testdb')",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerIdHelper.GetDeterministicHashCode(name), name);

    private static Task SeedLongQueryAsync(NpgsqlConnection c, string name, DateTime at) =>
        DarlingMcpTestData.ExecAsync(c, TestContext.Current.CancellationToken,
            @"INSERT INTO long_query_completions (long_query_completion_id, collection_time, server_id, server_name, event_time, event_type, database_name, duration_microseconds, statement_text)
VALUES ($1,$2,$3,$4,$2,'rpc_completed','testdb',5000000,'EXEC p')",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerIdHelper.GetDeterministicHashCode(name), name);

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement.Clone();

    private static string AsOf(DateTime end) => WebDataStartNote.FormatWindowEnd(end);

    /* Review Focus 5: an empty answer over a lossy window is not an all-clear, so the loss rides the hints the #4966 window keys ride in. */
    [Fact]
    public async Task GetBlocking_EmptyWindowWithLoss_CarriesRingBufferLoss_AgainstDevPostgres() =>
        await RunAsync("blocking-empty", async (c, ds, name, end) =>
        {
            await SeedRunAsync(BlockedProcessReportCollector.Instance.Name, end.AddHours(-3), LossNote, c, name);
            await SeedRunAsync(BlockedProcessReportCollector.Instance.Name, end.AddMinutes(-2), null, c, name);

            var root = Parse(await DarlingMcpBlockingTools.GetBlocking(ds, name, 24, 15, as_of: AsOf(end)));

            Assert.Equal("empty", root.GetProperty("status").GetString());
            var loss = root.GetProperty("hints").GetProperty("ring_buffer_loss");
            Assert.Equal(12, loss.GetProperty("evicted_events").GetInt64());
            Assert.Equal(0, loss.GetProperty("dropped_events").GetInt64());
        });

    [Fact]
    public async Task GetDeadlocks_RowsAndLoss_TopLevelKey_AgainstDevPostgres() =>
        await RunAsync("deadlocks-rows", async (c, ds, name, end) =>
        {
            await SeedDeadlockAsync(c, name, end.AddHours(-2));
            await SeedRunAsync(DeadlocksCollector.Instance.Name, end.AddHours(-4), LossNote, c, name);
            await SeedRunAsync(DeadlocksCollector.Instance.Name, end.AddHours(-1), "xe_session_dropped=4 ring_buffer_loss_unknown=1", c, name);

            var root = Parse(await DarlingMcpBlockingTools.GetDeadlocks(ds, name, 24, 15, as_of: AsOf(end)));

            Assert.Equal(1, root.GetProperty("deadlocks_returned").GetInt32());
            var loss = root.GetProperty("ring_buffer_loss");
            Assert.Equal(12, loss.GetProperty("evicted_events").GetInt64());
            Assert.Equal(4, loss.GetProperty("dropped_events").GetInt64());
            Assert.Equal(1, loss.GetProperty("unknown_spans").GetInt32());
        });

    [Fact]
    public async Task GetLongQueryCompletions_CleanWindow_NoKey_AgainstDevPostgres() =>
        await RunAsync("longquery-clean", async (c, ds, name, end) =>
        {
            await SeedLongQueryAsync(c, name, end.AddHours(-2));
            await SeedRunAsync(LongQueryCompletionsCollector.Instance.Name, end.AddHours(-3), "ring_buffer_evicted=0 ring_buffer_loss_window_s=60 ring_buffer_events_limit=1000", c, name);
            await SeedRunAsync(LongQueryCompletionsCollector.Instance.Name, end.AddHours(-1), null, c, name);

            var json = await DarlingMcpLongQueryTools.GetLongQueryCompletions(ds, name, 24, 15, as_of: AsOf(end));

            var root = Parse(json);
            Assert.Equal(1, root.GetProperty("completions_returned").GetInt32());
            Assert.False(root.TryGetProperty("ring_buffer_loss", out _));
            Assert.DoesNotContain("ring_buffer_loss", json, StringComparison.Ordinal);
        });

    /* The events a run reads can sit in the window's last minutes while the run that lost them is the first one after its end. */
    [Fact]
    public async Task LossRunAfterWindowEnd_Counts_AgainstDevPostgres() =>
        await RunAsync("after-end", async (c, ds, name, end) =>
        {
            var pastEnd = end.AddDays(-2);
            await SeedDeadlockAsync(c, name, pastEnd.AddHours(-1));
            await SeedRunAsync(DeadlocksCollector.Instance.Name, pastEnd.AddMinutes(10), "ring_buffer_evicted=7 ring_buffer_loss_window_s=60 ring_buffer_events_limit=1000", c, name);
            await SeedRunAsync(DeadlocksCollector.Instance.Name, pastEnd.AddMinutes(40), "ring_buffer_evicted=900 ring_buffer_loss_window_s=60 ring_buffer_events_limit=1000", c, name);

            var root = Parse(await DarlingMcpBlockingTools.GetDeadlocks(ds, name, 24, 15, as_of: AsOf(pastEnd)));

            Assert.Equal(7, root.GetProperty("ring_buffer_loss").GetProperty("evicted_events").GetInt64());
        });

    [Fact]
    public async Task OtherCollectorsLoss_NotCounted_AgainstDevPostgres() =>
        await RunAsync("other-collector", async (c, ds, name, end) =>
        {
            await SeedLongQueryAsync(c, name, end.AddHours(-2));
            await SeedRunAsync(DeadlocksCollector.Instance.Name, end.AddHours(-3), LossNote, c, name);
            await SeedRunAsync(BlockedProcessReportCollector.Instance.Name, end.AddHours(-3), LossNote, c, name);
            await SeedRunAsync(LongQueryCompletionsCollector.Instance.Name, end.AddHours(-1), null, c, name);

            var root = Parse(await DarlingMcpLongQueryTools.GetLongQueryCompletions(ds, name, 24, 15, as_of: AsOf(end)));

            Assert.False(root.TryGetProperty("ring_buffer_loss", out _));
        });

    /* get_blocking_stats reads the blocked-process reports and the deadlocks: the loss of both counts, summed. */
    [Fact]
    public async Task GetBlockingStats_EmptyWindow_SumsTheLossOfBothCollectors_AgainstDevPostgres() =>
        await RunAsync("stats-empty", async (c, ds, name, end) =>
        {
            await SeedRunAsync(BlockedProcessReportCollector.Instance.Name, end.AddHours(-3), LossNote, c, name);
            await SeedRunAsync(DeadlocksCollector.Instance.Name, end.AddHours(-2), "ring_buffer_evicted=30 ring_buffer_loss_window_s=60 ring_buffer_events_limit=1000", c, name);
            await SeedRunAsync(DeadlocksCollector.Instance.Name, end.AddMinutes(-2), null, c, name);

            var root = Parse(await DarlingMcpDataTools.GetBlockingStats(ds, name, 24, as_of: AsOf(end)));

            Assert.Equal("empty", root.GetProperty("status").GetString());
            Assert.Equal(42, root.GetProperty("hints").GetProperty("ring_buffer_loss").GetProperty("evicted_events").GetInt64());
        });

    [Fact]
    public async Task GetDeadlockTrend_EmptyWindowWithLoss_CarriesRingBufferLoss_AgainstDevPostgres() =>
        await RunAsync("trend-empty", async (c, ds, name, end) =>
        {
            await SeedRunAsync(DeadlocksCollector.Instance.Name, end.AddHours(-3), LossNote, c, name);
            await SeedRunAsync(DeadlocksCollector.Instance.Name, end.AddMinutes(-2), null, c, name);

            var root = Parse(await DarlingMcpBlockingTools.GetDeadlockTrend(ds, name, 24, AsOf(end)));

            Assert.Equal("empty", root.GetProperty("status").GetString());
            Assert.Equal(12, root.GetProperty("hints").GetProperty("ring_buffer_loss").GetProperty("evicted_events").GetInt64());
        });

    [Fact]
    public async Task GetBlockingTrend_WithRowsAndLoss_TopLevelKey_AgainstDevPostgres() =>
        await RunAsync("trend-rows", async (c, ds, name, end) =>
        {
            await DarlingMcpTestData.ExecAsync(c, TestContext.Current.CancellationToken,
                @"INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid, blocking_status, database_name)
VALUES ($1,$2,$3,$4,$2,12000,60,61,'suspended','AppDb')",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(end.AddHours(-2)), ServerIdHelper.GetDeterministicHashCode(name), name);
            await SeedRunAsync(BlockedProcessReportCollector.Instance.Name, end.AddHours(-1), LossNote, c, name);

            var root = Parse(await DarlingMcpBlockingTools.GetBlockingTrend(ds, name, 24, AsOf(end), databases: default));

            Assert.Equal(12, root.GetProperty("ring_buffer_loss").GetProperty("evicted_events").GetInt64());
        });
}
