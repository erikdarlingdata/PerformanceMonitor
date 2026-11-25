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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9518: get_blocking on a server whose <c>blocked process threshold (s)</c> is 0. The blocked_process_report collector reads
/// nothing there and says so with <c>blocked_process_threshold_off=1</c> on its SUCCESS run. An empty answer stays <c>empty</c>
/// and adds <see cref="BlockedProcessThresholdOffNote.Hint"/> under <c>hints</c> when the newest report run in the window
/// carries that count and a dmv_blocking_snapshot run succeeded in the window. The collector half is in
/// <c>BlockedProcessThresholdOffTests</c>.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpBlockingThresholdOffTests
{
    private const string OffNote = "blocked_process_threshold_off=1 events_read=0 report_xml_empty=0 report_xml_unparsed=0 events_stored=0";
    private const string OnNote = "shred_gated=1 events_read=0 report_xml_empty=0 report_xml_unparsed=0 events_stored=0";
    private const string LossNote = "ring_buffer_evicted=12 ring_buffer_loss_window_s=60 ring_buffer_events_limit=1000";
    private static readonly string[] Tables = ["blocked_process_reports"];

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string Name(string scenario) => "darling-mcp-bproff-" + scenario;

    private static Task RunAsync(string scenario, Func<NpgsqlConnection, NpgsqlDataSource, string, DateTime, Task> body) =>
        WindowFloorLiveHarness.RunAsync(ConnectionString, [], [Name(scenario)], Tables, async (c, ds, end) =>
        {
            var name = Name(scenario);
            await WindowFloorLiveHarness.SeedServerAsync(c, name, end.AddDays(-30), "wait_stats", null, 30, end, Tables, TestContext.Current.CancellationToken);
            await body(c, ds, name, end);
        });

    private static Task SeedRunAsync(string collector, DateTime at, string status, string? note, NpgsqlConnection c, string name) =>
        DarlingMcpTestData.ExecAsync(c, TestContext.Current.CancellationToken,
            @"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected, error_message)
SELECT COALESCE(MAX(log_id), 0) + 1, $1, $2, $3, $4, 12, $5, 0, $6 FROM collection_log",
            ServerIdHelper.GetDeterministicHashCode(name), name, collector, DarlingMcpTestData.Naive(at), status, note);

    private static async Task<JsonElement> GetBlockingAsync(NpgsqlDataSource ds, string name, DateTime end) =>
        JsonDocument.Parse(await DarlingMcpBlockingTools.GetBlocking(ds, name, 24, 15, as_of: WebDataStartNote.FormatWindowEnd(end))).RootElement.Clone();

    private static bool HasThresholdNote(JsonElement root) =>
        root.TryGetProperty("hints", out var hints)
        && hints.ValueKind == JsonValueKind.Object
        && hints.TryGetProperty(BlockedProcessThresholdOffNote.HintKey, out _);

    private static readonly string ReportCollector = BlockedProcessReportCollector.Instance.Name;
    private static readonly string SnapshotCollector = DmvBlockingSnapshotCollector.Instance.Name;

    [Fact]
    public async Task GetBlocking_ReportsOff_NamesTheThresholdOnceTheDmvSnapshotsCoveredTheWindow_AgainstDevPostgres() =>
        await RunAsync("snapshot-cover", async (c, ds, name, end) =>
        {
            /* The report collector's older run lost events, its newest found the threshold at 0. */
            await SeedRunAsync(ReportCollector, end.AddHours(-3), "SUCCESS", LossNote, c, name);
            await SeedRunAsync(ReportCollector, end.AddMinutes(-2), "SUCCESS", OffNote, c, name);

            /* The DMV snapshot did not cover the window: its one good run is before the window and the run inside it failed. The
               answer is what it is on any server, with no word about the threshold. */
            await SeedRunAsync(SnapshotCollector, end.AddHours(-25), "SUCCESS", null, c, name);
            await SeedRunAsync(SnapshotCollector, end.AddMinutes(-5), "ERROR", "Timeout expired.", c, name);

            var uncovered = await GetBlockingAsync(ds, name, end);
            Assert.Equal("empty", uncovered.GetProperty("status").GetString());
            Assert.False(HasThresholdNote(uncovered));

            /* One successful snapshot run inside the window: still empty, and now the hint says why the reports half is silent. */
            await SeedRunAsync(SnapshotCollector, end.AddMinutes(-1), "SUCCESS", null, c, name);

            var covered = await GetBlockingAsync(ds, name, end);
            Assert.Equal("empty", covered.GetProperty("status").GetString());
            Assert.Equal(uncovered.GetProperty("message").GetString(), covered.GetProperty("message").GetString());
            var hints = covered.GetProperty("hints");
            Assert.Equal(BlockedProcessThresholdOffNote.Hint, hints.GetProperty(BlockedProcessThresholdOffNote.HintKey).GetString());

            /* Beside what the hints already carried, not instead of it: every key the uncovered answer had is still there with the same value. */
            foreach (var before in uncovered.GetProperty("hints").EnumerateObject())
            {
                Assert.Equal(before.Value.GetRawText(), hints.GetProperty(before.Name).GetRawText());
            }

            Assert.Equal(12, hints.GetProperty("ring_buffer_loss").GetProperty("evicted_events").GetInt64());
        });

    [Fact]
    public async Task GetBlocking_TheNewestReportRunInTheWindowDecides_AgainstDevPostgres() =>
        await RunAsync("newest-run", async (c, ds, name, end) =>
        {
            await SeedRunAsync(SnapshotCollector, end.AddMinutes(-90), "SUCCESS", null, c, name);
            await SeedRunAsync(SnapshotCollector, end.AddMinutes(-1), "SUCCESS", null, c, name);

            /* Threshold at 0 two hours ago. A failed run after it reads nothing about the setting, so the off run is still the newest that did. */
            await SeedRunAsync(ReportCollector, end.AddHours(-2), "SUCCESS", OffNote, c, name);
            await SeedRunAsync(ReportCollector, end.AddMinutes(-80), "ERROR", "Timeout expired.", c, name);

            Assert.True(HasThresholdNote(await GetBlockingAsync(ds, name, end)));

            /* Somebody set the threshold: the next run reads the session and carries no off count, and the hint goes with it. */
            await SeedRunAsync(ReportCollector, end.AddMinutes(-10), "SUCCESS", OnNote, c, name);

            var now = await GetBlockingAsync(ds, name, end);
            Assert.Equal("empty", now.GetProperty("status").GetString());
            Assert.False(HasThresholdNote(now));

            /* A window that ends before that run still reads the reports as off: the run at the window's end is in, a later one is not. */
            Assert.True(HasThresholdNote(await GetBlockingAsync(ds, name, end.AddMinutes(-80))));
            Assert.False(HasThresholdNote(await GetBlockingAsync(ds, name, end.AddMinutes(-10))));
        });
}
