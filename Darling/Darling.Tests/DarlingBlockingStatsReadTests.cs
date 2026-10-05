/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
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
/// get_blocking_stats (#2484), and specifically the three-source capture probe behind its empty verdict.
///
/// <para>The verdict gates on the blocking series AND the deadlock series both being empty, so the probe
/// has to cover every path that could have filled either. The first cut checked only the two BLOCKING
/// sources, which would have called a server "genuinely clear" on the strength of blocking capture alone
/// while deadlock capture had never run — the reassuring-wrong answer the probe exists to prevent, missed
/// for the deadlock half. Review caught it; there was no test here to catch it, which is why there is one
/// now.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingBlockingStatsReadTests
{
    private const int ServerId = -949554;
    private const string ServerName = "blocking-stats-read";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>One victim, one blocker, so total wait sums both and victim_count is 1.</summary>
    private const string GraphXml =
        "<deadlock><victim-list><victimProcess id=\"p1\"/></victim-list><process-list>" +
        "<process id=\"p1\" spid=\"55\" waittime=\"1000\"><inputbuf>x</inputbuf></process>" +
        "<process id=\"p2\" spid=\"66\" waittime=\"3000\"><inputbuf>y</inputbuf></process>" +
        "</process-list></deadlock>";

    [Fact]
    public async Task DeadlockCaptureAlone_CountsAsCaptured_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live blocking-stats test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            /* ── collectors never ran: NOT a clean bill of health ── */
            var never = await DarlingMcpDataTools.GetBlockingStats(dataSource, ServerName, 24);
            var neverDoc = JsonDocument.Parse(never);
            Assert.Equal("unavailable", neverDoc.RootElement.GetProperty("status").GetString());
            Assert.Contains("NOT a clean bill of health", neverDoc.RootElement.GetProperty("message").GetString()!, StringComparison.Ordinal);

            /*
                ── the healthy-server case, which the first design got backwards ──
                A collector that RAN and saw nothing, with no blocking or deadlock rows anywhere. This is
                the ordinary state of a well-behaved server, and it must read as a clear window. An
                event-existence probe answers no here -- there are no rows to find -- and would tell a
                healthy server its collection is broken, sending someone to fix what is working.
            */
            await SeedCollectorRunAsync(connection, ct, "blocked_process_report", MinutesAgo(20));

            var healthy = await DarlingMcpDataTools.GetBlockingStats(dataSource, ServerName, 24);
            var healthyDoc = JsonDocument.Parse(healthy);
            Assert.Equal("empty", healthyDoc.RootElement.GetProperty("status").GetString());
            var healthyText = healthyDoc.RootElement.GetProperty("message").GetString()!;
            /* #4966: the store's only run is 20 minutes old, so a 24-hour window is CUT, and a cut window may not say clear. */
            Assert.True(healthyDoc.RootElement.GetProperty("hints").GetProperty("window_truncated").GetBoolean());
            Assert.Equal($"No blocking or deadlocks recorded for {ServerName} in the last 24 hour(s). {McpHelpers.CutWindowNothingMessage}", healthyText);
            Assert.NotEqual(JsonValueKind.Null, healthyDoc.RootElement.GetProperty("hints").GetProperty("effective_start").ValueKind);
            Assert.DoesNotContain("NEVER", healthyText, StringComparison.Ordinal);

            /*
                ── the regression this file exists for ──
                A deadlock OUTSIDE the asked-for window, and no blocking rows at all. The server has
                plainly captured something, so the honest answer is "the window is clear". A probe that
                only looked at the two blocking sources would say "never captured" here, which is the
                same defect in the opposite direction: it tells an operator to go fix collection that is
                working.
            */
            await SeedDeadlockAsync(connection, ct, HoursAgo(48));

            var quiet = await DarlingMcpDataTools.GetBlockingStats(dataSource, ServerName, 1);
            var quietDoc = JsonDocument.Parse(quiet);
            Assert.Equal("empty", quietDoc.RootElement.GetProperty("status").GetString());
            /* #4966: a deadlock 48 hours old reaches back past the one-hour window, so it is covered and keeps the clear sentence. */
            Assert.False(quietDoc.RootElement.GetProperty("hints").GetProperty("window_truncated").GetBoolean());
            Assert.Contains("genuinely clear", quietDoc.RootElement.GetProperty("message").GetString()!, StringComparison.Ordinal);

            /* ── in-window deadlock: severity sums EVERY process, not just the victim ── */
            await SeedDeadlockAsync(connection, ct, MinutesAgo(10));

            var hit = await DarlingMcpDataTools.GetBlockingStats(dataSource, ServerName, 24);
            var severity = JsonDocument.Parse(hit).RootElement.GetProperty("deadlock_severity");
            Assert.True(severity.GetArrayLength() >= 1);

            var bucket = severity[severity.GetArrayLength() - 1];
            Assert.Equal(1, bucket.GetProperty("victim_count").GetInt32());

            /* 4000, not 1000: the blocker's wait counts too. */
            Assert.Equal(4000, bucket.GetProperty("total_wait_ms").GetInt64());
            Assert.Equal(3000, bucket.GetProperty("max_wait_ms").GetInt64());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /* ── #4966: the data-start notice. Two separate series (blocking, deadlocks), one notice at the LATER of their floors. ── */

    private static readonly string[] WindowTables = ["blocked_process_reports", "dmv_blocking_snapshots", "deadlocks"];
    private static readonly string[] WindowCollectors = ["blocked_process_reports", "dmv_blocking_snapshots", "deadlocks"];

    private static string WindowName(string w) => "blocking-stats-window-" + w;

    private static Task<string> CallWindowAsync(NpgsqlDataSource ds, string w, int hours, DateTime end) =>
        DarlingMcpDataTools.GetBlockingStats(ds, WindowName(w), hours, WebDataStartNote.FormatWindowEnd(end));

    private static Task RunWindowAsync(string w, Func<NpgsqlConnection, NpgsqlDataSource, DateTime, Task> body) =>
        WindowFloorLiveHarness.RunAsync(ConnectionString, WindowCollectors, [WindowName(w)], WindowTables, body);

    /* Registered at `created`: both series share that floor, and an event stamped before it is what moves one series' floor earlier. */
    private static Task SeedWindowAsync(NpgsqlConnection c, string w, DateTime end, DateTime created) =>
        WindowFloorLiveHarness.SeedServerAsync(c, WindowName(w), created, "blocked_process_report", null, 30, end, WindowTables, TestContext.Current.CancellationToken);

    private static Task SeedBprAsync(NpgsqlConnection c, string w, DateTime eventTime, DateTime collectedAt) =>
        DarlingMcpTestData.ExecAsync(c, TestContext.Current.CancellationToken,
            "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms) VALUES ($1,$2,$3,$4,$5,'d',50,90,8000)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectedAt), ServerIdHelper.GetDeterministicHashCode(WindowName(w)), WindowName(w), DarlingMcpTestData.Naive(eventTime));

    private static Task SeedDmvAsync(NpgsqlConnection c, string w, DateTime eventTime, DateTime collectedAt) =>
        DarlingMcpTestData.ExecAsync(c, TestContext.Current.CancellationToken,
            "INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, monitor_loop, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms) VALUES ($1,$2,$3,$4,-1,$5,'d',50,90,3000)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectedAt), ServerIdHelper.GetDeterministicHashCode(WindowName(w)), WindowName(w), DarlingMcpTestData.Naive(eventTime));

    private static Task SeedDeadlockWindowAsync(NpgsqlConnection c, string w, DateTime at, DateTime collectedAt) =>
        DarlingMcpTestData.ExecAsync(c, TestContext.Current.CancellationToken,
            "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml) VALUES ($1,$2,$3,$4,$5,'p1','x',$6)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectedAt), ServerIdHelper.GetDeterministicHashCode(WindowName(w)), WindowName(w), DarlingMcpTestData.Naive(at), GraphXml);

    private static string Start(DateTime t) => McpHelpers.FormatEffectiveStart(t);

    [Fact]
    public async Task BlockingCoveredFromBefore_DeadlocksStartInside_NamesTheDeadlockStart_AgainstDevPostgres() =>
        await RunWindowAsync("bc", async (c, ds, end) =>
        {
            await SeedWindowAsync(c, "bc", end, end.AddHours(-10));
            await SeedBprAsync(c, "bc", end.AddHours(-11), end.AddHours(-9));
            await SeedDeadlockWindowAsync(c, "bc", end.AddHours(-1), end.AddHours(-1));
            var root = WindowFloorLiveHarness.Parse(await CallWindowAsync(ds, "bc", 168, end));
            Assert.True(root.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(Start(end.AddHours(-10)), root.GetProperty("effective_start").GetString());
            Assert.Contains("blocked_process_report", root.GetProperty("truncation_note").GetString(), StringComparison.Ordinal);
            Assert.Contains("deadlocks", root.GetProperty("truncation_note").GetString(), StringComparison.Ordinal);
        });

    [Fact]
    public async Task DeadlocksCoveredFromBefore_BlockingStartsInside_NamesTheBlockingStart_AgainstDevPostgres() =>
        await RunWindowAsync("dc", async (c, ds, end) =>
        {
            await SeedWindowAsync(c, "dc", end, end.AddHours(-12));
            await SeedDeadlockWindowAsync(c, "dc", end.AddHours(-13), end.AddHours(-11));
            await SeedBprAsync(c, "dc", end.AddHours(-1), end.AddHours(-1));
            var root = WindowFloorLiveHarness.Parse(await CallWindowAsync(ds, "dc", 168, end));
            Assert.True(root.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(Start(end.AddHours(-12)), root.GetProperty("effective_start").GetString());
        });

    [Fact]
    public async Task TheLaterOfTheTwoFloors_Wins_AgainstDevPostgres() =>
        await RunWindowAsync("later", async (c, ds, end) =>
        {
            await SeedWindowAsync(c, "later", end, end.AddHours(-5));
            await SeedBprAsync(c, "later", end.AddHours(-30), end.AddHours(-4));
            await SeedDeadlockWindowAsync(c, "later", end.AddHours(-20), end.AddHours(-4));
            var root = WindowFloorLiveHarness.Parse(await CallWindowAsync(ds, "later", 168, end));
            Assert.Equal(Start(end.AddHours(-20)), root.GetProperty("effective_start").GetString());
        });

    [Fact]
    public async Task DeadlockRowsOnly_IsADataAnswer_WithTheDeadlockFloor_AgainstDevPostgres() =>
        await RunWindowAsync("donly", async (c, ds, end) =>
        {
            await SeedWindowAsync(c, "donly", end, end.AddHours(-6));
            await SeedDeadlockWindowAsync(c, "donly", end.AddHours(-2), end.AddHours(-6));
            var root = WindowFloorLiveHarness.Parse(await CallWindowAsync(ds, "donly", 168, end));
            Assert.False(root.TryGetProperty("status", out _));
            Assert.Equal(Start(end.AddHours(-6)), root.GetProperty("effective_start").GetString());
            Assert.Equal(1, root.GetProperty("deadlock_severity").GetArrayLength());
        });

    [Fact]
    public async Task XeEarlyDmvLate_GivesTheXeStartAsTheBlockingFloor_AgainstDevPostgres() =>
        await RunWindowAsync("xedmv", async (c, ds, end) =>
        {
            await SeedWindowAsync(c, "xedmv", end, end.AddHours(-3));
            await SeedBprAsync(c, "xedmv", end.AddHours(-20), end.AddHours(-2));
            await SeedDmvAsync(c, "xedmv", end.AddHours(-2), end.AddHours(-2));
            var root = WindowFloorLiveHarness.Parse(await CallWindowAsync(ds, "xedmv", 168, end));
            Assert.Equal(Start(end.AddHours(-20)), root.GetProperty("effective_start").GetString());
        });

    [Fact]
    public async Task BothCovered_GivesNoNotice_AgainstDevPostgres() =>
        await RunWindowAsync("both", async (c, ds, end) =>
        {
            await SeedWindowAsync(c, "both", end, end.AddDays(-30));
            await SeedBprAsync(c, "both", end.AddDays(-5), end.AddDays(-5));
            await SeedBprAsync(c, "both", end.AddHours(-2), end.AddHours(-2));
            await SeedDeadlockWindowAsync(c, "both", end.AddDays(-5), end.AddDays(-5));
            await SeedDeadlockWindowAsync(c, "both", end.AddHours(-2), end.AddHours(-2));
            var root = WindowFloorLiveHarness.Parse(await CallWindowAsync(ds, "both", 72, end));
            Assert.False(root.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(System.Text.Json.JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
            var names = root.EnumerateObject().Select(p => p.Name).ToList();
            Assert.Equal(["effective_start", "window_truncated", "truncation_note"], names.Skip(names.IndexOf("hours_back") + 1).Take(3));
            Assert.True(names.IndexOf("truncation_note") < names.IndexOf("blocking_duration"));
        });

    [Fact]
    public async Task AnEventOlderThanItsCollection_MovesTheFloorEarlier_AgainstDevPostgres() =>
        await RunWindowAsync("early", async (c, ds, end) =>
        {
            await SeedWindowAsync(c, "early", end, end.AddHours(-10));
            /* Collected ten hours ago, but the event happened eleven and a half hours ago. */
            await SeedBprAsync(c, "early", end.AddHours(-11).AddMinutes(-30), end.AddHours(-10));
            await SeedDeadlockWindowAsync(c, "early", end.AddHours(-11).AddMinutes(-30), end.AddHours(-10));
            var root = WindowFloorLiveHarness.Parse(await CallWindowAsync(ds, "early", 12, end));
            Assert.False(root.GetProperty("window_truncated").GetBoolean());
        });

    [Fact]
    public async Task AnEmptyAnswer_PastCoverage_CarriesHints_AgainstDevPostgres() =>
        await RunWindowAsync("empty", async (c, ds, end) =>
        {
            await SeedWindowAsync(c, "empty", end, end.AddHours(-2));
            await DarlingMcpTestData.ExecAsync(c, TestContext.Current.CancellationToken,
                "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected) VALUES ($1,$2,$3,'blocked_process_report',$4,10,'SUCCESS',0)",
                CollectionIdGenerator.Next(), ServerIdHelper.GetDeterministicHashCode(WindowName("empty")), WindowName("empty"), DarlingMcpTestData.Naive(end.AddMinutes(-5)));
            var root = WindowFloorLiveHarness.Parse(await CallWindowAsync(ds, "empty", 6, end));
            Assert.Equal("empty", root.GetProperty("status").GetString());
            var hints = root.GetProperty("hints");
            Assert.True(hints.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(Start(end.AddHours(-2)), hints.GetProperty("effective_start").GetString());
        });

    [Fact]
    public async Task AShortWindow_WithRows_StartsNoProbe_AgainstDevPostgres() =>
        await RunWindowAsync("short", async (c, ds, end) =>
        {
            await SeedWindowAsync(c, "short", end, end.AddDays(-30));
            await SeedBprAsync(c, "short", end.AddMinutes(-20), end.AddMinutes(-20));
            var calls = 0;
            DarlingMcpWindowNotice.TestOnlyProbe = () => { calls++; return Task.FromResult<DateTime?>(null); };
            var root = WindowFloorLiveHarness.Parse(await CallWindowAsync(ds, "short", 1, end));
            Assert.Equal(0, calls);
            Assert.False(root.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(1, root.GetProperty("blocking_duration").GetArrayLength());
        });

    [Fact]
    public async Task AFailedProbe_CostsTheNotice_NeverTheRows_AgainstDevPostgres() =>
        await RunWindowAsync("probefail", async (c, ds, end) =>
        {
            await SeedWindowAsync(c, "probefail", end, end.AddDays(-2));
            await SeedBprAsync(c, "probefail", end.AddHours(-3), end.AddHours(-3));
            DarlingMcpWindowNotice.TestOnlyProbe = () => throw new TimeoutException("the probe's deadline passed");
            var root = WindowFloorLiveHarness.Parse(await CallWindowAsync(ds, "probefail", 168, end));
            Assert.False(root.TryGetProperty("status", out _));
            Assert.Equal(1, root.GetProperty("blocking_duration").GetArrayLength());
            Assert.False(root.TryGetProperty("effective_start", out _));
            Assert.False(root.TryGetProperty("window_truncated", out _));
            Assert.False(root.TryGetProperty("truncation_note", out _));
        });

    private static DateTime MinutesAgo(int minutes) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-minutes));

    private static DateTime HoursAgo(int hours) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddHours(-hours));

    /// <summary>A SUCCESSFUL collector run that found nothing — the denominator the verdict rests on.</summary>
    private static async Task SeedCollectorRunAsync(
        NpgsqlConnection connection, CancellationToken ct, string collector, DateTime at) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
            CollectionIdGenerator.Next(), ServerId, ServerName, collector,
            DarlingMcpTestData.Naive(at), 100, "SUCCESS", null, 0, 80, 20);

    private static async Task SeedDeadlockAsync(NpgsqlConnection connection, CancellationToken ct, DateTime at) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName,
            DarlingMcpTestData.Naive(at), "p1", "UPDATE Users SET Reputation = 1", GraphXml);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM deadlocks WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
