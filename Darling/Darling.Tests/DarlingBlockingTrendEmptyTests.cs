/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// get_blocking_trend / get_deadlock_trend (#2485). Both used to serialize <c>{ server, hours_back, trend }</c>
/// unconditionally, so a server that never blocked and a server that never collected produced the SAME bytes.
/// A person looking at the web dashboard could infer the difference from the neighbouring panels; an MCP client
/// has only the JSON, and on a monitoring tool turning a clean bill of health into an apparent gap in coverage
/// is close to the worst available failure.
///
/// <para>The fix is not merely an envelope. These are EDGE tables — rows exist only where an event happened —
/// so the read itself cannot supply the denominator, and a bare "unavailable" would replace one ambiguity with
/// a friendlier-sounding one. The denominator comes from <c>collection_log</c>, which records a SUCCESS with
/// zero rows for a collector that ran and saw nothing, and it is what separates "42 collector runs saw no
/// blocking" from "no collector run happened".</para>
///
/// <para>The blocking case probes BOTH capture paths on purpose. The trend unions the XE blocked-process
/// reports with the always-on DMV snapshot, so counting only the XE collector would report "never captured"
/// for an RDS server capturing perfectly well through the DMV — the wrong branch in exactly the case the
/// probe exists to get right. That is what <see cref="DmvOnlyCapture_StillCountsAsHavingLooked_AgainstDevPostgres"/>
/// holds down.</para>
///
/// <para>Gated on DARLING_TEST_PG like every other live class.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingBlockingTrendEmptyTests
{
    private const int ServerId = -949554;
    private const string ServerName = "blocking-trend-empty";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task NeverCollected_AGapInTheWindow_AndAGenuineAllClear_AreThreeDifferentAnswers_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live blocking-trend empty test.");

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

            /* ── never collected: NOT an all-clear, and it must refuse to read as one ── */
            var never = await DarlingMcpBlockingTools.GetBlockingTrend(dataSource, ServerName, 4);
            var neverRoot = JsonDocument.Parse(never).RootElement;
            Assert.Equal("unavailable", neverRoot.GetProperty("status").GetString());
            var neverText = neverRoot.GetProperty("message").GetString()!;
            Assert.Contains("NOT an all-clear", neverText, StringComparison.Ordinal);
            Assert.Contains("EVER", neverText, StringComparison.Ordinal);
            Assert.Equal(0, neverRoot.GetProperty("hints").GetProperty("capture_count").GetInt64());

            /* ── collected before, but nothing inside THIS window: a gap, not a dead collector ── */
            await SeedRunAsync(connection, ct, "blocked_process_report", HoursAgo(48));

            var gap = await DarlingMcpBlockingTools.GetBlockingTrend(dataSource, ServerName, 1);
            var gapRoot = JsonDocument.Parse(gap).RootElement;
            Assert.Equal("unavailable", gapRoot.GetProperty("status").GetString());
            var gapText = gapRoot.GetProperty("message").GetString()!;
            Assert.Contains("NOT an all-clear", gapText, StringComparison.Ordinal);
            Assert.Contains("widen hours_back", gapText, StringComparison.Ordinal);

            /* Same status as the case above and it must NOT reach for the same word: a server that has
               collected before is a coverage gap, not a server that never collected. */
            Assert.DoesNotContain("EVER", gapText, StringComparison.Ordinal);

            /* ── the collector ran inside the window and stored nothing: a genuine all-clear ── */
            await SeedRunAsync(connection, ct, "blocked_process_report", MinutesAgo(10));
            await SeedRunAsync(connection, ct, "dmv_blocking_snapshot", MinutesAgo(10));

            var clear = await DarlingMcpBlockingTools.GetBlockingTrend(dataSource, ServerName, 4);
            var clearRoot = JsonDocument.Parse(clear).RootElement;
            Assert.Equal("empty", clearRoot.GetProperty("status").GetString());
            var clearText = clearRoot.GetProperty("message").GetString()!;
            Assert.Contains("genuine all-clear", clearText, StringComparison.Ordinal);

            /* Same zero rows as both cases above, and it must share wording with NEITHER. */
            Assert.DoesNotContain("EVER", clearText, StringComparison.Ordinal);
            Assert.DoesNotContain("NOT an all-clear", clearText, StringComparison.Ordinal);

            /* The denominator is the honest part: "no blocking" means something different across two
               captures than across two hundred, and the caller cannot supply that number itself. */
            var hints = clearRoot.GetProperty("hints");
            Assert.Equal(2, hints.GetProperty("capture_count").GetInt64());
            Assert.Equal(2, hints.GetProperty("captures").GetArrayLength());

            /* ── a real event: the trend payload comes back, envelope gone ── */
            await SeedBlockedProcessReportAsync(connection, ct, MinutesAgo(5));

            var hit = await DarlingMcpBlockingTools.GetBlockingTrend(dataSource, ServerName, 4);
            var hitRoot = JsonDocument.Parse(hit).RootElement;
            Assert.False(hitRoot.TryGetProperty("status", out _));
            Assert.Equal(1, hitRoot.GetProperty("trend").GetArrayLength());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// The two-capture-path guard. An RDS instance cannot run the blocked-process-report XE session, so its
    /// blocking arrives entirely through the DMV snapshot — and a probe that only counted
    /// <c>blocked_process_report</c> runs would tell that server its blocking has never been captured, which
    /// is the exact wrong branch on a server whose collection is fine.
    /// </summary>
    [Fact]
    public async Task DmvOnlyCapture_StillCountsAsHavingLooked_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live blocking-trend empty test.");

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
            await SeedRunAsync(connection, ct, "dmv_blocking_snapshot", MinutesAgo(10));

            var root = JsonDocument.Parse(
                await DarlingMcpBlockingTools.GetBlockingTrend(dataSource, ServerName, 4)).RootElement;

            Assert.Equal("empty", root.GetProperty("status").GetString());
            Assert.Contains("genuine all-clear", root.GetProperty("message").GetString()!, StringComparison.Ordinal);
            Assert.Equal(1, root.GetProperty("hints").GetProperty("capture_count").GetInt64());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// The deadlock twin. One capture path rather than two, but the same three answers — and the same refusal
    /// to let "no deadlocks" and "nothing collected" share a sentence.
    /// </summary>
    [Fact]
    public async Task DeadlockTrend_MakesTheSameThreeDistinctions_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live deadlock-trend empty test.");

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

            var never = JsonDocument.Parse(
                await DarlingMcpBlockingTools.GetDeadlockTrend(dataSource, ServerName, 4)).RootElement;
            Assert.Equal("unavailable", never.GetProperty("status").GetString());
            var neverText = never.GetProperty("message").GetString()!;
            Assert.Contains("NOT an all-clear", neverText, StringComparison.Ordinal);
            Assert.Contains("EVER", neverText, StringComparison.Ordinal);

            /* A blocking capture is NOT a deadlock capture: the deadlock probe must not be satisfied by the
               neighbouring collector's runs, or a server with the deadlocks collector switched off would be
               told its empty deadlock trend is an all-clear. */
            await SeedRunAsync(connection, ct, "blocked_process_report", MinutesAgo(10));
            var stillNever = JsonDocument.Parse(
                await DarlingMcpBlockingTools.GetDeadlockTrend(dataSource, ServerName, 4)).RootElement;
            Assert.Equal("unavailable", stillNever.GetProperty("status").GetString());
            Assert.Contains("EVER", stillNever.GetProperty("message").GetString()!, StringComparison.Ordinal);

            await SeedRunAsync(connection, ct, "deadlocks", MinutesAgo(10));
            var clear = JsonDocument.Parse(
                await DarlingMcpBlockingTools.GetDeadlockTrend(dataSource, ServerName, 4)).RootElement;
            Assert.Equal("empty", clear.GetProperty("status").GetString());
            var clearText = clear.GetProperty("message").GetString()!;
            Assert.Contains("genuine all-clear", clearText, StringComparison.Ordinal);
            Assert.DoesNotContain("EVER", clearText, StringComparison.Ordinal);
            Assert.Equal(1, clear.GetProperty("hints").GetProperty("capture_count").GetInt64());

            await SeedDeadlockAsync(connection, ct, MinutesAgo(5));
            var hit = JsonDocument.Parse(
                await DarlingMcpBlockingTools.GetDeadlockTrend(dataSource, ServerName, 4)).RootElement;
            Assert.False(hit.TryGetProperty("status", out _));
            Assert.Equal(1, hit.GetProperty("trend").GetArrayLength());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// The dialect pin, ungated: the probes must read the SAME source the trend does and cover EVERY capture
    /// path it unions. A probe that named one collector, or that read the base table while the trend reads the
    /// view, would report "collected" or "never collected" for rows the trend cannot see — the wrong branch in
    /// exactly the case the probe exists to get right.
    /// </summary>
    [Fact]
    public void CollectorRunProbes_CoverBothPaths_AndReadTheSameSourceTheLogReadDoes()
    {
        foreach (var sql in new[]
                 {
                     DarlingBlockingTrendReader.BlockingCaptureCountsSql,
                     DarlingBlockingTrendReader.HasAnyBlockingCollectorRunSql,
                 })
        {
            Assert.Contains("v_collection_log", sql, StringComparison.Ordinal);
            Assert.Contains("'blocked_process_report'", sql, StringComparison.Ordinal);
            Assert.Contains("'dmv_blocking_snapshot'", sql, StringComparison.Ordinal);

            /* Only a SUCCESS counts as having looked; a PERMISSIONS or ERROR row is a collector that did not
               see the window either, and counting it would manufacture an all-clear out of a failure. */
            Assert.Contains("status = 'SUCCESS'", sql, StringComparison.Ordinal);
        }

        foreach (var sql in new[]
                 {
                     DarlingBlockingTrendReader.DeadlockCaptureCountsSql,
                     DarlingBlockingTrendReader.HasAnyDeadlockCollectorRunSql,
                 })
        {
            Assert.Contains("v_collection_log", sql, StringComparison.Ordinal);
            Assert.Contains("collector_name = 'deadlocks'", sql, StringComparison.Ordinal);
            Assert.Contains("status = 'SUCCESS'", sql, StringComparison.Ordinal);
        }

        /* The existence probes stop at the first row rather than scanning: they run on a path that already
           found nothing, and their only job is to pick which sentence is true. */
        Assert.Contains("LIMIT 1", DarlingBlockingTrendReader.HasAnyBlockingCollectorRunSql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", DarlingBlockingTrendReader.HasAnyDeadlockCollectorRunSql, StringComparison.Ordinal);
    }

    private static DateTime MinutesAgo(int minutes) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-minutes));

    private static DateTime HoursAgo(int hours) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddHours(-hours));

    /// <summary>A collector run that SUCCEEDED and stored nothing — the row that makes an empty trend
    /// interpretable, and the one the edge tables can never produce.</summary>
    private static async Task SeedRunAsync(
        NpgsqlConnection connection, CancellationToken ct, string collector, DateTime collectionTimeUtc) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
            CollectionIdGenerator.Next(), ServerId, ServerName, collector,
            DarlingMcpTestData.Naive(collectionTimeUtc), 100, "SUCCESS", null, 0, 80, 20);

    private static async Task SeedBlockedProcessReportAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime eventTimeUtc) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, database_name,
     blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocked_sql_text, blocking_sql_text,
     blocked_process_report_xml, contentious_object)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(eventTimeUtc), ServerId, ServerName,
            DarlingMcpTestData.Naive(eventTimeUtc), "AppDb", 55, 60, 8000L, "X", "SELECT 1",
            "UPDATE Orders SET Total = 1", "<blocked-process-report/>", "dbo.Orders");

    private static async Task SeedDeadlockAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime deadlockTimeUtc) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO deadlocks
    (deadlock_id, collection_time, server_id, server_name, deadlock_time,
     victim_process_id, victim_sql_text, deadlock_graph_xml)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(deadlockTimeUtc), ServerId, ServerName,
            DarlingMcpTestData.Naive(deadlockTimeUtc), "process1", "UPDATE Orders SET Total = 1",
            "<deadlock/>");

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM blocked_process_reports WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM dmv_blocking_snapshots WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM deadlocks WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
