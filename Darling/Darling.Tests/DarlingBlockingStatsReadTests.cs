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
            Assert.Contains("genuinely clear", healthyText, StringComparison.Ordinal);
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
