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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7573, review round 3. Three behaviours of the Monitor statements switch that the first live class does not cover:
/// the Viewer's Latest Snapshot anchors on the newest capture that has rows the grid shows (so a newest capture holding only the monitor's statements does
/// not empty the read); a monitor session that heads a blocking chain stays on the default page, so its victim's blocker is named; and an empty answer
/// under a filter says the monitor's statements were left out. Skips without <c>DARLING_TEST_PG</c>, like its siblings.
/// </summary>
[Collection("live-postgres")]
public sealed class ActiveQueryMonitorStatementsReviewLiveTests
{
    private const string ServerName = "darling-active-queries-monitor-review-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live active-query monitor-statement review test.";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task InsertAsync(NpgsqlConnection connection, DateTime at, int session, string? app, int blockedBy, string text, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, elapsed_time_formatted, query_text, status, blocking_session_id, wait_type, wait_time_ms, cpu_time_ms, total_elapsed_time_ms, reads, writes, logical_reads, granted_query_memory_gb, transaction_isolation_level, dop, parallel_worker_count, login_name, host_name, program_name, open_transaction_count, request_id)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, session, "AppDb", "00 00:00:05.000",
            text, "running", blockedBy, null, 0L, 1000L, 1000L, 100L, 0L, 2000L, 0.5m, "Read Committed", 1, 0, "sa", "APP01", (object?)app ?? DBNull.Value, 1, 0);

    private static async Task<NpgsqlConnection> OpenAndResetAsync(string cs, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        return connection;
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM collect.query_snapshots WHERE server_id = {ServerId}; DELETE FROM collect.servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    /* An older capture with application rows and a newer one holding only the monitor's statements: the store really does look like this on a
       quiet server, now that the collector keeps the monitor's rows. The old anchor took the newest capture over every row and the program filter then
       emptied it, so the Latest Snapshot button said nothing was stored. */
    [Fact]
    public async Task LatestSnapshot_AnchorsOnTheNewestCaptureThatHasRowsTheGridShows()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;
        try
        {
            using var connection = await OpenAndResetAsync(cs!, ct);
            var end = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var older = end.AddMinutes(-10);
            var newer = end.AddMinutes(-2);
            await InsertAsync(connection, older, 61, "Orders.Api", 0, "SELECT app", ct);
            await InsertAsync(connection, newer, 62, "PerformanceMonitorDarling", 0, "SELECT monitor", ct);

            await using var viewer = new ViewerDataService(cs!);
            var (batchTime, batch) = await viewer.GetLatestQuerySnapshotBatchAsync(ServerId, cancellationToken: ct);
            Assert.Equal(DarlingMcpTestData.Naive(older), batchTime);
            Assert.Equal(61, Assert.Single(batch).SessionId);

            var (allTime, all) = await viewer.GetLatestQuerySnapshotBatchAsync(ServerId, cancellationToken: ct, includeMonitor: true);
            Assert.Equal(DarlingMcpTestData.Naive(newer), allTime);
            Assert.Equal(62, Assert.Single(all).SessionId);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /* A monitor query that holds a lock is the thing someone is looking for when an application session is stuck behind it. One capture: the monitor's
       session 50 heads the chain, the application's session 60 waits on it, and the monitor's session 70 is idle noise that blocks nobody. */
    [Fact]
    public async Task AMonitorSessionThatHeadsABlockingChain_StaysOnTheDefaultPage_AndItsVictimNamesIt()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            using var connection = await OpenAndResetAsync(cs!, ct);
            var end = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var at = end.AddMinutes(-2);
            await InsertAsync(connection, at, 50, "PerformanceMonitorDarling", 0, "SELECT head", ct);
            await InsertAsync(connection, at, 60, "Orders.Api", 50, "SELECT victim", ct);
            await InsertAsync(connection, at, 70, "PerformanceMonitorLite", 0, "SELECT noise", ct);

            var page = await DarlingSessionReader.GetActiveQueriesAsync(postgres, ServerId, end.AddHours(-1), end, 100, DatabaseFilter.All, cancellationToken: ct);
            Assert.Equal(new[] { 50, 60 }, page.Rows.Select(r => r.SessionId).OrderBy(s => s).ToArray());

            using var tool = JsonDocument.Parse(await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, 24, cancellationToken: ct));
            var queries = tool.RootElement.GetProperty("queries").EnumerateArray().ToList();
            var victim = queries.Single(q => q.GetProperty("session_id").GetInt32() == 60);
            Assert.Equal(JsonValueKind.Null, victim.GetProperty("blocker_not_shown").ValueKind);
            Assert.True(queries.Single(q => q.GetProperty("session_id").GetInt32() == 50).GetProperty("is_head_blocker").GetBoolean());

            /* The strip counts the same page. */
            var list = await DarlingSessionReader.GetActiveQuerySnapshotsAsync(postgres, ServerId, end.AddHours(-1), end, 100, DatabaseFilter.All, cancellationToken: ct);
            Assert.Equal(2, Assert.Single(list.Snapshots).RowCount);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /* A filter that matches nothing is not a verdict on the monitor's rows: when the switch is off the answer says they were left out too. */
    [Fact]
    public async Task AnEmptyAnswerUnderAFilter_SaysTheMonitorsStatementsWereLeftOut_WhenTheSwitchIsOff()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            using var connection = await OpenAndResetAsync(cs!, ct);
            var end = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var at = end.AddMinutes(-2);
            await InsertAsync(connection, at, 50, "PerformanceMonitorDarling", 0, "SELECT head", ct);
            const string LeftOut = "monitor's own statements are left out";

            static string Message(string json) => JsonDocument.Parse(json).RootElement.GetProperty("message").GetString()!;

            var rows = await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, 24, database_name: "NoSuchDb", cancellationToken: ct);
            Assert.Contains(LeftOut, Message(rows), StringComparison.Ordinal);
            var snapshots = await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, 24, database_name: "NoSuchDb", snapshots_only: true, cancellationToken: ct);
            Assert.Contains(LeftOut, Message(snapshots), StringComparison.Ordinal);
            var one = await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, 24, database_name: "NoSuchDb", collection_time: at.ToString("o"), cancellationToken: ct);
            Assert.Contains(LeftOut, Message(one), StringComparison.Ordinal);
            var waits = await DarlingMcpSessionTools.GetWaitingTasks(postgres, ServerName, 24, database_name: "NoSuchDb", cancellationToken: ct);
            Assert.Contains(LeftOut, Message(waits), StringComparison.Ordinal);

            /* With the switch on there is nothing left out to say. */
            var asked = await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, 24, database_name: "NoSuchDb", include_monitor: true, cancellationToken: ct);
            Assert.DoesNotContain(LeftOut, Message(asked), StringComparison.Ordinal);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }
}
