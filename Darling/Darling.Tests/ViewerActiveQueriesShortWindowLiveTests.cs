/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Viewer's Active Queries screen against the store. The grid read keeps a head blocker whose statement is a <c>WAITFOR</c> when a row of
/// the same capture names it as its blocker, and still drops an idle <c>WAITFOR</c> shell; the strip over a one-hour window buckets by the
/// minute, so its line reaches the window's end instead of stopping at the last whole hour. Skips without <c>DARLING_TEST_PG</c>, like its siblings.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerActiveQueriesShortWindowLiveTests
{
    private const string ServerName = "darling-active-queries-short-window-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live Active Queries short-window test.";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task InsertAsync(NpgsqlConnection connection, DateTime at, int session, int blockedBy, string text, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, elapsed_time_formatted, query_text, status, blocking_session_id, wait_type, wait_time_ms, cpu_time_ms, total_elapsed_time_ms, reads, writes, logical_reads, granted_query_memory_gb, transaction_isolation_level, dop, parallel_worker_count, login_name, host_name, program_name, open_transaction_count, request_id)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, session, "AppDb", "00 00:05:00.000",
            text, "suspended", blockedBy, blockedBy > 0 ? "LCK_M_S" : null, 0L, 10L, 300000L, 100L, 0L, 2000L, 0.5m, "Read Committed", 1, 0, "sa", "APP01", "Orders.Api", 1, 0);

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

    private static DateTime MinuteFloor(DateTime at) => new(at.Ticks - at.Ticks % TimeSpan.TicksPerMinute, at.Kind);

    /* One capture: session 60 sits in WAITFOR holding the lock 61 waits on, 62 is a WAITFOR shell nothing waits on, 63 is a plain reader. An older
       capture holds 60's WAITFOR with no victim, so a head is judged in its own capture, not across the window. */
    [Fact]
    public async Task TheGridRead_KeepsTheWaitforHeadBlocker_AndDropsTheIdleShell()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;
        try
        {
            using var connection = await OpenAndResetAsync(cs!, ct);
            var end = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var at = end.AddMinutes(-5);
            await InsertAsync(connection, at, 60, 0, "WAITFOR DELAY '00:10:00'", ct);
            await InsertAsync(connection, at, 61, 60, "SELECT [v] FROM [dbo].[walk_rows] WHERE [id]=@1", ct);
            await InsertAsync(connection, at, 62, 0, "WAITFOR DELAY '00:10:00'", ct);
            await InsertAsync(connection, at, 63, 0, "SELECT 1", ct);
            await InsertAsync(connection, at.AddMinutes(-1), 60, 0, "WAITFOR DELAY '00:10:00'", ct);

            await using var viewer = new ViewerDataService(cs!);
            var (total, rows) = await viewer.GetLatestQuerySnapshotsAsync(ServerId, end.AddHours(-1), end, cancellationToken: ct);

            Assert.Equal(new[] { 60, 61, 63 }, rows.Where(r => r.CollectionTime == DarlingMcpTestData.Naive(at)).Select(r => r.SessionId).OrderBy(s => s).ToArray());
            Assert.Equal(3, total);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /* The Latest Snapshot button reads the newest capture alone, and feeds the same grid: the head stays and the idle shell is dropped there too. */
    [Fact]
    public async Task TheLatestSnapshotRead_KeepsTheWaitforHeadBlocker_AndDropsTheIdleShell()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;
        try
        {
            using var connection = await OpenAndResetAsync(cs!, ct);
            var end = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var at = end.AddMinutes(-5);
            await InsertAsync(connection, at, 60, 0, "WAITFOR DELAY '00:10:00'", ct);
            await InsertAsync(connection, at, 61, 60, "SELECT [v] FROM [dbo].[walk_rows] WHERE [id]=@1", ct);
            await InsertAsync(connection, at, 62, 0, "WAITFOR DELAY '00:10:00'", ct);
            await InsertAsync(connection, at, 63, 0, "SELECT 1", ct);
            await InsertAsync(connection, at.AddMinutes(-1), 60, 0, "WAITFOR DELAY '00:10:00'", ct);

            await using var viewer = new ViewerDataService(cs!);
            var (batchTime, rows) = await viewer.GetLatestQuerySnapshotBatchAsync(ServerId, cancellationToken: ct);

            Assert.Equal(DarlingMcpTestData.Naive(at), batchTime);
            Assert.Equal(new[] { 60, 61, 63 }, rows.Select(r => r.SessionId).OrderBy(s => s).ToArray());
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task TheStrip_BucketsAOneHourWindowByTheMinute()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;
        try
        {
            using var connection = await OpenAndResetAsync(cs!, ct);
            var end = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var newer = MinuteFloor(end).AddMinutes(-3).AddSeconds(7);
            var older = MinuteFloor(end).AddMinutes(-6).AddSeconds(7);
            await InsertAsync(connection, older, 51, 0, "SELECT 1", ct);
            await InsertAsync(connection, newer, 52, 0, "SELECT 2", ct);
            await InsertAsync(connection, newer, 53, 0, "SELECT 3", ct);

            await using var viewer = new ViewerDataService(cs!);
            var buckets = await viewer.GetActiveQuerySlicerDataAsync(ServerId, end.AddHours(-1), end, cancellationToken: ct);

            Assert.Equal(new[] { DarlingMcpTestData.Naive(MinuteFloor(older)), DarlingMcpTestData.Naive(MinuteFloor(newer)) }, buckets.Select(b => b.BucketTime).ToArray());
            Assert.Equal(new long[] { 1, 2 }, buckets.Select(b => b.SessionCount).ToArray());
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }
}
