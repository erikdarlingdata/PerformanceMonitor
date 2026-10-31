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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5644 review of W1: the displayed blocking count is WAITS, but the health band's per-hour count tiers
/// (<c>ServerHealthThresholds.BlockingWarnPerHour</c> 5 / <c>BlockingCriticalPerHour</c> 20) were measured in
/// blocked-process REPORTS per server-hour, so the band reads the report count. One wait that SQL Server reported
/// 22 times in the last ten minutes (short waits, so the 10 s / 60 s wait arms stay out of it) is ONE wait on the
/// card and 22 reports for the band: Critical by count, as it was before W1. Read through the fleet reader and the
/// viewer's per-server summary. Gated on DARLING_TEST_PG.
/// </summary>
/* #1776 own-store: every fact plants its rows under one dedicated server id and deletes them in cleanup. */
[Collection("live-postgres")]
public sealed class BlockingBandReportUnitLiveTests
{
    private const string ServerName = "darling-blocking-band-report-unit";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private const int Reports = 22;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task ExecAsync(NpgsqlConnection c, string sql, CancellationToken ct, params object[] p)
    {
        using var cmd = new NpgsqlCommand(sql, c);
        foreach (var v in p)
            cmd.Parameters.AddWithValue(v);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /* One wait reported 22 times, 100 ms apart: the k-th report carries wait_time_ms = k * 100 and is written at
       start + wait, so every report derives the same wait start (the rule BlockingWaits collapses on). The longest
       wait is 2.2 s: under the 10 s wait arm, so only the count arm can band it. */
    private static async Task PlantAsync(NpgsqlConnection c, bool dmv, CancellationToken ct)
    {
        var start = DateTime.SpecifyKind(DateTime.UtcNow.AddMinutes(-10), DateTimeKind.Unspecified);
        for (var k = 1; k <= Reports; k++)
        {
            var at = start.AddMilliseconds(100 * k);
            if (dmv)
                await ExecAsync(c, "INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms, blocking_status) VALUES ($1,$2,$3,$4,$2,'db',52,51,$5,'suspended')",
                    ct, CollectionIdGenerator.Next(), at, ServerId, ServerName, (long)(100 * k));
            else
                await ExecAsync(c, "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid, blocking_status, database_name) VALUES ($1,$2,$3,$4,$2,$5,51,52,'suspended','db')",
                    ct, CollectionIdGenerator.Next(), at, ServerId, ServerName, (long)(100 * k));
        }
    }

    private static async Task<NpgsqlConnection> OpenSeededAsync(bool dmv, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await ExecAsync(connection, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, 16, now()::timestamp, now()::timestamp)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE", ct, ServerId, ServerName);
        await PlantAsync(connection, dmv, ct);
        return connection;
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TheFleetCard_ShowsOneWait_AndBandsOnTheWaits(bool dmv)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;
        try
        {
            await using var connection = await OpenSeededAsync(dmv, ct);
            await using var postgres = NpgsqlDataSource.Create(ConnectionString!);
            var now = DateTime.UtcNow;

            var result = await DarlingFleetReader.GetFleetOverviewAsync(
                postgres, now.AddHours(-1), now, now, cancellationToken: ct);

            var card = result.Cards.Single(c => c.ServerId == ServerId);
            /* #7028: the band reads the same count the card shows, the blocking WAITS (#5644). One wait that SQL
               Server reported 22 times is ONE wait to the band too, so it is no longer Critical by count. */
            Assert.Equal(1, card.BlockingCount);
            Assert.Equal(1.0, card.BlockingRatePerHour);
            Assert.Equal(HealthSeverity.Healthy, card.BlockingSeverity);
            Assert.True(card.MaxBlockingWaitMs < 10_000, "the wait arm must not be what bands this card");
            /* The re-band the fleet ranking runs reads the same wait count. */
            Assert.Equal(1, card.ToHealthMetrics().BlockingCount);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(ConnectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TheViewerSummary_ShowsOneWait_AndBandsOnTheReports(bool dmv)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;
        try
        {
            await using var connection = await OpenSeededAsync(dmv, ct);
            await using var viewer = new ViewerDataService(ConnectionString!);

            var summary = await viewer.GetServerSummaryAsync(ServerId, ServerName, null, ct);

            Assert.Equal(1, summary.BlockingCount);
            Assert.Equal(Reports, summary.BlockingReportCount);
            Assert.Equal(Reports, summary.BlockingCountForBand);
            Assert.Equal((double)Reports, summary.BlockingRatePerHour);
            Assert.Equal(HealthSeverity.Critical, summary.BlockingSeverity);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(ConnectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>A card or summary built without a report count (the pre-W1 shape) bands on its own count.</summary>
    [Fact]
    public void ACardBuiltWithoutAReportCount_BandsOnItsCount()
    {
        var summary = new ServerSummaryItem { BlockingCount = 7, BlockingWindow = TimeSpan.FromHours(1) };
        Assert.Equal(7, summary.BlockingCountForBand);

        var withReports = new ServerSummaryItem { BlockingCount = 1, BlockingReportCount = 30, BlockingWindow = TimeSpan.FromHours(1) };
        Assert.Equal(30, withReports.BlockingCountForBand);
        Assert.Equal(HealthSeverity.Critical, withReports.BlockingSeverity);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM blocked_process_reports WHERE server_id = {ServerId}; " +
            $"DELETE FROM dmv_blocking_snapshots WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
