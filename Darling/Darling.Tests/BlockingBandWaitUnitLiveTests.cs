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
/// #7028: the blocking band counts WAITS, the unit the blocking alerts count, not report rows. One wait that SQL Server
/// reported 22 times in the last ten minutes (short waits, so the 10 s / 60 s wait arms stay out of it) is ONE wait on
/// the card and ONE for the band: Healthy by count (#5644 banded it on its 22 report rows, Critical). Six separate
/// waits are 6 per hour for the band, Warning at the 5 per hour bar. Read through the fleet reader and the viewer's
/// per-server summary. Gated on DARLING_TEST_PG.
/// </summary>
/* #1776 own-store: every fact plants its rows under one dedicated server id and deletes them in cleanup. */
[Collection("live-postgres")]
public sealed class BlockingBandWaitUnitLiveTests
{
    private const string ServerName = "darling-blocking-band-wait-unit";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task ExecAsync(NpgsqlConnection c, string sql, CancellationToken ct, params object[] p)
    {
        using var cmd = new NpgsqlCommand(sql, c);
        foreach (var v in p)
            cmd.Parameters.AddWithValue(v);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /* `waits` waits of three reports each, 100 ms apart: the k-th report of a wait carries wait_time_ms = k * 100 and is
       written at start + wait, so every report of a wait derives the same wait start (the rule BlockingWaits collapses
       on). Each wait is its own blocked session, a minute apart, ten minutes ago or later. The longest wait is 300 ms:
       under the 10 s wait arm, so only the count arm can band it. With waits = 1 the one wait has 22 reports. */
    private static async Task PlantAsync(NpgsqlConnection c, bool dmv, int waits, CancellationToken ct)
    {
        var perWait = waits == 1 ? 22 : 3;
        for (var w = 0; w < waits; w++)
        {
            var start = DateTime.SpecifyKind(DateTime.UtcNow.AddMinutes(-10 + w), DateTimeKind.Unspecified);
            for (var k = 1; k <= perWait; k++)
            {
                var at = start.AddMilliseconds(100 * k);
                if (dmv)
                    await ExecAsync(c, "INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms, blocking_status) VALUES ($1,$2,$3,$4,$2,'db',$6,51,$5,'suspended')",
                        ct, CollectionIdGenerator.Next(), at, ServerId, ServerName, (long)(100 * k), 60 + w);
                else
                    await ExecAsync(c, "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid, blocking_status, database_name) VALUES ($1,$2,$3,$4,$2,$5,51,$6,'suspended','db')",
                        ct, CollectionIdGenerator.Next(), at, ServerId, ServerName, (long)(100 * k), 60 + w);
            }
        }
    }

    private static async Task<NpgsqlConnection> OpenSeededAsync(bool dmv, int waits, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await ExecAsync(connection, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, 16, now()::timestamp, now()::timestamp)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE", ct, ServerId, ServerName);
        await PlantAsync(connection, dmv, waits, ct);
        return connection;
    }

    [Theory]
    [InlineData(false, 1, HealthSeverity.Healthy)]
    [InlineData(true, 1, HealthSeverity.Healthy)]
    [InlineData(false, 6, HealthSeverity.Warning)]
    [InlineData(true, 6, HealthSeverity.Warning)]
    public async Task TheFleetCard_BandsOnWaits_NotOnReports(bool dmv, int waits, HealthSeverity expected)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;
        try
        {
            await using var connection = await OpenSeededAsync(dmv, waits, ct);
            await using var postgres = NpgsqlDataSource.Create(ConnectionString!);
            var now = DateTime.UtcNow;

            var result = await DarlingFleetReader.GetFleetOverviewAsync(
                postgres, now.AddHours(-1), now, now, cancellationToken: ct);

            var card = result.Cards.Single(c => c.ServerId == ServerId);
            Assert.Equal(waits, card.BlockingCount);                    // displayed: the waits
            Assert.Equal((double)waits, card.BlockingRatePerHour);      // banded: the same waits per hour
            Assert.Equal(expected, card.BlockingSeverity);
            Assert.True(card.MaxBlockingWaitMs < 10_000, "the wait arm must not be what bands this card");
            Assert.Equal(waits, card.ToHealthMetrics().BlockingCount);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(ConnectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Theory]
    [InlineData(false, 1, HealthSeverity.Healthy)]
    [InlineData(true, 1, HealthSeverity.Healthy)]
    [InlineData(false, 6, HealthSeverity.Warning)]
    [InlineData(true, 6, HealthSeverity.Warning)]
    public async Task TheViewerSummary_BandsOnWaits_NotOnReports(bool dmv, int waits, HealthSeverity expected)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;
        try
        {
            await using var connection = await OpenSeededAsync(dmv, waits, ct);
            await using var viewer = new ViewerDataService(ConnectionString!);

            var summary = await viewer.GetServerSummaryAsync(ServerId, ServerName, null, ct);

            Assert.Equal(waits, summary.BlockingCount);
            Assert.Equal(waits, summary.BlockingCountForBand);
            Assert.Equal((double)waits, summary.BlockingRatePerHour);
            Assert.Equal(expected, summary.BlockingSeverity);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(ConnectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>The count arm bands the wait count the card shows: seven an hour are Warning (the 5 per hour bar), one is not.</summary>
    [Fact]
    public void ACardBands_OnItsWaitCount()
    {
        var summary = new ServerSummaryItem { BlockingCount = 7, BlockingWindow = TimeSpan.FromHours(1) };
        Assert.Equal(7, summary.BlockingCountForBand);
        Assert.Equal(HealthSeverity.Warning, summary.BlockingSeverity);

        var calm = new ServerSummaryItem { BlockingCount = 1, BlockingWindow = TimeSpan.FromHours(1) };
        Assert.Equal(HealthSeverity.Healthy, calm.BlockingSeverity);
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
