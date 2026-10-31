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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7028 part A, through <c>GetFleetOverviewAsync</c>: the fleet card bands on the blocking WAITS the blocking
/// alerts count, and reads the <c>Blocking Wait Time</c> alert's own measure for the snapshot arm. Gated on
/// DARLING_TEST_PG.
/// </summary>
/* #1776 own-store: every fact plants its rows under one dedicated server id and deletes them in cleanup. */
[Collection("live-postgres")]
public sealed class BlockingBandWaitsAndSnapshotsLiveTests
{
    private const string ServerName = "darling-blocking-band-waits-snapshots";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    /* Each snapshot row waits 8 s: under the 10 s wait arm, so only the count and snapshot arms can band it. */
    private const long RowWaitMs = 8000;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task ExecAsync(NpgsqlConnection c, string sql, CancellationToken ct, params object[] p)
    {
        using var cmd = new NpgsqlCommand(sql, c);
        foreach (var v in p)
            cmd.Parameters.AddWithValue(v);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task<NpgsqlConnection> OpenAsync(CancellationToken ct)
    {
        var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await ExecAsync(connection, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, 16, now()::timestamp, now()::timestamp)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE", ct, ServerId, ServerName);
        return connection;
    }

    /* One report is one wait: each carries its own blocked spid, so no two merge. The report is written at
       `at` with the wait time given, so its derived wait start is `at` minus the wait. */
    private static async Task PlantXeAsync(NpgsqlConnection c, DateTime at, int blockedSpid, long waitMs, CancellationToken ct) =>
        await ExecAsync(c, "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid, blocking_status, database_name) VALUES ($1,$2,$3,$4,$2,$5,50,$6,'suspended','db')",
            ct, CollectionIdGenerator.Next(), at, ServerId, ServerName, waitMs, blockedSpid);

    private static async Task PlantDmvAsync(NpgsqlConnection c, DateTime at, int blockedSpid, long waitMs, CancellationToken ct) =>
        await ExecAsync(c, "INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms, blocking_status) VALUES ($1,$2,$3,$4,$2,'db',$6,50,$5,'suspended')",
            ct, CollectionIdGenerator.Next(), at, ServerId, ServerName, waitMs, blockedSpid);

    private static DateTime Unspecified(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

    /// <summary>
    /// A 24 hour window with two busy hours: one holds blocked-process reports (XE) and DMV snapshot waits, the
    /// other holds DMV snapshot waits only. Each hour counts the source the alerts would choose for it, so the
    /// window count is both hours' waits, and the card shows the count it bands.
    /// </summary>
    [Fact]
    public async Task ATwentyFourHourWindow_ChoosesTheSourcePerHour_AndShowsTheCountItBands()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;
        try
        {
            await using var connection = await OpenAsync(ct);
            var now = DateTime.UtcNow;
            DateTime At(int minutesAgo) => Unspecified(now.AddMinutes(-minutesAgo));

            /* Hour A, 30 minutes back: two XE waits (3000 ms and 2000 ms) and one DMV wait of 9000 ms that the XE
               hour hides. */
            await PlantXeAsync(connection, At(30), 101, 3000, ct);
            await PlantXeAsync(connection, At(31), 102, 2000, ct);
            await PlantDmvAsync(connection, At(32), 103, 9000, ct);
            /* Hour B, 90 minutes back: three DMV waits (4000, 1500, 1200 ms) and no XE report. */
            await PlantDmvAsync(connection, At(90), 111, 4000, ct);
            await PlantDmvAsync(connection, At(91), 112, 1500, ct);
            await PlantDmvAsync(connection, At(92), 113, 1200, ct);

            await using var postgres = NpgsqlDataSource.Create(ConnectionString!);
            var result = await DarlingFleetReader.GetFleetOverviewAsync(
                postgres, now.AddHours(-24), now, now, cancellationToken: ct);

            var card = result.Cards.Single(c => c.ServerId == ServerId);
            /* 2 XE waits in hour A plus 3 DMV waits in hour B. The old whole window rule (any XE report in the
               window hides every DMV wait) would give 2. */
            Assert.Equal(5, card.BlockingCount);
            Assert.Equal(5 / 24.0, card.BlockingRatePerHour);
            /* The banded count is the shown count. */
            Assert.Equal(card.BlockingCount, card.ToHealthMetrics().BlockingCount);
            /* The longest wait is the longest of the CHOSEN waits: hour B's 4000 ms, not the hidden 9000 ms. */
            Assert.Equal(4000, card.MaxBlockingWaitMs);
            Assert.Equal(HealthSeverity.Healthy, card.BlockingSeverity);
            /* The snapshot arm reads the DMV snapshots whatever the count chose. Four snapshots: the 9000 ms one
               stands alone (a lone snapshot counts at a third of its total, 3000 ms), and hour B's three adjacent
               ones (1200, 1500, 4000) peak at the larger of their smallest (1200) and 4000 / 3 (1333.3). */
            Assert.Equal(3000, card.MaxSnapshotWaitMs);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(ConnectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// The snapshot arm through the fleet card. The card carries the arm's value (the largest gated DMV snapshot
    /// total) and the band compares it with a snapshot tier. A snapshot is a few rows of 8 s each, and the bar here
    /// is 40 s (five rows). Three adjacent snapshots at the bar set the value to the bar. One snapshot under the
    /// gate's multiple (3 times the bar) counts at a third of its total and does not reach the bar, one at the
    /// multiple does. Snapshots further apart than the episode gap are not adjacent.
    /// </summary>
    [Theory]
    // Three adjacent snapshots (90 s apart, the shipped cadence) of 40 s: the smallest of the three is 40 s.
    [InlineData(1, 90, "5,5,5", 40_000L, true)]
    [InlineData(24, 90, "5,5,5", 40_000L, true)]
    // One snapshot of 40 s: under 3 times the bar, so a third of it (13,333 ms) is all that counts.
    [InlineData(1, 90, "5", 13_333L, false)]
    // One snapshot of 120 s is 3 times the bar and pages alone.
    [InlineData(1, 90, "15", 40_000L, true)]
    [InlineData(24, 90, "15", 40_000L, true)]
    // Three adjacent snapshots with a dip to 8 s: the smallest of the three is under the bar, so only a third of 40 s counts.
    [InlineData(1, 90, "5,1,5", 13_333L, false)]
    // Three snapshots ten minutes apart are three episodes, each a lone snapshot.
    [InlineData(1, 600, "5,5,5", 13_333L, false)]
    public async Task TheSnapshotArm_ReadsTheAlertsGatedMeasure_ThroughTheFleetCard(
        int windowHours, int spacingSeconds, string rowsPerSnapshot, long expectedPeakMs, bool expectedCritical)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;
        try
        {
            await using var connection = await OpenAsync(ct);
            var now = DateTime.UtcNow;
            var rows = rowsPerSnapshot.Split(',').Select(int.Parse).ToList();
            /* The last snapshot lands 10 minutes back, the earlier ones before it. */
            for (var i = 0; i < rows.Count; i++)
            {
                var at = Unspecified(now.AddMinutes(-10).AddSeconds(-spacingSeconds * (rows.Count - 1 - i)));
                for (var r = 0; r < rows[i]; r++)
                    await PlantDmvAsync(connection, at, 200 + r, RowWaitMs, ct);
            }

            await using var postgres = NpgsqlDataSource.Create(ConnectionString!);
            var result = await DarlingFleetReader.GetFleetOverviewAsync(
                postgres, now.AddHours(-windowHours), now, now, cancellationToken: ct);

            var card = result.Cards.Single(c => c.ServerId == ServerId);
            /* Every row is its own wait (a repeated spid in a later snapshot starts 90 s or more later). */
            Assert.Equal(rows.Sum(), card.BlockingCount);
            Assert.Equal(RowWaitMs, card.MaxBlockingWaitMs);
            Assert.Equal(expectedPeakMs, card.MaxSnapshotWaitMs);

            /* The snapshot tiers default to 0 (off), so the arm leaves the card's own band alone: at most the
               count arm speaks (15 waits an hour is a Warning, never Critical). */
            Assert.NotEqual(HealthSeverity.Critical, card.BlockingSeverity);

            /* With the critical snapshot tier at 40 s, and the count tiers out of the way, the band reads the card's own value. */
            var tiers = new BlockingBandThresholds(
                1000, 2000,
                ServerHealthThresholds.BlockingWarnWaitSeconds, ServerHealthThresholds.BlockingCriticalWaitSeconds,
                CriticalSnapshotWaitSecondsRaw: 40);
            var metrics = card.ToHealthMetrics();
            var severity = ServerHealthClassifier.BlockingSeverity(
                metrics.BlockingCount, metrics.MaxBlockedSeconds, metrics.BlockingWindow, tiers, metrics.PeakSnapshotWaitSeconds);
            Assert.Equal(expectedCritical ? HealthSeverity.Critical : HealthSeverity.Healthy, severity);

            /* The same through the store: set the critical snapshot tier to 40 s on the settings row and the
               card bands itself. The row is restored afterwards. */
            await ExecAsync(connection, "INSERT INTO config_alert_settings (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);
            var saved = await ReadSnapshotTierAsync(connection, ct);
            try
            {
                await ExecAsync(connection, "UPDATE config_alert_settings SET blocking_critical_snapshot_wait_seconds = 40 WHERE id = 1", ct);
                var banded = await DarlingFleetReader.GetFleetOverviewAsync(
                    postgres, now.AddHours(-windowHours), now, now, cancellationToken: ct);
                var bandedCard = banded.Cards.Single(c => c.ServerId == ServerId);
                Assert.Equal(expectedPeakMs, bandedCard.MaxSnapshotWaitMs);
                if (expectedCritical)
                    Assert.Equal(HealthSeverity.Critical, bandedCard.BlockingSeverity);
                else
                    Assert.NotEqual(HealthSeverity.Critical, bandedCard.BlockingSeverity);
            }
            finally
            {
                await ExecAsync(connection, "UPDATE config_alert_settings SET blocking_critical_snapshot_wait_seconds = $1 WHERE id = 1", ct, saved);
            }
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(ConnectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task<double> ReadSnapshotTierAsync(NpgsqlConnection c, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand("SELECT blocking_critical_snapshot_wait_seconds FROM config_alert_settings WHERE id = 1", c);
        var value = await cmd.ExecuteScalarAsync(ct);
        Assert.NotNull(value);
        return Convert.ToDouble(value);
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
