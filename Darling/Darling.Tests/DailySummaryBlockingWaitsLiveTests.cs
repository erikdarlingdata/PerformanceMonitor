/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5644 final review: the daily summary's <c>blocking_events</c> counts blocking WAITS, as the alert value, the fleet
/// tile and the MCP counts do, not blocked-process report rows. SQL Server writes a report for a still-blocked session
/// every threshold seconds, so a nine-report wait and a six-report wait are 2 events, not 15. Gated on DARLING_TEST_PG;
/// read through the product's own statement (<see cref="DailySummarySql.RangeSql"/>), on the blocked-process arm, the
/// always-on DMV snapshot fallback, and the Azure master scope.
/// </summary>
/* #1776 own-store: every fact plants its rows under one dedicated server id and deletes them in cleanup. */
[Collection("live-postgres")]
public sealed class DailySummaryBlockingWaitsLiveTests
{
    private const string ServerName = "darling-daily-blocking-waits";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task ExecAsync(NpgsqlConnection c, string sql, CancellationToken ct, params object[] p)
    {
        using var cmd = new NpgsqlCommand(sql, c);
        foreach (var v in p)
            cmd.Parameters.AddWithValue(v);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /* One wait as SQL Server reports it: a report every 5 s while the session stays blocked, the k-th carrying
       wait_time_ms = k * 5 s. */
    private static async Task PlantWaitAsync(
        NpgsqlConnection c, DateTime start, int blocked, int blocker, int reports, string database, bool dmv, CancellationToken ct, int spacingMs = 5000)
    {
        for (var k = 1; k <= reports; k++)
        {
            var at = start.AddMilliseconds(spacingMs * k);
            if (dmv)
                await ExecAsync(c, "INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms, blocking_status) VALUES ($1,$2,$3,$4,$2,$5,$6,$7,$8,'suspended')",
                    ct, CollectionIdGenerator.Next(), at, ServerId, ServerName, database, blocked, blocker, (long)(spacingMs * k));
            else
                await ExecAsync(c, "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid, blocking_status, database_name) VALUES ($1,$2,$3,$4,$2,$5,$6,$7,'suspended',$8)",
                    ct, CollectionIdGenerator.Next(), at, ServerId, ServerName, (long)(spacingMs * k), blocker, blocked, database);
        }
    }

    /// <summary>Plants nine reports of one wait (database <c>master</c>) and six of another (<c>GP</c>), 15 rows and
    /// 2 waits, on a quiet UTC day (as DMV snapshots when <paramref name="dmv"/>), then reads that day's
    /// <c>blocking_events</c> through the statement, or the master-scoped one with the sibling list at $5 when
    /// <paramref name="scoped"/>.</summary>
    private static async Task<(long Events, long PeakBlockWaitMs)> DayBlockingCountsAsync(bool dmv, bool scoped)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        var bodySucceeded = false;
        try
        {
            await ExecAsync(connection, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, 16, now()::timestamp, now()::timestamp)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE", ct, ServerId, ServerName);

            var day = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-3), DateTimeKind.Unspecified);
            await PlantWaitAsync(connection, day.AddHours(2), blocked: 52, blocker: 51, reports: 9, "master", dmv, ct);
            await PlantWaitAsync(connection, day.AddHours(3), blocked: 53, blocker: 51, reports: 6, "GP", dmv, ct);

            var sql = scoped ? DailySummaryAzureMasterScope.Scope(DailySummarySql.RangeSql) : DailySummarySql.RangeSql;
            using var read = new NpgsqlCommand("SELECT blocking_events, peak_block_wait_ms FROM (" + sql + ") AS calendar", connection);
            read.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
            read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = day });
            read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = day.AddDays(1) });
            read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = EventWindowFloor.For(day) });
            if (scoped)
                read.Parameters.Add(new NpgsqlParameter { Value = new[] { "GP" }, NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text });
            using var reader = await read.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));
            var counts = (Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture),
                Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture));
            bodySucceeded = true;
            return counts;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task TheDay_CountsBlockedProcessWaits_NotReports()
    {
        var (events, peak) = await DayBlockingCountsAsync(dmv: false, scoped: false);
        Assert.Equal(2, events);
        Assert.Equal(45_000, peak);      // the longest of the counted waits
    }

    [Fact]
    public async Task TheDay_CountsDmvSnapshotWaits_NotSnapshots_WhenThereAreNoReports()
    {
        var (events, peak) = await DayBlockingCountsAsync(dmv: true, scoped: false);
        Assert.Equal(2, events);
        Assert.Equal(45_000, peak);
    }

    [Fact]
    public async Task TheAzureMasterScope_StillSkipsTheSiblingDatabase_AndCountsWaits()
    {
        /* The GP wait belongs to a database monitored as its own target: the master's day keeps the one master wait. */
        var (events, peak) = await DayBlockingCountsAsync(dmv: false, scoped: true);
        Assert.Equal(1, events);
        Assert.Equal(45_000, peak);
    }

    private static async Task SeedServerAsync(NpgsqlConnection connection, CancellationToken ct) =>
        await ExecAsync(connection, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, 16, now()::timestamp, now()::timestamp)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE", ct, ServerId, ServerName);

    /* One DMV snapshot: `rows` blocked sessions, each waiting `eachMs`, all stamped with the snapshot's collection_time. */
    private static async Task PlantSnapshotAsync(
        NpgsqlConnection c, DateTime at, int rows, long eachMs, CancellationToken ct)
    {
        for (var i = 0; i < rows; i++)
            await ExecAsync(c, "INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms, blocking_status) VALUES ($1,$2,$3,$4,$2,'db',$5,51,$6,'suspended')",
                ct, CollectionIdGenerator.Next(), at, ServerId, ServerName, 60 + i, eachMs);
    }

    private static async Task<(long Events, long PeakBlockWaitMs, long PeakSnapshotWaitMs)> ReadDayAsync(
        NpgsqlConnection connection, DateTime day, CancellationToken ct, DateTime? rangeStart = null)
    {
        /* rangeStart reads a longer range that ends with `day`, and picks `day`'s row out of it. */
        var start = rangeStart ?? day;
        using var read = new NpgsqlCommand("SELECT blocking_events, peak_block_wait_ms, peak_snapshot_wait_ms FROM (" + DailySummarySql.RangeSql + ") AS calendar WHERE day = $5", connection);
        read.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = start });
        read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = day.AddDays(1) });
        read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = EventWindowFloor.For(start) });
        read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = day });
        using var reader = await read.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return (Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture),
            Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture),
            Convert.ToInt64(reader.GetValue(2), CultureInfo.InvariantCulture));
    }

    /// <summary>#7028: one long wait written as 130 reports (a report every 50 ms, so the longest block is 6.5 s and the wait
    /// arm stays out of it) is ONE blocking event. #5644 banded the day on its 130 report rows (5.4 per hour, Warning);
    /// the band now reads the waits the blocking alerts count, so 1 wait over 24 h is Healthy. Read through the reader the
    /// calendar and the MCP tools use, and through the fleet sweep.</summary>
    [Fact]
    public async Task TheBand_ReadsTheWaitCount_NotTheReportRows()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(DarlingStoreConnection.PinSessionTimeZoneUtc(cs!));
        var bodySucceeded = false;
        try
        {
            await SeedServerAsync(connection, ct);

            var day = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-3), DateTimeKind.Unspecified);
            await ExecAsync(connection, "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES ($1,$2,$3,'wait_stats',$4,'SUCCESS')",
                ct, CollectionIdGenerator.Next(), ServerId, ServerName, day.AddHours(1));
            await PlantWaitAsync(connection, day.AddHours(2), blocked: 52, blocker: 51, reports: 130, "master", dmv: false, ct, spacingMs: 50);

            DarlingHealthReader.ResetRangeCacheForTests();
            var rows = (await DarlingHealthReader.GetDailySummaryRangeAsync(
                postgres, ServerId, day, day.AddDays(1), separatelyMonitored: null, logger: NullLogger.Instance, cancellationToken: ct)).Rows;
            var row = Assert.Single(rows);

            Assert.Equal(1, row.BlockingEvents);
            Assert.Equal(6_500, row.MaxBlockDurationMs);
            Assert.Equal(DailyHealthBand.Healthy, row.HealthBand);
            Assert.Equal(1, row.ToSignals().BlockingEvents);
            Assert.Null(row.ToSignals().BlockingReports);

            var sweep = await FleetSweepEngine.ReadServerSignalsAsync(postgres, ServerId, ServerName, day, day.AddDays(1), ct);
            Assert.Null(sweep.ReadFault);
            Assert.Equal(1, sweep.Signals.BlockingEvents);
            bodySucceeded = true;
        }
        finally
        {
            DarlingHealthReader.ResetRangeCacheForTests();
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>#7028: the XE-or-DMV choice is made per HOUR, as the blocking alerts make it for their rolling hour. A day
    /// holds blocked-process reports in hour 2 (with DMV snapshots of another wait beside them) and DMV snapshots alone in
    /// hour 5. The hour-2 wait counts once, from the reports (the DMV wait beside it is left out). The hour-5 wait counts
    /// too: chosen by the day it counted only the reports and dropped it, so an hour that paged on DMV waits alone stayed
    /// out of its day's band.</summary>
    [Fact]
    public async Task ADmvOnlyHour_CountsInADayThatAlsoHoldsReportsInAnotherHour()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        var bodySucceeded = false;
        try
        {
            await SeedServerAsync(connection, ct);
            var day = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-3), DateTimeKind.Unspecified);
            await PlantWaitAsync(connection, day.AddHours(2), blocked: 52, blocker: 51, reports: 9, "master", dmv: false, ct);
            await PlantWaitAsync(connection, day.AddHours(2).AddMinutes(10), blocked: 53, blocker: 51, reports: 4, "master", dmv: true, ct);
            await PlantWaitAsync(connection, day.AddHours(5), blocked: 54, blocker: 51, reports: 6, "master", dmv: true, ct);

            var (events, peak, _) = await ReadDayAsync(connection, day, ct);

            /* hour 2: the one report wait, hour 5: the one DMV wait. */
            Assert.Equal(2, events);
            Assert.Equal(45_000, peak);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>#7028: the snapshot arm is the Blocking Wait Time alert's own measure after its persistence gate. One
    /// snapshot that totals 60 s sets the value to a third of that, 20 s (a lone snapshot pages only at three times the
    /// bar), so a 60 s bar is not reached. Three adjacent snapshots that each total 60 s set it to 60 s, the bar the alert
    /// would page on. Three snapshots ten minutes apart are not a streak, so they read as lone snapshots.</summary>
    [Fact]
    public async Task TheSnapshotArm_DoesNotTakeALoneBlip_AndTakesASustainedRun()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        var bodySucceeded = false;
        try
        {
            await SeedServerAsync(connection, ct);
            var blipDay = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-3), DateTimeKind.Unspecified);
            var runDay = blipDay.AddDays(1);
            var gapDay = blipDay.AddDays(-1);

            /* a lone snapshot: two blocked sessions of 30 s, 60 s in total */
            await PlantSnapshotAsync(connection, blipDay.AddHours(3), rows: 2, eachMs: 30_000, ct);

            /* three snapshots a minute apart, each 60 s in total */
            for (var i = 0; i < 3; i++)
                await PlantSnapshotAsync(connection, runDay.AddHours(3).AddMinutes(i), rows: 2, eachMs: 30_000, ct);

            /* three snapshots ten minutes apart, each 60 s in total: never adjacent */
            for (var i = 0; i < 3; i++)
                await PlantSnapshotAsync(connection, gapDay.AddHours(3).AddMinutes(10 * i), rows: 2, eachMs: 30_000, ct);

            var blip = await ReadDayAsync(connection, blipDay, ct);
            var run = await ReadDayAsync(connection, runDay, ct);
            var gap = await ReadDayAsync(connection, gapDay, ct);

            Assert.Equal(20_000, blip.PeakSnapshotWaitMs);
            Assert.Equal(60_000, run.PeakSnapshotWaitMs);
            Assert.Equal(20_000, gap.PeakSnapshotWaitMs);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>#7028: the gate runs over the snapshots in time order, not per day. Three snapshots a minute apart that
    /// straddle midnight (23:58, 23:59, 00:00, each 60 s in total) are one streak: the later day, whose range starts at
    /// its own midnight, still sees the two before it and reads 60 s. In a range that holds both days the earlier day
    /// holds only the first two snapshots, so it reads a third of 60 s, 20 s.</summary>
    [Fact]
    public async Task TheSnapshotArm_SeesAStreakAcrossMidnight_OnTheLaterDay()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        var bodySucceeded = false;
        try
        {
            await SeedServerAsync(connection, ct);
            var earlier = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-4), DateTimeKind.Unspecified);
            var later = earlier.AddDays(1);
            await PlantSnapshotAsync(connection, later.AddMinutes(-2), rows: 2, eachMs: 30_000, ct);
            await PlantSnapshotAsync(connection, later.AddMinutes(-1), rows: 2, eachMs: 30_000, ct);
            await PlantSnapshotAsync(connection, later, rows: 2, eachMs: 30_000, ct);

            var laterAlone = await ReadDayAsync(connection, later, ct);
            var laterInRange = await ReadDayAsync(connection, later, ct, rangeStart: earlier);
            var earlierInRange = await ReadDayAsync(connection, earlier, ct, rangeStart: earlier.AddDays(-1));

            Assert.Equal(60_000, laterAlone.PeakSnapshotWaitMs);
            Assert.Equal(60_000, laterInRange.PeakSnapshotWaitMs);
            Assert.Equal(20_000, earlierInRange.PeakSnapshotWaitMs);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM blocked_process_reports WHERE server_id = {ServerId}; " +
            $"DELETE FROM dmv_blocking_snapshots WHERE server_id = {ServerId}; " +
            $"DELETE FROM collection_log WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
