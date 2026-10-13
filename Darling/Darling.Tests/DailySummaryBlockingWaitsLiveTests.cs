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
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
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
        NpgsqlConnection c, DateTime start, int blocked, int blocker, int reports, string database, bool dmv, CancellationToken ct)
    {
        for (var k = 1; k <= reports; k++)
        {
            var at = start.AddSeconds(5 * k);
            if (dmv)
                await ExecAsync(c, "INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms, blocking_status) VALUES ($1,$2,$3,$4,$2,$5,$6,$7,$8,'suspended')",
                    ct, CollectionIdGenerator.Next(), at, ServerId, ServerName, database, blocked, blocker, (long)(5000 * k));
            else
                await ExecAsync(c, "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid, blocking_status, database_name) VALUES ($1,$2,$3,$4,$2,$5,$6,$7,'suspended',$8)",
                    ct, CollectionIdGenerator.Next(), at, ServerId, ServerName, (long)(5000 * k), blocker, blocked, database);
        }
    }

    /// <summary>Plants nine reports of one wait (database <c>master</c>) and six of another (<c>GP</c>), 15 rows and
    /// 2 waits, on a quiet UTC day (as DMV snapshots when <paramref name="dmv"/>), then reads that day's
    /// <c>blocking_events</c> through the statement, or the master-scoped one with the sibling list at $5 when
    /// <paramref name="scoped"/>.</summary>
    private static async Task<long> DayBlockingEventsAsync(bool dmv, bool scoped)
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
            using var read = new NpgsqlCommand("SELECT blocking_events FROM (" + sql + ") AS calendar", connection);
            read.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
            read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = day });
            read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = day.AddDays(1) });
            read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = EventWindowFloor.For(day) });
            if (scoped)
                read.Parameters.Add(new NpgsqlParameter { Value = new[] { "GP" }, NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text });
            var count = Convert.ToInt64(await read.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
            bodySucceeded = true;
            return count;
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
        Assert.Equal(2, await DayBlockingEventsAsync(dmv: false, scoped: false));
    }

    [Fact]
    public async Task TheDay_CountsDmvSnapshotWaits_NotSnapshots_WhenThereAreNoReports()
    {
        Assert.Equal(2, await DayBlockingEventsAsync(dmv: true, scoped: false));
    }

    [Fact]
    public async Task TheAzureMasterScope_StillSkipsTheSiblingDatabase_AndCountsWaits()
    {
        /* The GP wait belongs to a database monitored as its own target: the master's day keeps the one master wait. */
        Assert.Equal(1, await DayBlockingEventsAsync(dmv: false, scoped: true));
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
