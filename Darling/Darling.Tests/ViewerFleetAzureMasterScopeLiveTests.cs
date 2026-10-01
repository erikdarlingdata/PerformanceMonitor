/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// An Azure SQL Database master registration also sees the events of the databases on its logical server
/// that are monitored as their own targets. The Viewer's per-server summary skips those events, and the fleet
/// totals count each event once. Gated on DARLING_TEST_PG.
/// </summary>
/* #1776 own-store: this fact mints its own scratch database through ScratchPostgres and never touches another
   fact's rows. */
[Collection("live-postgres")]
public sealed class ViewerFleetAzureMasterScopeLiveTests
{
    private const int MasterId = 8101;
    private const int GpId = 8102;
    private const int HsId = 8103;
    private const int PlainId = 8104;
    private const int LoneMasterId = 8105;

    private static string Graph(string db) =>
        "<deadlock><process-list><process id=\"p0\" currentdbname=\"" + db + "\" /></process-list></deadlock>";

    [Fact]
    public async Task MasterSummarySkipsSeparatelyMonitoredEvents_AndFleetTotalsCountEachEventOnce()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live Viewer scope pin.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            await RegisterAsync(connection, MasterId, "host-a.example", "master", 5, ct);
            await RegisterAsync(connection, GpId, "host-a.example", "GP", 5, ct);
            await RegisterAsync(connection, HsId, "host-a.example", "HS", 5, ct);
            await RegisterAsync(connection, PlainId, "host-b.example", null, 3, ct);
            await RegisterAsync(connection, LoneMasterId, "host-c.example", "master", 5, ct);

            var at = DateTime.UtcNow.AddMinutes(-10);

            /* The master sees its own server's events: two in GP, one in HS (all monitored on their own), two
               in a database nobody else monitors, one with no database. Scoped, 3 remain. */
            foreach (var db in new[] { "GP", "GP", "HS", "OTHER", "OTHER", null })
            {
                await BprAsync(connection, MasterId, db, at, ct);
            }

            /* The databases' own targets record their own events. */
            await BprAsync(connection, GpId, "GP", at, ct);
            await BprAsync(connection, GpId, "GP", at, ct);
            await BprAsync(connection, HsId, "HS", at, ct);

            /* Master deadlocks: one wholly in GP (skipped), one outside (counts). GP records its own. */
            await DeadlockAsync(connection, MasterId, "GP", at, ct);
            await DeadlockAsync(connection, MasterId, "OTHER", at, ct);
            await DeadlockAsync(connection, GpId, "GP", at, ct);

            /* Unchanged: a non-Azure server and an Azure master with no monitored sibling. */
            await BprAsync(connection, PlainId, null, at, ct);
            await BprAsync(connection, PlainId, null, at, ct);
            await DeadlockAsync(connection, PlainId, "x", at, ct);
            await BprAsync(connection, LoneMasterId, "GP", at, ct);
            await BprAsync(connection, LoneMasterId, null, at, ct);
            await DeadlockAsync(connection, LoneMasterId, "GP", at, ct);

            await using var viewer = new ViewerDataService(scratch.ConnectionString);

            var master = await viewer.GetServerSummaryAsync(MasterId, "master", null, ct);
            Assert.Equal(3, master.BlockingCount);
            Assert.Equal(1, master.DeadlockCount);

            var plain = await viewer.GetServerSummaryAsync(PlainId, "plain", null, ct);
            Assert.Equal(2, plain.BlockingCount);
            Assert.Equal(1, plain.DeadlockCount);

            var lone = await viewer.GetServerSummaryAsync(LoneMasterId, "lone", null, ct);
            Assert.Equal(2, lone.BlockingCount);
            Assert.Equal(1, lone.DeadlockCount);

            /* Each event once: master 3 + GP 2 + HS 1 + plain 2 + lone 2 blocking; master 1 + GP 1 + plain 1 + lone 1 deadlocks. */
            var now = DateTime.UtcNow;
            var totals = await viewer.GetFleetTotalsAsync(now.AddHours(-1), now, ct);
            Assert.Equal(10, totals.TotalBlockingEvents);
            Assert.Equal(4, totals.TotalDeadlocks);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    private static async Task RegisterAsync(NpgsqlConnection connection, int id, string host, string? database, int edition, System.Threading.CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_engine_edition, sql_major_version, created_date, modified_date) VALUES ($1, $2, $2, TRUE, $3, 16, now()::timestamp, now()::timestamp)",
            id, host + "/" + (database ?? "-"), edition);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO config.config_monitored_servers (server_id, name, host, database, is_enabled) VALUES ($1, $2, $3, $4, TRUE)",
            id, host + "/" + (database ?? "-"), host, (object?)database ?? DBNull.Value);
    }

    private static Task BprAsync(NpgsqlConnection connection, int serverId, string? db, DateTime at, System.Threading.CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, database_name) VALUES ($1,$2,$3,'s',$2,5000,$4)",
            CollectionIdGenerator.Next(), at, serverId, (object?)db ?? DBNull.Value);

    private static Task DeadlockAsync(NpgsqlConnection connection, int serverId, string db, DateTime at, System.Threading.CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml, database_name) VALUES ($1,$2,$3,'s',$2,$4,$5)",
            CollectionIdGenerator.Next(), at, serverId, Graph(db), db);
}
