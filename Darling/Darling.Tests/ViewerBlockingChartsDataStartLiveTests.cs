/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Blocking tab's chart floors against a real store (#4966): the viewer's own data-start reads for blocked process reports,
/// deadlocks and wait_stats, over a server added 2 days ago whose first event came a day later.
/// </summary>
public sealed class ViewerBlockingChartsDataStartLiveTests
{
    private const int ServerId = -496701;
    private const string ServerName = "blocking-charts-new";

    [Fact]
    public async Task TheFloors_NameTheDayTheCollectorsStarted_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string.");

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var now = DateTime.UtcNow;
        var end = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Utc);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await Exec(connection, "UPDATE collect.servers SET created_date = $1 WHERE server_id = $2", ct, Naive(end.AddDays(-2)), ServerId);
            long id = 1;
            foreach (var collector in new[] { "blocked_process_report", "deadlocks", "wait_stats" })
            {
                await Exec(connection,
                    "INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected) " +
                    "SELECT row_number() OVER () + $5, $1, $2, $3, t, 12, 'SUCCESS', 0 FROM generate_series($4::timestamp, $6::timestamp, interval '30 minutes') AS t",
                    ct, ServerId, ServerName, collector, Naive(end.AddDays(-2)), id * 100000L, Naive(end));
                id++;
            }

            await Exec(connection,
                "INSERT INTO collect.blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms) " +
                "VALUES (1, $1, $2, $3, $1, 'EventDb', 55, 56, 1000)", ct, Naive(end.AddDays(-1)), ServerId, ServerName);
            await Exec(connection,
                "INSERT INTO collect.deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml) " +
                "VALUES (1, $1, $2, $3, $1, 'process1', 'SELECT 1', '<deadlock />')", ct, Naive(end.AddDays(-1)), ServerId, ServerName);
        }

        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        var start = end.AddDays(-7);

        Assert.Equal(end.AddDays(-2), await viewer.GetBlockedProcessReportsDataStartAsync(ServerId, start, end, ct));
        Assert.Equal(end.AddDays(-2), await viewer.GetDeadlocksDataStartAsync(ServerId, start, end, ct));
        Assert.Equal(end.AddDays(-2), await viewer.GetLockWaitTrendDataStartAsync(ServerId, start, end, ct));
        /* A window of 90 minutes or less runs no probe. */
        Assert.Null(await viewer.GetLockWaitTrendDataStartAsync(ServerId, end.AddMinutes(-60), end, ct));
    }

    private static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

    private static async Task Exec(NpgsqlConnection connection, string sql, CancellationToken ct, params object[] args)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var arg in args)
        {
            command.Parameters.AddWithValue(arg);
        }

        await command.ExecuteNonQueryAsync(ct);
    }
}
