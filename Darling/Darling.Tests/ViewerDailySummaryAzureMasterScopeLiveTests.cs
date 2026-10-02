/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4925: the Viewer's Performance Calendar counts an Azure SQL Database master's own blocking and deadlocks only,
/// the same figures and band <c>get_daily_health</c> reads for the same days. Gated on DARLING_TEST_PG.
/// </summary>
/* #1776 own-store: this fact mints its own scratch database through ScratchPostgres and never touches another
   fact's rows. */
[Collection("live-postgres")]
public sealed class ViewerDailySummaryAzureMasterScopeLiveTests
{
    private const int MasterId = 8201;
    private const int GpId = 8202;
    private const int LoneId = 8203;
    private const int PlainId = 8204;
    private static readonly string[] Separate = { "GP" };
    private static readonly DateTime Day1 = DateTime.UtcNow.Date.AddDays(-3);
    private static readonly DateTime Day2 = Day1.AddDays(1);

    private static string Graph(params string[] dbs) =>
        "<deadlock><process-list>" + string.Concat(dbs.Select((d, i) => $"<process id=\"p{i}\" currentdbname=\"{d}\" />")) + "</process-list></deadlock>";

    private static async Task WithStoreAsync(Func<ScratchPostgres, NpgsqlConnection, CancellationToken, Task> body)
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
            await RegisterAsync(connection, LoneId, "host-c.example", "master", 5, ct);
            await RegisterAsync(connection, PlainId, "host-b.example", null, 3, ct);

            /* Day 1: three GP reports waiting 600 s and one of the master's own waiting 2 s; day 2: only GP's, so
               the scoped day falls back to the DMV snapshots. Deadlocks: an own-database row, a graph wholly in
               GP, a mixed graph and a NULL-database graph wholly in GP. */
            for (var i = 0; i < 3; i++) await BprAsync(connection, MasterId, Day1, 600_000, "GP", ct);
            await BprAsync(connection, MasterId, Day1, 2_000, "master", ct);
            for (var i = 0; i < 2; i++) await BprAsync(connection, MasterId, Day2, 400_000, "GP", ct);
            await DmvAsync(connection, MasterId, Day2, 7_000, ct);
            await DmvAsync(connection, MasterId, Day2, 3_000, ct);
            await DeadAsync(connection, MasterId, Day1, 0, "Other", Graph("Other"), ct);
            await DeadAsync(connection, MasterId, Day1, 1, "master", Graph("GP", "GP"), ct);
            await DeadAsync(connection, MasterId, Day1, 2, "master", Graph("GP", "HS"), ct);
            await DeadAsync(connection, MasterId, Day1, 3, null, Graph("gp"), ct);

            /* The same events on a master with no sibling and on a SQL Server target read as ever. */
            await BprAsync(connection, LoneId, Day1, 5_000, "master", ct);
            await BprAsync(connection, PlainId, Day1, 5_000, "GP", ct);
            await DeadAsync(connection, PlainId, Day1, 0, "GP", Graph("GP"), ct);

            await body(scratch, connection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    private static async Task<List<DailySummaryRow>> ViewerAsync(ViewerDataService viewer, int id, CancellationToken ct)
        => await viewer.GetDailySummaryRangeAsync(id, Day1, Day2.AddDays(1), ct);

    private static async Task<List<DarlingHealthReader.DailySummaryReadRow>> ServiceAsync(
        string connectionString, int id, IReadOnlyList<string>? list, CancellationToken ct)
    {
        DarlingHealthReader.ResetRangeCacheForTests();
        await using var postgres = NpgsqlDataSource.Create(connectionString);
        return (await DarlingHealthReader.GetDailySummaryRangeAsync(
            postgres, id, Day1, Day2.AddDays(1), separatelyMonitored: list, logger: NullLogger.Instance, cancellationToken: ct)).Rows;
    }

    private static void AssertSame(List<DarlingHealthReader.DailySummaryReadRow> service, List<DailySummaryRow> viewer)
    {
        Assert.Equal(service.Count, viewer.Count);
        for (var i = 0; i < service.Count; i++)
        {
            Assert.Equal(service[i].SummaryDate.Date, viewer[i].SummaryDate.Date);
            Assert.Equal(service[i].BlockingEvents, viewer[i].BlockingEvents);
            Assert.Equal(service[i].MaxBlockDurationMs, viewer[i].MaxBlockDurationMs);
            Assert.Equal(service[i].DeadlockCount, viewer[i].DeadlockCount);
            Assert.Equal(service[i].HealthBand.ToString(), viewer[i].HealthBand.ToString());
        }
    }

    [Fact]
    public async Task TheCalendar_CountsOnlyTheMastersOwnEvents_AsTheServiceDoes() => await WithStoreAsync(async (scratch, _, ct) =>
    {
        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        var rows = await ViewerAsync(viewer, MasterId, ct);
        var unscoped = await ServiceAsync(scratch.ConnectionString, MasterId, null, ct);
        var scoped = await ServiceAsync(scratch.ConnectionString, MasterId, Separate, ct);

        Assert.Equal(1, rows[0].BlockingEvents);
        Assert.Equal(2_000, rows[0].MaxBlockDurationMs);
        Assert.Equal(2, rows[0].DeadlockCount);
        Assert.Equal(2, rows[1].BlockingEvents);
        Assert.Equal(7_000, rows[1].MaxBlockDurationMs);
        Assert.NotEqual(unscoped[0].HealthBand.ToString(), rows[0].HealthBand.ToString());
        AssertSame(scoped, rows);
    });

    [Fact]
    public async Task ALonelyMasterAndASqlServerTarget_ReadAsTheUnscopedService() => await WithStoreAsync(async (scratch, _, ct) =>
    {
        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        foreach (var id in new[] { LoneId, PlainId })
        {
            AssertSame(await ServiceAsync(scratch.ConnectionString, id, null, ct), await ViewerAsync(viewer, id, ct));
        }
    });

    [Fact]
    public async Task AFailedScopedRead_ReadsTheUnscopedRows_WithoutThrowing() => await WithStoreAsync(async (scratch, _, ct) =>
    {
        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        viewer.ScopeReadHookForTests = stage =>
            stage == "dailysummary" ? throw new InvalidOperationException("scope read failed") : Task.CompletedTask;

        var rows = await ViewerAsync(viewer, MasterId, ct);
        AssertSame(await ServiceAsync(scratch.ConnectionString, MasterId, null, ct), rows);
        Assert.Equal(4, rows[0].BlockingEvents);
    });

    private static async Task RegisterAsync(NpgsqlConnection connection, int id, string host, string? database, int edition, CancellationToken ct)
    {
        var name = host + "/" + (database ?? "-");
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_engine_edition, sql_major_version, created_date, modified_date) VALUES ($1, $2, $2, TRUE, $3, 16, now()::timestamp, now()::timestamp)",
            id, name, edition);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, engine_edition) VALUES ($1, now()::timestamp, $2, 's', $3)",
            CollectionIdGenerator.Next(), id, edition);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO config.config_monitored_servers (server_id, name, host, database, is_enabled) VALUES ($1, $2, $3, $4, TRUE)",
            id, name, host, (object?)database ?? DBNull.Value);
        foreach (var day in new[] { Day1, Day2 })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES ($1, $2, $3, 'wait_stats', $4, 'SUCCESS')",
                CollectionIdGenerator.Next(), id, name, day.AddHours(1));
        }
    }

    private static Task BprAsync(NpgsqlConnection connection, int id, DateTime day, int waitMs, string db, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid, blocking_status, database_name) VALUES ($1, $2, $3, 's', $2, $4, 60, 70, 'suspended', $5)",
            CollectionIdGenerator.Next(), day.AddHours(12), id, waitMs, db);

    private static Task DmvAsync(NpgsqlConnection connection, int id, DateTime day, int waitMs, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms, blocking_status) VALUES ($1, $2, $3, 's', $2, 'master', 70, 60, $4, 'suspended')",
            CollectionIdGenerator.Next(), day.AddHours(13), id, waitMs);

    private static Task DeadAsync(NpgsqlConnection connection, int id, DateTime day, int minute, string? stamp, string xml, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml, database_name) VALUES ($1, $2, $3, 's', $2, $4, $5)",
            CollectionIdGenerator.Next(), day.AddHours(14).AddMinutes(minute), id, xml, (object?)stamp ?? DBNull.Value);
}
