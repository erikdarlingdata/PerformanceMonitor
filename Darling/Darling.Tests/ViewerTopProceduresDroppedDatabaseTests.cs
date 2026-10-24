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

/* #1776 own-store: the live tests mint their own scratch database and seed their own server, so they share no rows with another test. */
/// <summary>
/// #6651 (#5633 item 4): the Viewer's Top Procedures by Duration read leaves out the databases the server dropped, as
/// <c>get_top_procedures_by_cpu</c> does, on the raw tier and on the hourly rollup. The dropped database is the HEAVIER one and the page
/// is top 1, so a read that only filtered after the LIMIT would come back empty or wrong. A database the caller names is read as asked.
/// </summary>
public sealed class ViewerTopProceduresDroppedDatabaseTests
{
    private const string Gone = "GoneDb";
    private const string Live = "LiveDb";

    private static async Task SnapshotAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime at, CancellationToken ct, params string[] databases)
    {
        var id = 1;
        foreach (var database in databases)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby) VALUES ($1,$2,$3,$4,$5,$6,'ONLINE',FALSE)",
                CollectionIdGenerator.Next(), DateTime.SpecifyKind(at, DateTimeKind.Unspecified), serverId, serverName, database, id++);
        }
    }

    private static async Task PlantProcedureAsync(NpgsqlConnection connection, int serverId, string serverName, string database, DateTime at, long executions, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collect.procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
                                                   delta_worker_time, delta_elapsed_time, delta_execution_count, delta_spills, sample_interval_seconds)
              VALUES ($1,$2,$3,$4,$5,'dbo','usp_Work',$6,$7,$8,$9,0,3600)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.TruncateToSeconds(at), serverId, serverName, database, "0xH" + database,
            executions * 1_000L, executions * 2_000L, executions);
    }

    [Fact]
    public async Task RawTier_LeavesTheDroppedDatabaseOut_RanksTheLiveOneAtTop1_AndReadsANamedDroppedDatabaseAsAsked()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live Viewer dropped-database test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        const string serverName = "viewer-dropped-raw";
        var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);
        await DarlingMcpTestData.RegisterServerAsync(connection, serverId, serverName, ct);
        var now = DateTime.UtcNow;
        await PlantProcedureAsync(connection, serverId, serverName, Gone, now.AddHours(-2), 900, ct);
        await PlantProcedureAsync(connection, serverId, serverName, Live, now.AddHours(-2), 100, ct);

        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        var start = now.AddHours(-24);
        var end = now.AddMinutes(1);

        /* No snapshot: the set of databases is unknown, so nothing is left out and the heavier database ranks first. */
        var unknown = await viewer.GetTopProceduresByCpuRoutedAsync(serverId, start, end, 20, cancellationToken: ct);
        Assert.Equal(new[] { Gone, Live }, unknown.Rows.Select(r => r.DatabaseName).ToArray());
        Assert.Null(unknown.LeftOutDatabases);

        /* An earlier snapshot named both; the newest names LiveDb only, so GoneDb was dropped in between. */
        await SnapshotAsync(connection, serverId, serverName, now.AddHours(-1), ct, Gone, Live);
        await SnapshotAsync(connection, serverId, serverName, now.AddMinutes(-5), ct, Live);

        var top1 = await viewer.GetTopProceduresByCpuRoutedAsync(serverId, start, end, 1, cancellationToken: ct);
        Assert.Equal("raw", top1.Tier);
        Assert.Equal(new[] { Live }, top1.Rows.Select(r => r.DatabaseName).ToArray());
        Assert.Equal(new[] { Gone }, top1.LeftOutDatabases);

        /* Named, a dropped database is a question about its history and is read as asked, with nothing left out. */
        var named = await viewer.GetTopProceduresByCpuRoutedAsync(serverId, start, end, 20, new[] { Gone }, cancellationToken: ct);
        Assert.Equal(new[] { Gone }, named.Rows.Select(r => r.DatabaseName).ToArray());
        Assert.Null(named.LeftOutDatabases);
    }

    [Fact]
    public async Task HourlyTier_LeavesTheDroppedDatabaseOut_AtTop1()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live Viewer dropped-database hourly test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live Viewer dropped-database hourly test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        const string serverName = "viewer-dropped-hourly";
        var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);
        await DarlingMcpTestData.RegisterServerAsync(connection, serverId, serverName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        /* A fixed past window, as the other hourly live tests use; the snapshots sit just before the window's end, which the read anchors at. */
        var windowStart = new DateTime(2026, 1, 5, 0, 0, 0, DateTimeKind.Unspecified);
        var hour = windowStart.AddHours(1);
        var end = windowStart.AddHours(8);
        var bodySucceeded = false;
        try
        {
            await PlantProcedureAsync(connection, serverId, serverName, Gone, hour.AddMinutes(10), 900, ct);
            await PlantProcedureAsync(connection, serverId, serverName, Live, hour.AddMinutes(10), 100, ct);
            await SnapshotAsync(connection, serverId, serverName, end.AddHours(-4), ct, Gone, Live);
            await SnapshotAsync(connection, serverId, serverName, end.AddHours(-1), ct, Live);

            foreach (var view in new[] { TimescaleSupport.ProcedureStatsHourlyView, TimescaleSupport.ProcedureStatsIntervalHourlyView, TimescaleSupport.ProcedureStatsIoHourlyView })
            {
                await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
                refresh.Parameters.AddWithValue(windowStart);
                refresh.Parameters.AddWithValue(end);
                await refresh.ExecuteNonQueryAsync(ct);
            }

            /* Purge raw so the router answers from the rollup. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "DELETE FROM collect.procedure_stats WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3", serverId, windowStart, windowStart.AddDays(1));

            await using var viewer = new ViewerDataService(scratch.ConnectionString);
            var read = await viewer.GetTopProceduresByCpuRoutedAsync(serverId, hour, end, 1, cancellationToken: ct);
            Assert.Equal("hourly", read.Tier);
            Assert.Equal(new[] { Live }, read.Rows.Select(r => r.DatabaseName).ToArray());
            Assert.Equal(new[] { Gone }, read.LeftOutDatabases);

            var named = await viewer.GetTopProceduresByCpuRoutedAsync(serverId, hour, end, 1, new[] { Gone }, cancellationToken: ct);
            Assert.Equal(new[] { Gone }, named.Rows.Select(r => r.DatabaseName).ToArray());
            Assert.Null(named.LeftOutDatabases);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (cleanup, cleanupCt) => Task.CompletedTask);
        }
    }

    private static readonly string[] Dropped = { "zz_a", "zz_b" };

    [Fact]
    public void LeftOutText_IsNullWhenNothingWasLeftOut()
    {
        Assert.Null(DatabasesLeftOut.ProcedureBanner(null, 5, null, 24));
        Assert.Null(DatabasesLeftOut.ProcedureBanner(Array.Empty<string>(), 0, null, 24));
    }

    [Fact]
    public void LeftOutText_WithRows_NamesTheDatabasesAndSaysHowToReadThem()
    {
        var text = DatabasesLeftOut.ProcedureBanner(Dropped, 3, null, 24)!;
        Assert.Contains("Left out: zz_a, zz_b.", text, StringComparison.Ordinal);
        Assert.Contains("left out of this read", text, StringComparison.Ordinal);
    }

    [Fact]
    public void LeftOutText_OnAnEmptyPage_SaysNoProcedureRanInADatabaseTheServerStillHas_AndHidesTheMcpKey()
    {
        var facts = new CollectorRunFacts(10, 4, DateTime.UtcNow, null);
        var text = DatabasesLeftOut.ProcedureBanner(Dropped, 0, facts, 24)!;
        Assert.Contains("No procedure ran in the last 24h in a database this server still has", text, StringComparison.Ordinal);
        Assert.Contains("2 database(s) the server no longer lists were left out of this read", text, StringComparison.Ordinal);
        Assert.DoesNotContain("databases_left_out", text, StringComparison.Ordinal);
        Assert.Contains("Left out: zz_a, zz_b.", text, StringComparison.Ordinal);
    }
}
