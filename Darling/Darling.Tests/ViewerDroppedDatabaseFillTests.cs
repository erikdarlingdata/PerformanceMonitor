/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live test mints its own scratch database and seeds its own server, so it shares no rows with another test. */
/// <summary>
/// #7725: the Viewer's Top Procedures and Top Queries grids rank the databases the server still lists first, then fill what the
/// limit leaves from the dropped databases (ranked the same way), and mark each of those rows <c>IsDropped</c>, which the grid's database
/// cell shows as "name (dropped)". The dropped database is the HEAVIER one, so a read that ranked it in with the rest would put it first.
/// </summary>
public sealed class ViewerDroppedDatabaseFillTests
{
    private const string Gone = "GoneDb";
    private const string Live = "LiveDb";

    private static Task PlantQueryAsync(NpgsqlConnection connection, int serverId, string serverName, string database, DateTime at, long weight, CancellationToken ct)
    {
        var text = "SELECT " + database;
        return DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collect.query_stats
                  (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
                   query_text, query_text_digest, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads,
                   delta_logical_writes, delta_physical_reads, min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time,
                   min_dop, max_dop, sample_interval_seconds)
              VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 10, $12, $12, 100, 0, 0, 1, $12, 1, $12, 1, 1, 3600)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.TruncateToSeconds(at), serverId, serverName, database,
            "0xQ" + database, "0xP" + database, "0xS" + database, "0xL" + database, text, SHA256.HashData(Encoding.UTF8.GetBytes(text)), weight);
    }

    private static Task PlantProcedureAsync(NpgsqlConnection connection, int serverId, string serverName, string database, DateTime at, long executions, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collect.procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
                                                   delta_worker_time, delta_elapsed_time, delta_execution_count, delta_spills, sample_interval_seconds)
              VALUES ($1,$2,$3,$4,$5,'dbo','usp_Work',$6,$7,$8,$9,0,3600)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.TruncateToSeconds(at), serverId, serverName, database, "0xH" + database,
            executions * 1_000L, executions * 2_000L, executions);

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

    [Fact]
    public async Task TopProceduresAndTopQueries_FillFromTheDroppedDatabase_AfterTheListedOne_AndMarkIt()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live Viewer dropped-database fill test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        const string serverName = "viewer-dropped-fill";
        var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);
        await DarlingMcpTestData.RegisterServerAsync(connection, serverId, serverName, ct);
        var now = DateTime.UtcNow;
        await PlantProcedureAsync(connection, serverId, serverName, Gone, now.AddHours(-2), 900, ct);
        await PlantProcedureAsync(connection, serverId, serverName, Live, now.AddHours(-2), 100, ct);
        await PlantQueryAsync(connection, serverId, serverName, Gone, now.AddHours(-2), 900_000, ct);
        await PlantQueryAsync(connection, serverId, serverName, Live, now.AddHours(-2), 100_000, ct);

        /* An earlier snapshot named both; the newest names LiveDb only, so GoneDb was dropped in between. */
        await SnapshotAsync(connection, serverId, serverName, now.AddHours(-1), ct, Gone, Live);
        await SnapshotAsync(connection, serverId, serverName, now.AddMinutes(-5), ct, Live);

        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        var start = now.AddHours(-24);
        var end = now.AddMinutes(1);

        /* Top Procedures: the page has room, so the dropped database's row follows the listed one, marked; no banner is left to name it. */
        var procedures = await viewer.GetTopProceduresByCpuRoutedAsync(serverId, start, end, 20, cancellationToken: ct);
        Assert.NotEmpty(procedures.Rows);
        Assert.Equal(new[] { Live, Gone }, procedures.Rows.Select(r => r.DatabaseName).ToArray());
        Assert.Equal(new[] { false, true }, procedures.Rows.Select(r => r.IsDropped).ToArray());
        Assert.Equal(new[] { Live, Gone + " (dropped)" }, procedures.Rows.Select(r => r.DatabaseDisplay).ToArray());
        Assert.Null(procedures.LeftOutDatabases);

        /* A page the listed row fills leaves the dropped database out, and the banner names it. */
        var proceduresTop1 = await viewer.GetTopProceduresByCpuRoutedAsync(serverId, start, end, 1, cancellationToken: ct);
        Assert.Equal(new[] { Live }, proceduresTop1.Rows.Select(r => r.DatabaseName).ToArray());
        Assert.Equal(new[] { Gone }, proceduresTop1.LeftOutDatabases);

        /* Top Queries: the same order and mark. */
        var queries = await viewer.GetTopQueriesByCpuRoutedAsync(serverId, start, end, 20, cancellationToken: ct);
        Assert.NotEmpty(queries.Rows);
        Assert.Equal(new[] { Live, Gone }, queries.Rows.Select(r => r.DatabaseName).ToArray());
        Assert.Equal(new[] { false, true }, queries.Rows.Select(r => r.IsDropped).ToArray());
        Assert.Equal(new[] { Live, Gone + " (dropped)" }, queries.Rows.Select(r => r.DatabaseDisplay).ToArray());

        var queriesTop1 = await viewer.GetTopQueriesByCpuRoutedAsync(serverId, start, end, 1, cancellationToken: ct);
        Assert.Equal(new[] { Live }, queriesTop1.Rows.Select(r => r.DatabaseName).ToArray());

        /* Named, a dropped database is read as asked and is not marked. */
        var named = await viewer.GetTopQueriesByCpuRoutedAsync(serverId, start, end, 20, new[] { Gone }, cancellationToken: ct);
        Assert.Equal(new[] { Gone }, named.Rows.Select(r => r.DatabaseName).ToArray());
        Assert.DoesNotContain(named.Rows, r => r.IsDropped);
    }
}
