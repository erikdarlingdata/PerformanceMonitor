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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live test mints its own scratch database and seeds its own server, so it shares no rows with another test. */
/// <summary>
/// #7725: the Viewer FinOps grids that keep a dropped database's row mark it: Database Resources, Top Databases (by total CPU and by
/// average CPU) and High Impact. A row whose database an earlier snapshot listed and the newest does not is <c>IsDropped</c>, and the
/// grid's Database cell (<c>DatabaseDisplay</c>) reads "name (dropped)" while <c>DatabaseName</c> stays the sort and filter key.
/// </summary>
public sealed class ViewerFinOpsDroppedDatabaseTests
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
    public async Task DatabaseResources_TopDatabases_AndHighImpact_MarkTheDroppedDatabase_AndKeepThePlainNameAsTheKey()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live Viewer FinOps dropped-database test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        const string serverName = "viewer-finops-dropped";
        var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);
        await DarlingMcpTestData.RegisterServerAsync(connection, serverId, serverName, ct);
        var now = DateTime.UtcNow;
        /* The dropped database is the heavier one. */
        await PlantQueryAsync(connection, serverId, serverName, Gone, now.AddHours(-2), 900_000, ct);
        await PlantQueryAsync(connection, serverId, serverName, Live, now.AddHours(-2), 100_000, ct);

        await using var viewer = new ViewerDataService(scratch.ConnectionString);

        /* No snapshot: the set is unknown, so nothing is marked. */
        var unknown = await viewer.GetDatabaseResourceUsageAsync(serverId, cancellationToken: ct);
        Assert.NotEmpty(unknown);
        Assert.DoesNotContain(unknown, r => r.IsDropped);

        /* An earlier snapshot named both; the newest names LiveDb only, so GoneDb was dropped in between. */
        await SnapshotAsync(connection, serverId, serverName, now.AddHours(-1), ct, Gone, Live);
        await SnapshotAsync(connection, serverId, serverName, now.AddMinutes(-5), ct, Live);

        var usage = await viewer.GetDatabaseResourceUsageAsync(serverId, cancellationToken: ct);
        Assert.Equal(new[] { Gone, Live }, usage.Select(r => r.DatabaseName).ToArray());
        Assert.Equal(new[] { Gone + " (dropped)", Live }, usage.Select(r => r.DatabaseDisplay).ToArray());

        var (byTotal, byAvg) = await viewer.GetTopResourceConsumersAsync(serverId, cancellationToken: ct);
        Assert.NotEmpty(byTotal);
        Assert.NotEmpty(byAvg);
        Assert.Equal(new[] { Gone, Live }, byTotal.Select(r => r.DatabaseName).ToArray());
        Assert.Equal(new[] { Gone + " (dropped)", Live }, byTotal.Select(r => r.DatabaseDisplay).ToArray());
        Assert.Equal(new[] { Gone + " (dropped)", Live }, byAvg.OrderBy(r => r.DatabaseName, StringComparer.Ordinal).Select(r => r.DatabaseDisplay).ToArray());

        var impact = await viewer.GetHighImpactQueriesAsync(serverId, cancellationToken: ct);
        Assert.NotEmpty(impact);
        Assert.Equal(Gone + " (dropped)", impact.Single(r => r.DatabaseName == Gone).DatabaseDisplay);
        Assert.Equal(Live, impact.Single(r => r.DatabaseName == Live).DatabaseDisplay);
    }

    [Fact]
    public void HighImpactCell_PutsTheDroppedMarkOnTheName_BeforeThePlusMore()
    {
        var row = new HighImpactQueryRow { DatabaseName = Gone, DatabaseCount = 3 };
        Assert.Equal(Gone + " + 2 more", row.DatabaseDisplay);

        row.MarkDropped(true);
        Assert.Equal(Gone + " (dropped) + 2 more", row.DatabaseDisplay);
    }
}
