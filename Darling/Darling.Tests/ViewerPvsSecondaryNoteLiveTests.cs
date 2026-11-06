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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the test seeds its own scratch database, so nothing here shares rows with another test. */
/// <summary>
/// #7706: the Viewer's Version Store grid carries the same sentence the web page and <c>get_pvs_stats</c> do for a database this server
/// holds only as an availability group secondary copy and that has no PVS row. The tab's strip shows exactly what
/// <see cref="ViewerDataService.GetPvsStatsLatestWithNoteAsync"/> returns, so the read is what is pinned here.
/// </summary>
public sealed class ViewerPvsSecondaryNoteLiveTests
{
    private const string BaseSkip = "Set DARLING_TEST_PG to a Postgres connection string to run the live Viewer PVS secondary note test.";
    private static int ServerId => FinOpsRecommendationsGoldenLiveTests.ServerIdA;
    private static string ServerName => FinOpsRecommendationsGoldenLiveTests.ServerNameA;

    private static DateTime Naive(TimeSpan age) => DateTime.SpecifyKind(DateTime.UtcNow - age, DateTimeKind.Unspecified);

    [Fact]
    public async Task ASecondaryCopyWithNoPvsRow_GetsTheNote_AndAChosenDatabaseThatLeavesItOutDoesNot()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), BaseSkip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await FinOpsRecommendationsGoldenLiveTests.SeedAsync(connection, ct);

        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO pvs_stats (collection_id, collection_time, server_id, server_name, database_name, database_id, is_accelerated_database_recovery_on, persistent_version_store_size_mb, database_data_size_mb)
VALUES ($1,$2,$3,$4,'master',1,TRUE,10,1000)",
            CollectionIdGenerator.Next(), Naive(TimeSpan.FromMinutes(1)), ServerId, ServerName);

        await using var viewer = new ViewerDataService(scratch.ConnectionString);

        /* No Availability Group snapshots: no note. */
        var (plainRows, plainNote) = await viewer.GetPvsStatsLatestWithNoteAsync(ServerId, cancellationToken: ct);
        Assert.Single(plainRows);
        Assert.Null(plainNote);

        /* A fresh snapshot that holds AgDb as a secondary copy here, with no PVS row for it: the sentence the web page shows. */
        var at = Naive(TimeSpan.FromMinutes(2));
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO ag_replica_states (collection_id, collection_time, server_id, server_name, ag_name, replica_server_name, role_desc, is_local) VALUES ($1,$2,$3,$4,'AG1','NODE1','SECONDARY',TRUE)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO ag_database_replica_states (collection_id, collection_time, server_id, server_name, ag_name, database_name, replica_server_name, is_local) VALUES ($1,$2,$3,$4,'AG1','AgDb','NODE1',TRUE)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName);

        var (rows, note) = await viewer.GetPvsStatsLatestWithNoteAsync(ServerId, cancellationToken: ct);
        Assert.Single(rows);
        Assert.Equal(PvsSecondaryNote.WithoutRow(new[] { "AgDb" }, new[] { "master" }, DatabaseFilter.All), note);
        Assert.StartsWith("1 database has no row here: this server holds a secondary copy of it in an availability group.", note, StringComparison.Ordinal);

        /* The saved database filter chose only master, so the secondary copy is not among the databases the grid is about. */
        var (_, filteredNote) = await viewer.GetPvsStatsLatestWithNoteAsync(ServerId, new[] { "master" }, ct);
        Assert.Null(filteredNote);

        /* The same database with a row of its own is not named. */
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO pvs_stats (collection_id, collection_time, server_id, server_name, database_name, database_id, is_accelerated_database_recovery_on, persistent_version_store_size_mb, database_data_size_mb)
VALUES ($1,$2,$3,$4,'AgDb',9,TRUE,5,500)",
            CollectionIdGenerator.Next(), Naive(TimeSpan.FromSeconds(30)), ServerId, ServerName);
        var (withRow, noNote) = await viewer.GetPvsStatsLatestWithNoteAsync(ServerId, cancellationToken: ct);
        Assert.NotEmpty(withRow);
        Assert.Null(noNote);
    }
}
