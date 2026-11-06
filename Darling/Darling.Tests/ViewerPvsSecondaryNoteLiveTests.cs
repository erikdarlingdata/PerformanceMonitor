/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
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

    /* A registry entry as the Viewer holds it: a SQL Server 2022 unless a test says otherwise. */
    private static DarlingServer Server(int majorVersion = 16) => new(ServerId, ServerName, ServerName, true, majorVersion);

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
        var (plainRows, plainNote) = await viewer.GetPvsStatsLatestWithNoteAsync(Server(), cancellationToken: ct);
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

        var (rows, note) = await viewer.GetPvsStatsLatestWithNoteAsync(Server(), cancellationToken: ct);
        Assert.Single(rows);
        Assert.Equal(PvsSecondaryNote.WithoutRow(new[] { "AgDb" }, new[] { "master" }, null), note);
        Assert.StartsWith("1 database has no row here: this server holds a secondary copy of it in an availability group.", note, StringComparison.Ordinal);

        /* The saved database filter chose only master, so the secondary copy is not among the databases the grid is about. */
        var (_, filteredNote) = await viewer.GetPvsStatsLatestWithNoteAsync(Server(), new[] { "master" }, ct);
        Assert.Null(filteredNote);

        /* The same database with a row of its own is not named. */
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO pvs_stats (collection_id, collection_time, server_id, server_name, database_name, database_id, is_accelerated_database_recovery_on, persistent_version_store_size_mb, database_data_size_mb)
VALUES ($1,$2,$3,$4,'AgDb',9,TRUE,5,500)",
            CollectionIdGenerator.Next(), Naive(TimeSpan.FromSeconds(30)), ServerId, ServerName);
        var (withRow, noNote) = await viewer.GetPvsStatsLatestWithNoteAsync(Server(), cancellationToken: ct);
        Assert.NotEmpty(withRow);
        Assert.Null(noNote);
    }

    [Fact]
    public async Task AFilterThatPicksOnlyASecondaryCopy_GetsTheNoteOnTheEmptyRowSet_AtTheSnapshotsOwnTime()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), BaseSkip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await FinOpsRecommendationsGoldenLiveTests.SeedAsync(connection, ct);

        /* The newest PVS snapshot (master only) is five hours old. AgDb was a secondary copy then; the node has since become its primary. */
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO pvs_stats (collection_id, collection_time, server_id, server_name, database_name, database_id, is_accelerated_database_recovery_on, persistent_version_store_size_mb, database_data_size_mb)
VALUES ($1,$2,$3,$4,'master',1,TRUE,10,1000)",
            CollectionIdGenerator.Next(), Naive(TimeSpan.FromHours(5)), ServerId, ServerName);
        foreach (var (role, age) in new[] { ("SECONDARY", TimeSpan.FromHours(5) + TimeSpan.FromMinutes(1)), ("PRIMARY", TimeSpan.FromMinutes(1)) })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO ag_replica_states (collection_id, collection_time, server_id, server_name, ag_name, replica_server_name, role_desc, is_local) VALUES ($1,$2,$3,$4,'AG1','NODE1',$5,TRUE)",
                CollectionIdGenerator.Next(), Naive(age), ServerId, ServerName, role);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO ag_database_replica_states (collection_id, collection_time, server_id, server_name, ag_name, database_name, replica_server_name, is_local) VALUES ($1,$2,$3,$4,'AG1','AgDb','NODE1',TRUE)",
                CollectionIdGenerator.Next(), Naive(age), ServerId, ServerName);
        }

        await using var viewer = new ViewerDataService(scratch.ConnectionString);

        /* The rows reply: judged at the snapshot's time, so the old secondary role still explains the missing row. */
        var (rows, note) = await viewer.GetPvsStatsLatestWithNoteAsync(Server(), cancellationToken: ct);
        Assert.Single(rows);
        Assert.Equal(PvsSecondaryNote.WithoutRow(new[] { "AgDb" }, new[] { "master" }), note);

        /* The saved filter chose only AgDb: no rows, and the grid strip still gets the sentence. */
        var (emptyRows, emptyNote) = await viewer.GetPvsStatsLatestWithNoteAsync(Server(), new[] { "AgDb" }, ct);
        Assert.Empty(emptyRows);
        Assert.Equal(PvsSecondaryNote.WithoutRow(new[] { "AgDb" }, Array.Empty<string>(), new[] { "AgDb" }), emptyNote);

        /* A filter that leaves the secondary copy out says nothing about it. */
        var (_, otherNote) = await viewer.GetPvsStatsLatestWithNoteAsync(Server(), new[] { "master" }, ct);
        Assert.Null(otherNote);
    }

    [Fact]
    public async Task ASql2017Secondary_GetsNoNote_BecauseThePrimaryHasNoPersistentVersionStoreEither()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), BaseSkip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await FinOpsRecommendationsGoldenLiveTests.SeedAsync(connection, ct);

        /* #7822: a fresh snapshot with AgDb as a secondary copy and no PVS row at all, which is every database on SQL Server 2017. */
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO ag_replica_states (collection_id, collection_time, server_id, server_name, ag_name, replica_server_name, role_desc, is_local) VALUES ($1,$2,$3,$4,'AG1','NODE1','SECONDARY',TRUE)",
            CollectionIdGenerator.Next(), Naive(TimeSpan.FromMinutes(2)), ServerId, ServerName);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO ag_database_replica_states (collection_id, collection_time, server_id, server_name, ag_name, database_name, replica_server_name, is_local) VALUES ($1,$2,$3,$4,'AG1','AgDb','NODE1',TRUE)",
            CollectionIdGenerator.Next(), Naive(TimeSpan.FromMinutes(2)), ServerId, ServerName);

        await using var viewer = new ViewerDataService(scratch.ConnectionString);

        foreach (var filter in new IReadOnlyList<string>?[] { null, new[] { "AgDb" } })
        {
            var (rows, note) = await viewer.GetPvsStatsLatestWithNoteAsync(Server(14), filter, ct);
            Assert.Empty(rows);
            Assert.Null(note);
        }

        /* The same data on SQL Server 2019 keeps the sentence. */
        var (_, kept) = await viewer.GetPvsStatsLatestWithNoteAsync(Server(15), null, ct);
        Assert.Equal(PvsSecondaryNote.WithoutRow(new[] { "AgDb" }, Array.Empty<string>()), kept);
    }
}
