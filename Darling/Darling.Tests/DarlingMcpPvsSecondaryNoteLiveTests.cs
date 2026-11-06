/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the test seeds its own scratch database, so nothing here shares rows with another test. */
/// <summary>
/// #7706: Darling's <c>get_pvs_stats</c> tool carries <c>secondary_replica_note</c> for a database this server holds only as an availability
/// group secondary copy and that has no PVS row: on the rows reply, and on the empty reply a database filter gets when it picks only such
/// copies. The note judges the replica roles at the snapshot's own time, not today's.
/// </summary>
public sealed class DarlingMcpPvsSecondaryNoteLiveTests
{
    private const string BaseSkip = "Set DARLING_TEST_PG to a Postgres connection string to run the live get_pvs_stats secondary note test.";
    private static int ServerId => FinOpsRecommendationsGoldenLiveTests.ServerIdA;
    private static string ServerName => FinOpsRecommendationsGoldenLiveTests.ServerNameA;

    private static DateTime Naive(TimeSpan age) => DateTime.SpecifyKind(DateTime.UtcNow - age, DateTimeKind.Unspecified);

    private static async Task<(ScratchPostgres Scratch, NpgsqlConnection Connection)> OpenAsync(string baseConnectionString, CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await FinOpsRecommendationsGoldenLiveTests.SeedAsync(connection, ct);
        return (scratch, connection);
    }

    private static Task SeedPvsAsync(NpgsqlConnection connection, string database, TimeSpan age, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO pvs_stats (collection_id, collection_time, server_id, server_name, database_name, database_id, is_accelerated_database_recovery_on, persistent_version_store_size_mb, database_data_size_mb)
VALUES ($1,$2,$3,$4,$5,9,TRUE,10,1000)",
            CollectionIdGenerator.Next(), Naive(age), ServerId, ServerName, database);

    /// <summary>One AG snapshot taken <paramref name="age"/> ago: the local replica has <paramref name="localRole"/> and holds <paramref name="databases"/>.</summary>
    private static async Task SeedAgAsync(NpgsqlConnection connection, string localRole, TimeSpan age, CancellationToken ct, params string[] databases)
    {
        var at = Naive(age);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO ag_replica_states (collection_id, collection_time, server_id, server_name, ag_name, replica_server_name, role_desc, is_local) VALUES ($1,$2,$3,$4,'AG1','NODE1',$5,TRUE)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, localRole);
        foreach (var database in databases)
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO ag_database_replica_states (collection_id, collection_time, server_id, server_name, ag_name, database_name, replica_server_name, is_local) VALUES ($1,$2,$3,$4,'AG1',$5,'NODE1',TRUE)",
                CollectionIdGenerator.Next(), at, ServerId, ServerName, database);
    }

    private static string? NoteOf(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.TryGetProperty(PvsSecondaryNote.Key, out var note) && note.ValueKind == JsonValueKind.String ? note.GetString() : null;
    }

    private static string StatusOf(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.TryGetProperty("status", out var status) ? status.GetString() ?? "" : "rows";
    }

    [Fact]
    public async Task ASecondaryCopyWithNoPvsRow_CarriesTheNoteOnTheRowsReplyAndOnTheEmptyReplyAFilterGets()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), BaseSkip);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection) = await OpenAsync(baseConnectionString!, ct);
        await using var _s = scratch;
        await using var _c = connection;
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await SeedPvsAsync(connection, "master", TimeSpan.FromMinutes(1), ct);

        /* A primary-only server (no availability group snapshot): no key on the rows reply, and none on the empty reply a filter gets. */
        Assert.Null(NoteOf(await DarlingMcpPvsTools.GetPvsStats(postgres, ServerName, 0, null, ct)));
        var plainEmpty = await DarlingMcpPvsTools.GetPvsStats(postgres, ServerName, 0, "AgDb", ct);
        Assert.Equal("empty", StatusOf(plainEmpty));
        Assert.Null(NoteOf(plainEmpty));

        /* A fresh snapshot that holds AgDb as a secondary copy here, with no PVS row for it. */
        await SeedAgAsync(connection, "SECONDARY", TimeSpan.FromMinutes(2), ct, "AgDb");
        var expected = PvsSecondaryNote.WithoutRow(new[] { "AgDb" }, new[] { "master" });
        Assert.StartsWith("1 database has no row here:", expected, StringComparison.Ordinal);

        var rowsReply = await DarlingMcpPvsTools.GetPvsStats(postgres, ServerName, 0, null, ct);
        Assert.Equal("rows", StatusOf(rowsReply));
        Assert.Equal(expected, NoteOf(rowsReply));

        /* The filter picked only the secondary copy: no rows, an empty reply, and the note that explains the missing row. */
        var emptyReply = await DarlingMcpPvsTools.GetPvsStats(postgres, ServerName, 0, "AgDb", ct);
        Assert.Equal("empty", StatusOf(emptyReply));
        Assert.Equal(PvsSecondaryNote.WithoutRow(new[] { "AgDb" }, Array.Empty<string>(), new[] { "AgDb" }), NoteOf(emptyReply));

        /* A filter that leaves the secondary copy out says nothing about it. */
        Assert.Null(NoteOf(await DarlingMcpPvsTools.GetPvsStats(postgres, ServerName, 0, "master", ct)));
    }

    [Fact]
    public async Task TheReplicaRolesAreJudgedAtTheSnapshotsOwnTime_NotTodays()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), BaseSkip);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection) = await OpenAsync(baseConnectionString!, ct);
        await using var _s = scratch;
        await using var _c = connection;
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* The newest PVS snapshot is five hours old. AgDb was a secondary copy then, and the node has since become its primary (a newer
           snapshot with the local replica PRIMARY), so judged at today's roles it is no secondary copy and the note would vanish. */
        await SeedPvsAsync(connection, "master", TimeSpan.FromHours(5), ct);
        await SeedAgAsync(connection, "SECONDARY", TimeSpan.FromHours(5) + TimeSpan.FromMinutes(1), ct, "AgDb");
        await SeedAgAsync(connection, "PRIMARY", TimeSpan.FromMinutes(1), ct, "AgDb");

        var expected = PvsSecondaryNote.WithoutRow(new[] { "AgDb" }, new[] { "master" });
        Assert.Equal(expected, NoteOf(await DarlingMcpPvsTools.GetPvsStats(postgres, ServerName, 0, null, ct)));

        /* The empty reply a filter gets is anchored on the same snapshot. */
        Assert.Equal(
            PvsSecondaryNote.WithoutRow(new[] { "AgDb" }, Array.Empty<string>(), new[] { "AgDb" }),
            NoteOf(await DarlingMcpPvsTools.GetPvsStats(postgres, ServerName, 0, "AgDb", ct)));
    }
}
