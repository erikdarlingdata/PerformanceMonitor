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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test seeds its own scratch database, so nothing here shares rows with another test. */
/// <summary>
/// #5558: the FinOps Recommendations list leaves the per-database findings of a database this node holds only as a
/// secondary copy in an Availability Group to the primary. Over the golden store, server A has an idle database
/// (ArchiveA), two dev/test names (app_dev_a, qa1_a), two TDE databases (SalesA, OrdersA), a 2 GB uncompressed index in
/// SalesA, and a low-latency database (OrdersA). Each test seeds the stored Availability Group snapshots for server A,
/// reads through the storage composer, the viewer and the MCP tool, and compares with the same read over no snapshots.
/// </summary>
public sealed class FinOpsRecommendationsSecondaryReplicaLiveTests
{
    private const int TimeoutSeconds = 30;
    private const string BaseSkip = "Set DARLING_TEST_PG to a Postgres connection string to run the live FinOps secondary replica test.";

    private static int ServerId => FinOpsRecommendationsGoldenLiveTests.ServerIdA;

    private static async Task<ScratchPostgres> SeedStoreAsync(string baseConnectionString, CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await FinOpsRecommendationsGoldenLiveTests.SeedAsync(connection, ct);
        return scratch;
    }

    /// <summary>One AG whose local replica has <paramref name="localRole"/>, holding <paramref name="databases"/>; snapshots taken <paramref name="age"/> ago.</summary>
    private static async Task SeedAgAsync(string connectionString, string localRole, TimeSpan age, CancellationToken ct, params string[] databases)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        var at = DateTime.SpecifyKind(DateTime.UtcNow - age, DateTimeKind.Unspecified);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO ag_replica_states (collection_id, collection_time, server_id, server_name, ag_name, replica_server_name, role_desc, is_local) VALUES ($1,$2,$3,$4,'AG1','NODE1',$5,TRUE)",
            CollectionIdGenerator.Next(), at, ServerId, FinOpsRecommendationsGoldenLiveTests.ServerNameA, localRole);
        foreach (var database in databases)
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO ag_database_replica_states (collection_id, collection_time, server_id, server_name, ag_name, database_name, replica_server_name, is_local) VALUES ($1,$2,$3,$4,'AG1',$5,'NODE1',TRUE)",
                CollectionIdGenerator.Next(), at, ServerId, FinOpsRecommendationsGoldenLiveTests.ServerNameA, database);
    }

    private static FinOpsRecommendation? Find(IEnumerable<FinOpsRecommendation> rows, string findingContains) =>
        rows.FirstOrDefault(r => r.Finding.Contains(findingContains, StringComparison.Ordinal));

    [Fact]
    public async Task ASecondaryCopy_IsDroppedFromIdleDevTestTdeAndCompression_ButKeptInLowIoLatency()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), BaseSkip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedStoreAsync(baseConnectionString!, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var baseline = await DarlingFinOpsRecommendationsReader.GetRecommendationsAsync(dataSource, ServerId, 1000m, TimeoutSeconds, cancellationToken: ct);
        Assert.NotNull(Find(baseline, "idle database"));
        Assert.Contains("2 possible dev/test", Find(baseline, "possible dev/test")!.Finding, StringComparison.Ordinal);
        Assert.Contains("SalesA", Find(baseline, "TDE in use")!.Detail, StringComparison.Ordinal);
        Assert.Contains("OrdersA", Find(baseline, "TDE in use")!.Detail, StringComparison.Ordinal);
        Assert.NotNull(Find(baseline, "uncompressed object"));
        Assert.Contains("OrdersA", Find(baseline, "low IO latency")!.Detail, StringComparison.Ordinal);

        /* ArchiveA (idle), app_dev_a (dev/test) and SalesA (TDE and the 2 GB index) are secondary copies. Names are matched
           exactly (#5558 round 2): the set and the stored names both come from sys.databases.name, so the casing is shared. */
        var secondary = new HashSet<string>(StringComparer.Ordinal) { "ArchiveA", "app_dev_a", "SalesA" };
        var rows = await DarlingFinOpsRecommendationsReader.GetRecommendationsAsync(
            dataSource, ServerId, 1000m, TimeoutSeconds, secondaryDatabases: secondary, cancellationToken: ct);

        Assert.Null(Find(rows, "idle database"));
        Assert.Contains("1 possible dev/test", Find(rows, "possible dev/test")!.Finding, StringComparison.Ordinal);
        Assert.DoesNotContain("app_dev_a", Find(rows, "possible dev/test")!.Detail, StringComparison.Ordinal);
        var tde = Find(rows, "TDE in use")!;
        Assert.Contains("OrdersA", tde.Detail, StringComparison.Ordinal);
        Assert.DoesNotContain("SalesA", tde.Detail, StringComparison.Ordinal);
        Assert.Null(Find(rows, "uncompressed object"));
        /* Low IO latency is unchanged, and so is every finding that is not about one database. */
        Assert.Equal(Find(baseline, "low IO latency")!.Detail, Find(rows, "low IO latency")!.Detail);
        static bool PerDatabase(FinOpsRecommendation r) =>
            r.Finding.Contains("idle database", StringComparison.Ordinal) || r.Finding.Contains("possible dev/test", StringComparison.Ordinal)
            || r.Finding.Contains("TDE in use", StringComparison.Ordinal) || r.Finding.Contains("uncompressed object", StringComparison.Ordinal);
        Assert.Equal(
            baseline.Where(r => !PerDatabase(r)).Select(r => (r.Finding, r.Detail)).ToArray(),
            rows.Where(r => !PerDatabase(r)).Select(r => (r.Finding, r.Detail)).ToArray());
    }

    [Fact]
    public async Task EveryTdeDatabaseASecondary_SaysNothingAboutTdeAndNeverClaimsNoEnterpriseFeatures()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), BaseSkip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedStoreAsync(baseConnectionString!, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var rows = await DarlingFinOpsRecommendationsReader.GetRecommendationsAsync(
            dataSource, ServerId, 1000m, TimeoutSeconds, secondaryDatabases: new[] { "SalesA", "OrdersA" }, cancellationToken: ct);

        Assert.DoesNotContain(rows, r => r.Category == "Licensing");
        Assert.NotNull(Find(rows, "low IO latency"));
    }

    [Fact]
    public async Task NullOrEmptySet_ChangesNothing()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), BaseSkip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedStoreAsync(baseConnectionString!, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var baseline = await DarlingFinOpsRecommendationsReader.GetRecommendationsAsync(dataSource, ServerId, 1000m, TimeoutSeconds, cancellationToken: ct);
        var empty = await DarlingFinOpsRecommendationsReader.GetRecommendationsAsync(
            dataSource, ServerId, 1000m, TimeoutSeconds, secondaryDatabases: new HashSet<string>(), cancellationToken: ct);

        Assert.Equal(baseline.Select(r => (r.Finding, r.Detail)).ToArray(), empty.Select(r => (r.Finding, r.Detail)).ToArray());
    }

    [Fact]
    public async Task TheViewerAndTheMcpTool_ReadTheStoredSnapshots_AndFailOpenWhenTheyAreStaleOrTheNodeIsPrimary()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), BaseSkip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedStoreAsync(baseConnectionString!, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using var viewer = new ViewerDataService(scratch.ConnectionString);

        /* No snapshots: no note, and the MCP payload carries the field as null. */
        var (plainRows, plainNote) = await viewer.GetRecommendationsWithNoteAsync(ServerId, 1000m, ct);
        Assert.Null(plainNote);
        var plainRead = await FinOpsAsync(dataSource, ct);
        Assert.Equal(System.Text.Json.JsonValueKind.Null, plainRead.GetProperty("secondary_replica_note").ValueKind);

        /* A stale secondary snapshot (a collector that stopped hours ago) vouches for nothing. */
        await SeedAgAsync(scratch.ConnectionString, "SECONDARY", TimeSpan.FromHours(6), ct, "ArchiveA");
        var (staleRows, staleNote) = await viewer.GetRecommendationsWithNoteAsync(ServerId, 1000m, ct);
        Assert.Null(staleNote);
        Assert.Equal(plainRows.Select(r => r.Finding).ToArray(), staleRows.Select(r => r.Finding).ToArray());

        /* A fresh secondary snapshot of ArchiveA drops its idle finding on both surfaces and writes the note. */
        await SeedAgAsync(scratch.ConnectionString, "SECONDARY", TimeSpan.FromMinutes(2), ct, "ArchiveA");
        var (rows, note) = await viewer.GetRecommendationsWithNoteAsync(ServerId, 1000m, ct);
        Assert.Equal(AgReplicaScope.SkippedNote(1), note);
        Assert.DoesNotContain(rows, r => r.Finding.Contains("idle database", StringComparison.Ordinal));
        Assert.Contains(plainRows, r => r.Finding.Contains("idle database", StringComparison.Ordinal));

        var payload = await FinOpsAsync(dataSource, ct);
        Assert.Equal(AgReplicaScope.SkippedNote(1), payload.GetProperty("secondary_replica_note").GetString());
        Assert.DoesNotContain(payload.GetProperty("recommendations").EnumerateArray(),
            r => r.GetProperty("finding").GetString()!.Contains("idle database", StringComparison.Ordinal));
    }

    [Fact]
    public async Task OnThePrimary_NothingIsSkipped_AndThereIsNoNote()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), BaseSkip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedStoreAsync(baseConnectionString!, ct);
        await using var viewer = new ViewerDataService(scratch.ConnectionString);

        var (plainRows, _) = await viewer.GetRecommendationsWithNoteAsync(ServerId, 1000m, ct);
        await SeedAgAsync(scratch.ConnectionString, "PRIMARY", TimeSpan.FromMinutes(2), ct, "ArchiveA", "app_dev_a", "SalesA");
        var (rows, note) = await viewer.GetRecommendationsWithNoteAsync(ServerId, 1000m, ct);

        Assert.Null(note);
        Assert.Equal(plainRows.Select(r => (r.Finding, r.Detail)).ToArray(), rows.Select(r => (r.Finding, r.Detail)).ToArray());
    }

    private static async Task<JsonElement> FinOpsAsync(NpgsqlDataSource dataSource, CancellationToken ct)
    {
        var json = await DarlingMcpFinOpsRecommendationsTools.GetFinOpsRecommendations(
            dataSource, FinOpsRecommendationsGoldenLiveTests.ServerNameA, ct);
        return JsonDocument.Parse(json).RootElement.Clone();
    }
}
