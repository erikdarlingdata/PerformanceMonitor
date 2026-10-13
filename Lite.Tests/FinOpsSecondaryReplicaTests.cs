/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5558: the FinOps Recommendations sub-tab leaves the per-database findings of a database this node holds only as a
/// secondary copy in an Availability Group to the primary. Idle databases are the rule the seeded store can drive end to
/// end (the dev/test, TDE and compression rules read the monitored server live, so their filter is pinned on the helper
/// they all call). The low IO latency rule keeps every database. A primary, stale snapshots and no snapshots skip nothing.
/// </summary>
public sealed class FinOpsSecondaryReplicaTests : IClassFixture<SharedDuckDbFixture>
{
    private static long s_id = -55_810_000;

    private readonly DuckDbInitializer _duckDb;

    public FinOpsSecondaryReplicaTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    private static string Idle(IEnumerable<RecommendationRow> rows) =>
        rows.FirstOrDefault(r => r.Category == "Databases" && r.Finding.Contains("idle", StringComparison.OrdinalIgnoreCase))?.Detail ?? "";

    /// <summary>Seeds the node as the local replica of AG1 with the given role and the given databases in it, at <paramref name="at"/>.</summary>
    private async Task SeedAgAsync(DateTime at, string localRole, params string[] databases)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        const string Replica = "INSERT INTO ag_replica_states (collection_id, collection_time, server_id, server_name, ag_name, replica_server_name, role_desc, is_local) VALUES ($1,$2,$3,'n',$4,$5,$6,$7)";
        await ExecAsync(connection, Replica, Interlocked.Decrement(ref s_id), at, TestDataSeeder.TestServerId, "AG1", "NODE1", localRole, true);
        await ExecAsync(connection, Replica, Interlocked.Decrement(ref s_id), at, TestDataSeeder.TestServerId, "AG1", "NODE2",
            localRole == "SECONDARY" ? "PRIMARY" : "SECONDARY", false);
        const string Db = "INSERT INTO ag_database_replica_states (collection_id, collection_time, server_id, server_name, ag_name, database_name, replica_server_name, is_local) VALUES ($1,$2,$3,'n',$4,$5,$6,$7)";
        foreach (var database in databases)
            await ExecAsync(connection, Db, Interlocked.Decrement(ref s_id), at, TestDataSeeder.TestServerId, "AG1", database, "NODE1", true);
    }

    private static async Task ExecAsync(DuckDBConnection connection, string sql, params object[] values)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        foreach (var v in values) cmd.Parameters.Add(new DuckDBParameter { Value = v });
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task<(List<RecommendationRow> Rows, string? Note)> RunAsync(Func<TestDataSeeder, Task> seed, Func<Task>? seedAg = null)
    {
        using var seeder = new TestDataSeeder(_duckDb);
        await seed(seeder);
        if (seedAg != null) await seedAg();
        var result = await new LocalDataService(_duckDb).GetRecommendationsWithNoteAsync(TestDataSeeder.TestServerId, "", "", 10000m);
        return (result.Rows, result.SkippedNote);
    }

    [Fact]
    public async Task Baseline_WithNoAgData_BothIdleDatabasesAreListed_AndThereIsNoNote()
    {
        var (rows, note) = await RunAsync(s => s.SeedIdleDatabasesAsync());

        var detail = Idle(rows);
        Assert.Contains("OldReportsDB", detail, StringComparison.Ordinal);
        Assert.Contains("ArchiveDB", detail, StringComparison.Ordinal);
        Assert.Null(note);
    }

    [Fact]
    public async Task ASecondaryCopy_IsDroppedFromTheIdleList_AndTheNoteSaysSo()
    {
        var (rows, note) = await RunAsync(s => s.SeedIdleDatabasesAsync(),
            () => SeedAgAsync(DateTime.UtcNow.AddMinutes(-2), "SECONDARY", "OldReportsDB"));

        var detail = Idle(rows);
        Assert.DoesNotContain("OldReportsDB", detail, StringComparison.Ordinal);
        Assert.Contains("ArchiveDB", detail, StringComparison.Ordinal);
        var finding = Assert.Single(rows, r => r.Category == "Databases" && r.Finding.Contains("idle", StringComparison.OrdinalIgnoreCase));
        Assert.StartsWith("1 idle database(s)", finding.Finding, StringComparison.Ordinal);
        Assert.Equal(AgReplicaScope.SkippedNote(1), note);
    }

    [Fact]
    public async Task EveryIdleDatabaseASecondary_LeavesNoIdleFinding()
    {
        var (rows, note) = await RunAsync(s => s.SeedIdleDatabasesAsync(),
            () => SeedAgAsync(DateTime.UtcNow.AddMinutes(-2), "SECONDARY", "OldReportsDB", "ArchiveDB"));

        Assert.DoesNotContain(rows, r => r.Category == "Databases" && r.Finding.Contains("idle", StringComparison.OrdinalIgnoreCase));
        Assert.Equal(AgReplicaScope.SkippedNote(2), note);
    }

    [Fact]
    public async Task OnThePrimary_EveryIdleDatabaseIsKept_AndThereIsNoNote()
    {
        var (rows, note) = await RunAsync(s => s.SeedIdleDatabasesAsync(),
            () => SeedAgAsync(DateTime.UtcNow.AddMinutes(-2), "PRIMARY", "OldReportsDB", "ArchiveDB"));

        var detail = Idle(rows);
        Assert.Contains("OldReportsDB", detail, StringComparison.Ordinal);
        Assert.Contains("ArchiveDB", detail, StringComparison.Ordinal);
        Assert.Null(note);
    }

    [Fact]
    public async Task StaleAgData_FailsOpen_AndSkipsNothing()
    {
        var (rows, note) = await RunAsync(s => s.SeedIdleDatabasesAsync(),
            () => SeedAgAsync(DateTime.UtcNow.AddHours(-5), "SECONDARY", "OldReportsDB"));

        Assert.Contains("OldReportsDB", Idle(rows), StringComparison.Ordinal);
        Assert.Null(note);
    }

    [Fact]
    public async Task ASecondaryCopy_StaysInTheLowIoLatencyList()
    {
        var (rows, note) = await RunAsync(s => s.SeedLowIoLatencyAsync(),
            () => SeedAgAsync(DateTime.UtcNow.AddMinutes(-2), "SECONDARY", "UserDB"));

        var storage = Assert.Single(rows, r => r.Category == "Storage");
        Assert.Contains("UserDB", storage.Detail, StringComparison.Ordinal);
        /* The note still tells the reader the other findings left the database out. */
        Assert.Equal(AgReplicaScope.SkippedNote(1), note);
    }

    /// <summary>Seeds one <c>database_states</c> snapshot naming <paramref name="databases"/>, <paramref name="age"/> old.</summary>
    private async Task SeedSnapshotAsync(TimeSpan age, params string[] databases)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        var at = DateTime.UtcNow - age;
        await ExecAsync(connection, "DELETE FROM database_states WHERE server_id = $1", TestDataSeeder.TestServerId);
        const string Sql = "INSERT INTO database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby) VALUES ($1,$2,$3,'n',$4,5,'ONLINE',false)";
        foreach (var database in databases)
            await ExecAsync(connection, Sql, Interlocked.Decrement(ref s_id), at, TestDataSeeder.TestServerId, database);
    }

    /// <summary>Review M3: Darling leaves a dropped database out of its idle rule; Lite read DuckDB history with no current-database
    /// filter and still listed it, with a cost-saving estimate.</summary>
    [Fact]
    public async Task ADroppedDatabase_LeavesTheIdleList_ButAnUnknownSetKeepsEverything()
    {
        var (rows, _) = await RunAsync(s => s.SeedIdleDatabasesAsync(),
            () => SeedSnapshotAsync(TimeSpan.FromMinutes(5), "ArchiveDB"));

        var detail = Idle(rows);
        Assert.DoesNotContain("OldReportsDB", detail, StringComparison.Ordinal);
        Assert.Contains("ArchiveDB", detail, StringComparison.Ordinal);
        var finding = Assert.Single(rows, r => r.Category == "Databases" && r.Finding.Contains("idle", StringComparison.OrdinalIgnoreCase));
        Assert.StartsWith("1 idle database(s)", finding.Finding, StringComparison.Ordinal);

        /* A snapshot older than the horizon is unknown: nothing is filtered. */
        var (stale, _) = await RunAsync(s => s.SeedIdleDatabasesAsync(),
            () => SeedSnapshotAsync(LocalDataService.CurrentDatabasesHorizon + TimeSpan.FromHours(1), "ArchiveDB"));
        Assert.Contains("OldReportsDB", Idle(stale), StringComparison.Ordinal);
    }

    /// <summary>Review M3: the storage-tier rule reads v_file_io_stats history, where a dropped database keeps its rows.</summary>
    [Fact]
    public async Task ADroppedDatabase_LeavesTheLowIoLatencyList()
    {
        var (gone, _) = await RunAsync(s => s.SeedLowIoLatencyAsync(),
            () => SeedSnapshotAsync(TimeSpan.FromMinutes(5), "SomeOtherDb"));
        Assert.DoesNotContain(gone, r => r.Category == "Storage");

        var (kept, _) = await RunAsync(s => s.SeedLowIoLatencyAsync(),
            () => SeedSnapshotAsync(TimeSpan.FromMinutes(5), "UserDB", "SomeOtherDb"));
        Assert.Contains("UserDB", Assert.Single(kept, r => r.Category == "Storage").Detail, StringComparison.Ordinal);
    }

    [Fact]
    public void TheFilterHelper_DropsOnlyTheSecondaries_ThatTheDevTestTdeAndCompressionRulesShare()
    {
        var secondary = new HashSet<string>(StringComparer.Ordinal) { "SalesDev" };

        /* Exact names (#5558 round 2): the set and the lists come from the same catalog, so a name that differs only by
           case is a different database on a case-sensitive server and is kept. */
        Assert.Equal(["salesdev", "OrdersTest", "qa_one"], AgReplicaScope.WithoutSecondaries(["salesdev", "SalesDev", "OrdersTest", "qa_one"], secondary));
        Assert.True(AgReplicaScope.IsSkipped(secondary, "SalesDev"));
        Assert.False(AgReplicaScope.IsSkipped(secondary, "SALESDEV"));
        Assert.False(AgReplicaScope.IsSkipped(secondary, "OrdersTest"));
        /* Fail open: nothing, null and blank skip nothing. */
        Assert.Equal(["a", "b"], AgReplicaScope.WithoutSecondaries(["a", "b"], null));
        Assert.Equal(["a", "b"], AgReplicaScope.WithoutSecondaries(["a", "b"], new HashSet<string>()));
        Assert.False(AgReplicaScope.IsSkipped(secondary, null));
        Assert.False(AgReplicaScope.IsSkipped(secondary, " "));
    }
}
