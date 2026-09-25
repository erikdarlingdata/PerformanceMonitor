/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3691 part a2's SQL pin: <see cref="ViewerDataService.CollectionCaveatsSql"/> reads
/// <c>collect.analysis_collection_caveats</c> (V141, part a1) by server_id, ordered by family.
/// </summary>
public sealed class ViewerCollectionCaveatsSqlTests
{
    [Fact]
    public void CollectionCaveatsSql_ReadsTheV141Table_ByServerId_OrderedByFamily()
    {
        var sql = ViewerDataService.CollectionCaveatsSql;

        Assert.Contains("collect.analysis_collection_caveats", sql, StringComparison.Ordinal);
        Assert.Contains("family", sql, StringComparison.Ordinal);
        Assert.Contains("reason", sql, StringComparison.Ordinal);
        Assert.Contains("first_seen_utc", sql, StringComparison.Ordinal);
        Assert.Contains("last_seen_utc", sql, StringComparison.Ordinal);
        Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY family", sql, StringComparison.Ordinal);
    }
}

/// <summary>
/// Live round-trip for <see cref="ViewerDataService.GetCollectionCaveatsAsync"/>: seeds two caveat rows for
/// one server and one for another, and asserts the read returns exactly the first server's two, ordered by
/// family. Shares the serialized "live-postgres" collection; uses negative sentinel server_ids and cleans
/// up via <see cref="LiveStoreCleanup"/>.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerCollectionCaveatsLivePostgresTests
{
    private const int ServerId = -369101;
    private const int OtherServerId = -369102;

    [Fact]
    public async Task GetCollectionCaveatsAsync_ReturnsExactlyThisServersRows_OrderedByFamily()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live collection-caveats test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            /* Whole seconds: timestamptz keeps microseconds, and Windows' DateTime.UtcNow carries 100 ns ticks,
               so an unrounded value comes back one digit short and the exact-equality asserts below fail. */
            var firstSeen = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddDays(-2));
            var lastSeen = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddHours(-1));

            /* Two families for the target server, deliberately inserted out of alphabetical order, plus one
               row for a different server that must NOT come back. */
            await InsertCaveatAsync(connection, ServerId, "wait", "timeout", firstSeen, lastSeen);
            await InsertCaveatAsync(connection, ServerId, "vacuum", "error", firstSeen, lastSeen);
            await InsertCaveatAsync(connection, OtherServerId, "buffer", "missing_schema", firstSeen, lastSeen);

            var caveats = await viewer.GetCollectionCaveatsAsync(ServerId);

            Assert.Equal(new[] { "vacuum", "wait" }, caveats.Select(c => c.Family).ToArray());
            Assert.Equal("error", caveats[0].Reason);
            Assert.Equal("timeout", caveats[1].Reason);
            Assert.All(caveats, c => Assert.Equal(firstSeen, c.FirstSeenUtc));
            Assert.All(caveats, c => Assert.Equal(lastSeen, c.LastSeenUtc));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task InsertCaveatAsync(
        NpgsqlConnection connection, int serverId, string family, string reason, DateTime firstSeen, DateTime lastSeen)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO collect.analysis_collection_caveats (server_id, family, reason, first_seen_utc, last_seen_utc) " +
            "VALUES ($1, $2, $3, $4, $5)", connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = family });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = reason });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = firstSeen });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = lastSeen });
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "DELETE FROM collect.analysis_collection_caveats WHERE server_id IN ($1, $2)", connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = OtherServerId });
        await command.ExecuteNonQueryAsync(ct);
    }
}

/// <summary>
/// The connect-time schema gate for #3691 part a2: below V141, <see cref="ViewerDataService.GetCollectionCaveatsAsync"/>
/// returns an empty list rather than querying a table that does not exist yet on a lagging store.
/// </summary>
public sealed class ViewerCollectionCaveatsGateTests
{
    [Fact]
    public void MapProbedSchemaVersion_CollectionCaveatsAbsent_CapsBelow141()
    {
        var method = typeof(ViewerDataService)
            .GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var arity = method.GetParameters().Length;
        var collectionCaveatsOrdinal = Array.FindIndex(method.GetParameters(), p => p.Name == "hasCollectionCaveats");
        Assert.True(collectionCaveatsOrdinal >= 0, "hasCollectionCaveats is gone from the signature");

        /* Every sentinel BELOW hasCollectionCaveats true, it and everything from it up false: a store that
           has not reached V141 also has not reached whatever landed above it. Found BY NAME, not
           `arity - 1` -- V143 (#3953) appended its own parameter after this one, so the last ordinal is no
           longer V141's, and leaving it true would let the V143 arm answer 143 regardless of V141. */
        var throughV140 = Enumerable.Range(0, arity).Select(i => (object)(i < collectionCaveatsOrdinal)).ToArray();
        var result = (int)method.Invoke(null, throughV140)!;
        Assert.True(result < 141, $"expected below 141 with the V141 sentinel absent, got {result}");
    }

    /// <summary>#3691 part a2: the Collection Health tab is SQL Server-only, and the PostgreSQL-target analysis engine writes
    /// most caveat rows, so the PostgreSQL Overview tab carries the same section and loads it.</summary>
    [Fact]
    public void ThePostgresOverviewTab_CarriesAndLoadsTheCaveatsSection()
    {
        var xaml = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.xaml");
        var pgOverview = xaml[xaml.IndexOf("x:Name=\"PgOverviewTab\"", StringComparison.Ordinal)..];
        pgOverview = pgOverview[..pgOverview.IndexOf("</TabItem>", StringComparison.Ordinal)];
        Assert.Contains("x:Name=\"PgCollectionCaveatsExpander\"", pgOverview, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"PgCollectionCaveatsGrid\"", pgOverview, StringComparison.Ordinal);

        var code = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.Postgres.cs");
        var load = code[code.IndexOf("private async Task LoadPgOverviewAsync()", StringComparison.Ordinal)..];
        load = load[..load.IndexOf("private ", 10, StringComparison.Ordinal)];
        Assert.Contains("GetCollectionCaveatsAsync(_server.ServerId)", load, StringComparison.Ordinal);
        Assert.Contains("PgCollectionCaveatsExpander.Visibility", load, StringComparison.Ordinal);
    }
}
