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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The gated live round-trip for #3691 part a1's store (DARLING_TEST_PG): <c>collect.analysis_collection_caveats</c>
/// via <see cref="CollectionCaveatStore"/>. The pure rung tests already pin the DDL and the migration ladder;
/// this proves the upsert/delete/prune SQL against a real Postgres. A random synthetic <c>server_id</c> per
/// test run, rather than try/finally teardown: <see cref="CollectionCaveatStore.ApplyPassAsync"/>'s own
/// contract is that a clean pass with nothing unread deletes every row for that server, so the LAST
/// assertion in every scenario below leaves its own server's rows at the state the next scenario needs —
/// there is nothing left over to tear down, and a synthetic id keeps two servers' rows from ever colliding
/// even if a scenario fails mid-way.
/// </summary>
[Collection("live-postgres")]
public sealed class CollectionCaveatStoreLiveTests
{
    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (owner/superuser) to run the collection-caveats live tests.");
        return connectionString!;
    }

    private static async Task<NpgsqlDataSource> OpenMigratedDataSourceAsync(string connectionString, System.Threading.CancellationToken ct)
    {
        await using (var migrate = new NpgsqlConnection(connectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, ct);
        }

        var dataSourceConnectionString = new NpgsqlConnectionStringBuilder(connectionString)
        {
            SearchPath = "collect,config,public",
        }.ConnectionString;

        return NpgsqlDataSource.Create(dataSourceConnectionString);
    }

    private static async Task<List<(string Family, string Reason)>> ReadRowsAsync(
        NpgsqlDataSource dataSource, int serverId, System.Threading.CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var command = new NpgsqlCommand(
            "SELECT family, reason FROM collect.analysis_collection_caveats WHERE server_id = $1 ORDER BY family", connection);
        command.Parameters.AddWithValue(serverId);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var rows = new List<(string, string)>();
        while (await reader.ReadAsync(ct))
        {
            rows.Add((reader.GetString(0), reader.GetString(1)));
        }

        return rows;
    }

    [Fact]
    public async Task ApplyPassAsync_UpsertsUnreadFamilies_ThenClearsWhatIsRead_KeepingFirstSeen_AndAdvancingLastSeen()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var dataSource = await OpenMigratedDataSourceAsync(connectionString, ct);

        var serverId = Random.Shared.Next(1_000_000, 2_000_000);
        var otherServerId = serverId + 1;

        var pass1 = new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);
        var unread1 = new[]
        {
            new CollectionCaveatStore.UnreadFamily("wait_stats", "timeout"),
            new CollectionCaveatStore.UnreadFamily("pg_log_events", "missing_schema"),
        };
        await CollectionCaveatStore.ApplyPassAsync(dataSource, serverId, unread1, pass1, null, ct);
        await CollectionCaveatStore.ApplyPassAsync(dataSource, otherServerId, unread1, pass1, null, ct);

        var rowsAfterPass1 = await ReadRowsAsync(dataSource, serverId, ct);
        Assert.Equal(2, rowsAfterPass1.Count);
        Assert.Contains(("pg_log_events", "missing_schema"), rowsAfterPass1);
        Assert.Contains(("wait_stats", "timeout"), rowsAfterPass1);

        /* Pass 2: one family still unread (with a DIFFERENT reason — the newest reason always wins), the
           other read successfully — one row cleared, one row kept, first_seen unchanged, last_seen advanced. */
        var pass2 = pass1.AddHours(1);
        var unread2 = new[] { new CollectionCaveatStore.UnreadFamily("wait_stats", "cancelled") };
        await CollectionCaveatStore.ApplyPassAsync(dataSource, serverId, unread2, pass2, null, ct);

        var rowsAfterPass2 = await ReadRowsAsync(dataSource, serverId, ct);
        Assert.Single(rowsAfterPass2);
        Assert.Equal(("wait_stats", "cancelled"), rowsAfterPass2[0]);

        await using (var connection = await dataSource.OpenConnectionAsync(ct))
        await using (var command = new NpgsqlCommand(
            "SELECT first_seen_utc, last_seen_utc FROM collect.analysis_collection_caveats WHERE server_id = $1 AND family = 'wait_stats'",
            connection))
        {
            command.Parameters.AddWithValue(serverId);
            await using var reader = await command.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));
            var firstSeen = reader.GetDateTime(0);
            var lastSeen = reader.GetDateTime(1);
            Assert.Equal(pass1, DateTime.SpecifyKind(firstSeen, DateTimeKind.Utc));
            Assert.Equal(pass2, DateTime.SpecifyKind(lastSeen, DateTimeKind.Utc));
        }

        /* The other server's rows are untouched by anything done to this one. */
        var otherRows = await ReadRowsAsync(dataSource, otherServerId, ct);
        Assert.Equal(2, otherRows.Count);

        /* Pass 3: nothing unread — every row for THIS server clears; the other server's rows stand. */
        var pass3 = pass2.AddHours(1);
        await CollectionCaveatStore.ApplyPassAsync(dataSource, serverId, Array.Empty<CollectionCaveatStore.UnreadFamily>(), pass3, null, ct);

        Assert.Empty(await ReadRowsAsync(dataSource, serverId, ct));
        Assert.Equal(2, (await ReadRowsAsync(dataSource, otherServerId, ct)).Count);

        /* Clean up the other server's rows too, via the same clean-pass path under test. */
        await CollectionCaveatStore.ApplyPassAsync(dataSource, otherServerId, Array.Empty<CollectionCaveatStore.UnreadFamily>(), pass3, null, ct);
        Assert.Empty(await ReadRowsAsync(dataSource, otherServerId, ct));
    }

    [Fact]
    public async Task PruneAsync_RemovesOnlyRowsOlderThanTheCutoff()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var dataSource = await OpenMigratedDataSourceAsync(connectionString, ct);

        var staleServerId = Random.Shared.Next(2_000_000, 3_000_000);
        var freshServerId = staleServerId + 1;

        var staleAt = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var freshAt = DateTime.UtcNow;

        await CollectionCaveatStore.ApplyPassAsync(
            dataSource, staleServerId, new[] { new CollectionCaveatStore.UnreadFamily("wait_stats", "timeout") }, staleAt, null, ct);
        await CollectionCaveatStore.ApplyPassAsync(
            dataSource, freshServerId, new[] { new CollectionCaveatStore.UnreadFamily("wait_stats", "timeout") }, freshAt, null, ct);

        var cutoff = DateTime.UtcNow.AddDays(-CollectionCaveatStore.PruneAfterDays);
        var deleted = await CollectionCaveatStore.PruneAsync(dataSource, cutoff, null, ct);

        Assert.True(deleted >= 1);
        Assert.Empty(await ReadRowsAsync(dataSource, staleServerId, ct));
        Assert.Single(await ReadRowsAsync(dataSource, freshServerId, ct));

        /* Teardown the fresh row through the same clean-pass path the other test relies on. */
        await CollectionCaveatStore.ApplyPassAsync(
            dataSource, freshServerId, Array.Empty<CollectionCaveatStore.UnreadFamily>(), DateTime.UtcNow, null, ct);
        Assert.Empty(await ReadRowsAsync(dataSource, freshServerId, ct));
    }

    /// <summary>
    /// A closed data source is exactly the "store fault" this method exists to survive — never throws, and
    /// swallows into a Warning log instead. Disposing the data source first, then calling into it, is the
    /// simplest reliable way to force Npgsql to fail the connection attempt.
    /// </summary>
    [Fact]
    public async Task ApplyPassAsync_OnAClosedDataSource_DoesNotThrow()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;

        var dataSource = NpgsqlDataSource.Create(connectionString);
        await dataSource.DisposeAsync();

        var serverId = Random.Shared.Next(3_000_000, 4_000_000);
        await CollectionCaveatStore.ApplyPassAsync(
            dataSource, serverId, new[] { new CollectionCaveatStore.UnreadFamily("wait_stats", "timeout") }, DateTime.UtcNow, null, ct);

        var prunedFromClosedSource = await CollectionCaveatStore.PruneAsync(dataSource, DateTime.UtcNow, null, ct);
        Assert.Equal(0, prunedFromClosedSource);
    }
}
