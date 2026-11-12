/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8092: <c>pg_locks</c> is cluster-wide and the collector resolves a relation's name only in the database it is
/// connected to, so a lock on a relation in another database is stored with an oid and a null name. The reader groups
/// on both, and the tool's row carries the oid beside the name: three such relations are three rows a caller can tell
/// apart and resolve (<c>SELECT oid::regclass</c> in the database the row names), not three rows identical but for
/// <c>captures</c>.
/// </summary>
[Collection("live-postgres")]
public sealed class PgLockStatsRelationOidLiveTests
{
    private const int ServerId = -8092301;
    private const string ServerName = "pg-lock-stats-relation-oid";

    private static readonly DateTime WindowStart = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime WindowEnd = WindowStart.AddHours(1);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ThreeUnnamedRelations_AreThreeRowsEachCarryingItsOid_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the lock-stats relation oid test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        var at = DarlingMcpTestData.Naive(WindowStart.AddMinutes(30));
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
VALUES (1, $1, $2, 'pg_lock_stats', $3, 5, 'SUCCESS', 3)", ServerId, ServerName, at);

        /* Three relations in a database other than the collector's: same type, mode and grant, names unresolved. */
        foreach (var oid in new long[] { 16385, 16386, 16387 })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_lock_stats (collection_id, collection_time, server_id, server_name, database_name, lock_type, mode, granted, relation_oid, relation_name, backend_count, oldest_wait_ms)
VALUES ($1, $2, $3, $4, 'otherdb', 'relation', 'RowExclusiveLock', true, $1, NULL, 1, NULL)",
                oid, at, ServerId, ServerName);
        }

        await using var dataSource = StoreDataSource(scratch);
        var json = await DarlingMcpPgServerStateTools.GetPgLockStats(
            dataSource, ServerName, hours_back: 1, limit: 25, as_of: WebDataStartNote.FormatWindowEnd(WindowEnd), cancellationToken: ct);

        Assert.False(McpHelpers.IsErrorEnvelope(json), $"tool returned an error: {json}");
        using var doc = JsonDocument.Parse(json);
        var locks = doc.RootElement.GetProperty("locks").EnumerateArray().ToList();
        Assert.Equal(3, locks.Count);

        var oids = locks.Select(l =>
        {
            Assert.True(l.TryGetProperty("relation_oid", out var oid), $"the row carries no relation_oid: {l}");
            Assert.Equal(JsonValueKind.Null, l.GetProperty("relation_name").ValueKind);
            Assert.Equal("otherdb", l.GetProperty("database_name").GetString());
            return oid.GetInt64();
        }).OrderBy(o => o).ToList();
        Assert.Equal([16385L, 16386L, 16387L], oids);
    }

    /// <summary>
    /// A seed session on the scratch store with the store's <c>search_path</c> SET, never inherited (the #8013 trap): a
    /// cloned scratch database does not carry the template's database-default <c>search_path</c>, so on the bare table
    /// names the seeds use resolve to nothing (42P01) without it.
    /// </summary>
    private static async Task<NpgsqlConnection> OpenStoreAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var setPath = new NpgsqlCommand("SET search_path = " + PgSchemaGenerator.SearchPath, connection);
        await setPath.ExecuteNonQueryAsync(ct);
        return connection;
    }

    private static NpgsqlDataSource StoreDataSource(ScratchPostgres scratch) =>
        NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath });
}
