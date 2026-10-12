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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The actual-plan command resolves a stored <c>query_snapshots</c> row by (server, collection time, session) and
/// the request's database. The resolved row must be in the database the request names, the way the query_stats and
/// Query Store resolvers match it: a request that names another database resolves nothing, and a matching request
/// still resolves the stored text. Runs the real resolver SQL and the real parameter binding over a seeded row.
/// Skips without <c>DARLING_TEST_PG</c>, like its siblings.
/// </summary>
[Collection("live-postgres")]
public sealed class ActualPlanSnapshotResolveLiveTests
{
    private const string ServerName = "darling-actual-plan-snapshot-resolve-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const int SessionId = 61;
    private const int NullDbSessionId = 62;
    private const string StoredDb = "StoredDb";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task SnapshotResolve_MatchesTheRequestsDatabase_AndNothingElse()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live actual-plan snapshot resolve test.");
        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;
        try
        {
            using var connection = new NpgsqlConnection(cs);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await DeleteRowsAsync(connection, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var captured = DarlingMcpTestData.Naive(DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-10));
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text) VALUES ($1,$2,$3,$4,$5,$6,$7)",
                CollectionIdGenerator.Next(), captured, ServerId, ServerName, SessionId, StoredDb, "SELECT 1 FROM dbo.Posts");
            // A snapshot whose session had no database context stores NULL there (DB_NAME() returns NULL when the
            // login cannot see the database).
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text) VALUES ($1,$2,$3,$4,$5,$6,$7)",
                CollectionIdGenerator.Next(), captured, ServerId, ServerName, NullDbSessionId, null, "SELECT 2 FROM dbo.Votes");

            async Task<string?> ResolveAsync(string? requestDatabase, int sessionId = SessionId)
            {
                var request = new ActualPlanRequest(null, null, captured, sessionId, requestDatabase);
                await using var command = new NpgsqlCommand(DarlingWorker.ResolveStoredSnapshotForActualPlanSql, connection);
                DarlingWorker.BindActualPlanResolveParameters(command, ServerId, request);
                await using var reader = await command.ExecuteReaderAsync(ct);
                return await reader.ReadAsync(ct) ? reader.GetString(0) : null;
            }

            Assert.Equal("SELECT 1 FROM dbo.Posts", await ResolveAsync(StoredDb));
            Assert.Null(await ResolveAsync("OtherDb"));
            Assert.Null(await ResolveAsync(StoredDb.ToLowerInvariant()));
            Assert.Null(await ResolveAsync(null));

            // A row stored with no database matches a request with no database, and only that request.
            Assert.Equal("SELECT 2 FROM dbo.Votes", await ResolveAsync(null, NullDbSessionId));
            Assert.Null(await ResolveAsync(StoredDb, NullDbSessionId));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM query_snapshots WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
