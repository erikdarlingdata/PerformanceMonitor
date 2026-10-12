/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Review round 1 of PR #5709 (issue #5641, L1): the Runnable Queue card's top requests are read by CPU ahead of the
/// row cap, so 60 blocked requests (long elapsed, almost no CPU) cannot fill the 50-row page before the CPU-heavy ones.
/// Runs the real PostgreSQL read. #1776 own-store: this class registers one server of its own and removes its rows.
/// </summary>
[Collection("live-postgres")]
public sealed class RunnableQueueTopRequestsLiveTests
{
    private const string ServerName = "runnable-queue-top-requests-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheCpuOrderedRead_TakesTheHeaviestRequestsAheadOfTheCap_NullCpuLast()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live top-requests test.");
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var collected = DarlingMcpTestData.Naive(DateTime.UtcNow).AddMinutes(-1);
            const string sql = @"
INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, total_elapsed_time_ms, cpu_time_ms, reads, writes, query_hash, wait_type)
VALUES ($1, $2, $3, $4, $5, 'Db', 'SELECT 1', $6, $7, 1, 0, '0x01', 'LCK_M_X')";
            for (var i = 0; i < 60; i++)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct, sql,
                    CollectionIdGenerator.Next(), collected, ServerId, ServerName, 100 + i, 600_000L + i, 1L);
            }

            for (var i = 0; i < 5; i++)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct, sql,
                    CollectionIdGenerator.Next(), collected, ServerId, ServerName, 200 + i, 2_000L, 90_000L - (i * 1_000L));
            }

            /* A request with no CPU figure: PostgreSQL sorts NULL first on DESC, so the read must say NULLS LAST. */
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, total_elapsed_time_ms, cpu_time_ms, reads, writes, query_hash, wait_type)
VALUES ($1, $2, $3, $4, 300, 'Db', 'SELECT 1', 900000, NULL, 1, 0, '0x01', 'LCK_M_X')",
                CollectionIdGenerator.Next(), collected, ServerId, ServerName);

            var adapter = new DarlingAlertReadAdapter(postgres);
            var key = ServerId.ToString(CultureInfo.InvariantCulture);
            var byElapsed = await adapter.GetLongRunningQueriesAsync(
                key, 0, 50, false, false, false, false, false, Array.Empty<string>(), LongRunningQueryExclusions.None, cancellationToken: ct);
            var byCpu = await adapter.GetLongRunningQueriesAsync(
                key, 0, 50, false, false, false, false, false, Array.Empty<string>(), LongRunningQueryExclusions.None, orderByCpu: true, cancellationToken: ct);

            Assert.Equal(50, byElapsed.Sessions.Count);
            Assert.DoesNotContain(byElapsed.Sessions, q => q.SessionId is >= 200 and < 300);
            Assert.Equal(50, byCpu.Sessions.Count);
            Assert.Equal(new[] { 200, 201, 202, 203, 204 }, byCpu.Sessions.Take(5).Select(q => q.SessionId).ToArray());
            Assert.DoesNotContain(byCpu.Sessions.Take(50), q => q.SessionId == 300);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM query_snapshots WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
