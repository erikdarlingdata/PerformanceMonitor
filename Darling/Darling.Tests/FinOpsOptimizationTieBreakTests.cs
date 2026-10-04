/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/* #1776 own-store: the fixture seeds its own scratch database, so nothing here shares rows with another test. */
/// <summary>
/// The Optimization tab's most-expensive-queries read takes a top N by total CPU. Statements that tie on the total must
/// not be cut by the engine's whim, so the order ends with every grouping key. Lite carries the same statement.
/// </summary>
public sealed class FinOpsOptimizationTieBreakTests
{
    private const string TieBreak = "ORDER BY SUM(delta_worker_time) DESC, database_name, sql_handle, query_text";
    private const string ServerName = "darling-finops-opt-tiebreak-a";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    [Fact]
    public void DarlingStatement_EndsItsOrderWithEveryGroupingKey()
    {
        var sql = DarlingFinOpsOptimizationReader.ExpensiveQueriesSql;
        Assert.Contains("GROUP BY\n    database_name,\n    sql_handle,\n    query_text\n" + TieBreak + "\nLIMIT $3",
            sql.ReplaceLineEndings("\n"), StringComparison.Ordinal);
    }

    [Fact]
    public void LiteStatement_EndsItsOrderWithEveryGroupingKey()
    {
        var lite = ReadRepoFile("Lite", "Services", "LocalDataService.FinOps.Workload.cs").ReplaceLineEndings("\n");
        var start = lite.IndexOf("GetExpensiveQueriesAsync(int serverId", StringComparison.Ordinal);
        Assert.True(start >= 0);
        var end = lite.IndexOf("LIMIT $3", start, StringComparison.Ordinal);
        Assert.True(end > start);
        var body = lite[start..end];
        Assert.Contains("GROUP BY\n    database_name,\n    sql_handle,\n    query_text\n" + TieBreak + "\n", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TiedStatements_AtTheCut_SurviveInKeyOrder()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live tie-break test.");
        var ct = TestContext.Current.CancellationToken;
        {
            await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
            await using (var c = new NpgsqlConnection(scratch.ConnectionString))
            {
                await c.OpenAsync(ct);
                await PgMigrations.MigrateAsync(c, ct);
                await DarlingMcpTestData.RegisterServerAsync(c, ServerId, ServerName, ct);

                /* Fixed anchor, never the wall clock. Ten statements tie on total CPU; their key order (database, then
                   handle) differs from their insert order, so a heap-order cut cannot pass by luck. */
                var at = new DateTime(2026, 1, 15, 12, 0, 0, DateTimeKind.Unspecified);
                foreach (var (db, handle) in new[]
                         {
                             ("dbD", "0xH4"), ("dbC", "0xH3"), ("dbH", "0xH8"), ("dbB", "0xH2"),
                             ("dbG", "0xH7"), ("dbA", "0xH9"), ("dbF", "0xH6"), ("dbA", "0xH1"),
                             ("dbE", "0xH5"), ("dbB", "0xH1"),
                         })
                    await Query(c, ct, at, db, handle, "SELECT " + handle);
            }

            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            var cutoff = new DateTime(2026, 1, 15, 0, 0, 0, DateTimeKind.Utc);
            var rows = await DarlingFinOpsOptimizationReader.GetExpensiveQueriesAsync(dataSource, ServerId, cutoff, 2, 60, ct);

            Assert.Equal(
                new[] { ("dbA", "SELECT 0xH1"), ("dbA", "SELECT 0xH9") },
                rows.Select(r => (r.DatabaseName, r.FullQueryText)).ToArray());
        }
    }

    private static Task Query(NpgsqlConnection c, CancellationToken ct, DateTime at, string db, string handle, string text) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                query_text, delta_worker_time, delta_execution_count, delta_logical_reads, sample_interval_seconds)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, db, "0xQ" + handle, handle, text, 5_000_000L, 4L, 100L, 60);
}
