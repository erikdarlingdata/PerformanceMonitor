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
using Darling.Tests;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #3523: <c>get_top_queries_by_cpu</c> / <c>get_top_procedures_by_cpu</c> ranked by summed ELAPSED time.
///
/// <para><b>The defect.</b> On a wait-bound server, elapsed and CPU disagree wildly — a query that sleeps on
/// locks for minutes ranks above one that burns a core — so the real CPU consumers could be absent from the
/// page entirely, while <c>attributed_cpu_ratio</c> (page CPU / measured process CPU) read as "hidden or
/// evicted CPU" when it actually meant "wrong sort key". Every CPU investigation starts at this tool.</para>
///
/// <para><b>Why a LIVE test on top of the SQL-text pins.</b> The queries reads have TWO ordering sites — the
/// ranking CTE's <c>ORDER BY ... LIMIT top + 5</c> decides which groups SURVIVE at all, and the outer
/// post-WAITFOR-trim sort decides the returned order. A text pin restates each clause; only real rows through
/// real Postgres prove the cut. The seed makes the CPU king the WORST group by elapsed time with more
/// competing groups than the over-fetch admits, so under the old key it was not merely mis-sorted — it was
/// cut before the outer sort could see it.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class CpuRankingLiveTests
{
    private const string ServerName = "darling-cpu-ranking-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Db = "waitbound";

    [Fact]
    public async Task ByCpuReads_RankAndCutByWorkerTime_WhenCpuAndElapsedDisagree_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live CPU-ranking test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var succeeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var now = DarlingMcpTestData.Naive(DateTime.UtcNow);

            /* The wait-bound shape: one CPU king whose elapsed time is the SMALLEST on the box, and six
               lock-sleepers whose elapsed dwarfs it while their CPU is noise. Seven groups against
               top: 1 (over-fetch 6) means an elapsed-keyed CTE cuts the king before the outer sort. */
            await PlantQueryAsync(connection, ct, now.AddMinutes(-9), "0xCPUKING",
                "SELECT CpuBurner FROM Numbers", cpuUs: 900_000L, elapsedUs: 1_000L);
            for (var i = 1; i <= 6; i++)
            {
                await PlantQueryAsync(connection, ct, now.AddMinutes(-8), $"0xSLEEPER{i}",
                    $"SELECT Blocked{i} FROM Locked WHERE Id = {i}", cpuUs: 1_000L + i, elapsedUs: 800_000L + i * 10_000L);
            }

            /* ---- the cut: top 1 must be the CPU king, in BOTH groupings (the rollup const has its own
               copies of both ordering sites). All rows are ad-hoc, so the rollup grouping degenerates to
               per-hash and exercises purely its ordering keys. */
            foreach (var rollUp in new[] { false, true })
            {
                var top1 = await DarlingDataReader.GetTopQueriesByCpuAsync(
                    postgres, ServerId, now.AddHours(-1), now.AddMinutes(5), top: 1, databaseName: null,
                    rollUpByHostObject: rollUp, cancellationToken: ct);
                var king = Assert.Single(top1);
                Assert.Equal("0xCPUKING", king.QueryHash);
            }

            /* ---- the order: the full page comes back in descending CPU order, king first. */
            var page = await DarlingDataReader.GetTopQueriesByCpuAsync(
                postgres, ServerId, now.AddHours(-1), now.AddMinutes(5), top: 20, databaseName: null,
                rollUpByHostObject: false, cancellationToken: ct);
            Assert.Equal(7, page.Count);
            Assert.Equal("0xCPUKING", page[0].QueryHash);
            Assert.Equal(page.Select(r => r.TotalCpuUs).OrderByDescending(v => v), page.Select(r => r.TotalCpuUs));

            /* ---- procedures: same disagreement, same promise (single ordering site, no over-fetch). */
            await PlantProcedureAsync(connection, ct, now.AddMinutes(-7), "usp_CpuHog", cpuUs: 900_000L, elapsedUs: 1_000L);
            await PlantProcedureAsync(connection, ct, now.AddMinutes(-6), "usp_WaitBound", cpuUs: 5_000L, elapsedUs: 900_000L);

            var topProc = await DarlingDataReader.GetTopProceduresByCpuAsync(
                postgres, ServerId, now.AddHours(-1), now.AddMinutes(5), top: 1, databaseName: null, cancellationToken: ct);
            Assert.Equal("usp_CpuHog", Assert.Single(topProc).ObjectName);

            succeeded = true;
        }
        finally
        {
            /* #1902: teardown on its OWN connection, never the body's — see HostObjectRollupLiveTests. */
            await LiveStoreCleanup.RunAsync(connectionString!, succeeded, async (cleanup, cleanupCt) =>
                await CleanupAsync(cleanup, cleanupCt));
        }
    }

    private static async Task PlantQueryAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime at,
        string queryHash, string queryText, long cpuUs, long elapsedUs)
    {
        var sqlHandle = "0xSQLH" + Convert.ToHexString(
            System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(queryText)))[..12];
        var digest = System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(queryText));

        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name,
                                       query_hash, query_plan_hash, sql_handle, plan_handle, query_text,
                                       query_text_digest, delta_execution_count, delta_worker_time,
                                       delta_elapsed_time, delta_logical_reads, min_dop, max_dop)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, Db,
            queryHash, "0xPLANHASH", sqlHandle, "0xPLANH", queryText,
            digest, 10L, cpuUs, elapsedUs, 100L, 1, 1);
    }

    private static async Task PlantProcedureAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime at,
        string objectName, long cpuUs, long elapsedUs) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name,
                                           schema_name, object_name, object_type, delta_execution_count,
                                           delta_worker_time, delta_elapsed_time)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, Db,
            "dbo", objectName, "SQL_STORED_PROCEDURE", 10L, cpuUs, elapsedUs);

    private static async Task CleanupAsync(NpgsqlConnection connection, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            $"DELETE FROM query_stats WHERE server_id = {ServerId}; DELETE FROM procedure_stats WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId}");
}
