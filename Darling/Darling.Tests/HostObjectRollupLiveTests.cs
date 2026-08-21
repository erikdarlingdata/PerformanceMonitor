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
/// #2235: <c>group_by: "host_object"</c> — rolling a procedure's dynamic-SQL fragments into one row.
///
/// <para><b>The defect.</b> <c>query_hash</c> is a SHAPE hash, so dynamic SQL built with per-value literals
/// fragments one logical statement across as many hashes as there are literal sets — measured at 21 for a single
/// <c>API.GetInventoryWithLabsV5</c> statement on <c>prod-sql-use2-alpha-01</c>. Ranking by hash therefore
/// STRUCTURALLY cannot surface it: two fragments together were 58-65% of the instance's worker_time in every
/// window sampled, while the hash never entered the 168-hour top 20 and the ranking as a whole accounted for
/// roughly a tenth of the box's CPU. Nothing in the output said so.</para>
///
/// <para><b>Why this is a LIVE test and not an SQL-text pin.</b> The whole change is a <c>GROUP BY</c> — an
/// <c>Assert.Contains</c> on the clause would restate the code rather than test it. The two properties that
/// matter are what the grouping DOES to rows, and one of them (ad-hoc rows must not pool) is a silent
/// mis-attribution rather than an error, so it has to be observed on real rows through real Postgres grouping
/// semantics.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class HostObjectRollupLiveTests
{
    private const string ServerName = "darling-host-rollup-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Db = "apex";

    [Fact]
    public async Task HostObjectRollup_CollapsesProcFragments_ButNeverPoolsAdHoc_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live host-object rollup test.");

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

            /* The reported shape: one proc's statement fragmented across three literal variants, each too
               small to rank, but the largest consumer once summed. */
            await PlantAsync(connection, ct, now.AddMinutes(-9), "0xFRAG1", "API.GetInventoryWithLabsV5",
                "insert #result select ... where LocId = 101", 100L);
            await PlantAsync(connection, ct, now.AddMinutes(-8), "0xFRAG2", "API.GetInventoryWithLabsV5",
                "insert #result select ... where LocId = 102", 100L);
            await PlantAsync(connection, ct, now.AddMinutes(-7), "0xFRAG3", "API.GetInventoryWithLabsV5",
                "insert #result select ... where LocId = 103", 100L);

            /* A second proc, so the rollup is proven to group per host object rather than per database. */
            await PlantAsync(connection, ct, now.AddMinutes(-6), "0xOTHER1", "dbo.SomethingElse",
                "select 1 from dbo.Other where Id = 1", 40L);

            /* THE TRAP ROWS: ad-hoc statements, host_object_name NULL. A bare GROUP BY host_object_name
               would pool these two unrelated statements into one row — a worse attribution bug than the one
               being fixed. They must stay one row each. */
            await PlantAsync(connection, ct, now.AddMinutes(-5), "0xADHOC1", null, "SELECT AdHocOne FROM T1", 250L);
            await PlantAsync(connection, ct, now.AddMinutes(-4), "0xADHOC2", null, "SELECT AdHocTwo FROM T2", 240L);

            /* ---- default grouping is UNCHANGED: every fragment is its own row and none of them wins. */
            var perHash = await DarlingDataReader.GetTopQueriesByCpuAsync(
                postgres, ServerId, now.AddHours(-1), now.AddMinutes(5), top: 20, databaseName: null,
                rollUpByHostObject: false, cancellationToken: ct);

            Assert.Equal(3, perHash.Count(r => r.HostObjectName == "API.GetInventoryWithLabsV5"));
            Assert.All(perHash, r => Assert.Equal(1, r.DistinctQueryHashes));
            /* The reported failure, reproduced: the fragmented statement does NOT rank first per-hash. */
            Assert.NotEqual("API.GetInventoryWithLabsV5", perHash[0].HostObjectName);

            /* ---- rollup: the three fragments become ONE row that now outranks everything. */
            var rolled = await DarlingDataReader.GetTopQueriesByCpuAsync(
                postgres, ServerId, now.AddHours(-1), now.AddMinutes(5), top: 20, databaseName: null,
                rollUpByHostObject: true, cancellationToken: ct);

            var proc = Assert.Single(rolled, r => r.HostObjectName == "API.GetInventoryWithLabsV5");
            Assert.Equal(3, proc.DistinctQueryHashes);
            Assert.Equal(300L, proc.TotalExecutions);
            /* THE POINT: summed, it is the top consumer — which is what a per-hash ranking could not show. */
            Assert.Equal("API.GetInventoryWithLabsV5", rolled[0].HostObjectName);

            /* The other proc stays its own row: grouped per host object, not per database. */
            var other = Assert.Single(rolled, r => r.HostObjectName == "dbo.SomethingElse");
            Assert.Equal(1, other.DistinctQueryHashes);

            /* ---- THE TRAP, pinned: ad-hoc rows are STILL one per hash, not pooled into a NULL-host row. */
            var adHoc = rolled.Where(r => r.HostObjectName is null).ToList();
            Assert.Equal(2, adHoc.Count);
            Assert.All(adHoc, r => Assert.Equal(1, r.DistinctQueryHashes));
            Assert.Contains(adHoc, r => r.QueryHash == "0xADHOC1");
            Assert.Contains(adHoc, r => r.QueryHash == "0xADHOC2");
            /* And their text is their OWN, not a neighbour's — the LATERAL must still key on hash when the
               host is null, or an ad-hoc row would borrow an unrelated statement's text. */
            Assert.Equal("SELECT AdHocOne FROM T1", adHoc.Single(r => r.QueryHash == "0xADHOC1").QueryText);
            Assert.Equal("SELECT AdHocTwo FROM T2", adHoc.Single(r => r.QueryHash == "0xADHOC2").QueryText);

            /* ---- totals are conserved: a rollup must redistribute CPU, never invent or lose it. */
            Assert.Equal(perHash.Sum(r => r.TotalCpuUs), rolled.Sum(r => r.TotalCpuUs));
            Assert.Equal(perHash.Sum(r => r.TotalExecutions), rolled.Sum(r => r.TotalExecutions));

            succeeded = true;
        }
        finally
        {
            /* #1902: teardown on its OWN connection, never the body's. A finally that cleans up on the
               body's connection throws from the finally and REPLACES the body's exception with the
               teardown's — and it is the body's failure that closed the connection, so the teardown fails
               because of the very thing it then hides. Enforced by
               LiveCleanupConversionRatchetTests.NoLiveTestCleansUpOnItsOwnBodysConnection. */
            await LiveStoreCleanup.RunAsync(connectionString!, succeeded, async (cleanup, cleanupCt) =>
                await CleanupAsync(cleanup, cleanupCt));
        }
    }

    private static async Task PlantAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime at,
        string queryHash, string? hostObject, string queryText, long weight)
    {
        var sqlHandle = "0xSQLH" + Convert.ToHexString(
            System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(queryText)))[..12];
        var digest = System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(queryText));

        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name,
                                       query_hash, query_plan_hash, sql_handle, plan_handle, query_text,
                                       query_text_digest, host_object_name, delta_execution_count,
                                       delta_worker_time, delta_elapsed_time, delta_logical_reads, min_dop, max_dop)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, Db,
            queryHash, "0xPLANHASH", sqlHandle, "0xPLANH", queryText,
            digest, (object?)hostObject ?? DBNull.Value,
            weight, weight * 1000L, weight * 2000L, weight * 10L, 1, 1);
    }

    private static async Task CleanupAsync(NpgsqlConnection connection, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            $"DELETE FROM query_stats WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId}");
}
