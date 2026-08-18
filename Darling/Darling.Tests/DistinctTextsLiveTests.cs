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
using Darling.Tests;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// Pins #2012 stage 1 against dev Postgres: hash-grouped top-queries reads carry
/// <c>distinct_texts</c>, so a group that MERGED different statements (INSERT...EXEC statements
/// naming different callee procs share a <c>query_hash</c> — reproduced live on SQL Server 2022;
/// it mislabeled production triage) stops masquerading as one statement labeled by one
/// arbitrary representative text. The count rides the #1767 content digest already on every row;
/// the honest-zero case (legacy rows predating the digest column) is pinned too.
/// </summary>
[Collection("live-postgres")]
public sealed class DistinctTextsLiveTests
{
    private const string ServerName = "darling-distinct-texts-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Db = "StackOverflow";

    [Fact]
    public async Task TopQueries_CountDistinctTexts_AndAnnotateBlendedGroups_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live distinct-texts test.");

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

            /* The collision shape from the live repro: one query_hash, two DIFFERENT statements
               (different digests, different texts — the INSERT...EXEC callees). Plus a healthy
               single-text hash, and a legacy hash whose rows predate the digest column (NULL
               digest → COUNT(DISTINCT) = 0, the honest-unknown case). */
            await PlantAsync(connection, ct, now.AddMinutes(-10), "0xINSEXECHASH", "INSERT INTO #items EXEC dbo.inner_v3", new byte[] { 0x01 }, 100L);
            await PlantAsync(connection, ct, now.AddMinutes(-5), "0xINSEXECHASH", "INSERT INTO #items EXEC dbo.inner_v4", new byte[] { 0x02 }, 90L);
            await PlantAsync(connection, ct, now.AddMinutes(-8), "0xSINGLEHASH", "SELECT TOP (1) * FROM Posts", new byte[] { 0x03 }, 50L);
            await PlantAsync(connection, ct, now.AddMinutes(-7), "0xSINGLEHASH", "SELECT TOP (1) * FROM Posts", new byte[] { 0x03 }, 40L);
            await PlantAsync(connection, ct, now.AddMinutes(-6), "0xLEGACYHASH", "SELECT OldRow FROM PreDigestEra", digest: null, 30L);

            /* ---- reader: the blend counts 2, the single-text hash 1, the legacy hash 0. */
            var rows = await DarlingDataReader.GetTopQueriesByCpuAsync(
                postgres, ServerId, now.AddHours(-1), now.AddMinutes(5), top: 10, databaseName: null, cancellationToken: ct);

            Assert.Equal(2, rows.Single(r => r.QueryHash == "0xINSEXECHASH").DistinctTexts);
            Assert.Equal(1, rows.Single(r => r.QueryHash == "0xSINGLEHASH").DistinctTexts);
            Assert.Equal(0, rows.Single(r => r.QueryHash == "0xLEGACYHASH").DistinctTexts);

            /* ---- tool envelope: distinct_texts rides out, and text_note fires ONLY on the blend. */
            var json = await DarlingMcpDataTools.GetTopQueriesByCpu(postgres, Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, ServerName, hours_back: 2, top: 10);
            using var doc = JsonDocument.Parse(json);
            var queries = doc.RootElement.GetProperty("queries").EnumerateArray().ToList();

            var blend = queries.Single(q => q.GetProperty("query_hash").GetString() == "0xINSEXECHASH");
            Assert.Equal(2, blend.GetProperty("distinct_texts").GetInt64());
            Assert.Contains("INSERT...EXEC", blend.GetProperty("text_note").GetString(), StringComparison.Ordinal);

            var single = queries.Single(q => q.GetProperty("query_hash").GetString() == "0xSINGLEHASH");
            Assert.Equal(1, single.GetProperty("distinct_texts").GetInt64());
            Assert.Equal(JsonValueKind.Null, single.GetProperty("text_note").ValueKind);

            succeeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, succeeded, async (cleanup, cleanupCt) =>
                await CleanupAsync(cleanup, cleanupCt));
        }
    }

    private static async Task PlantAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime at,
        string queryHash, string queryText, byte[]? digest, long weight)
    {
        /* DISTINCT per caller, mirroring reality: different INSERT...EXEC callers have different
           sql_handles even when their statement texts are the same length — the remediation note's
           whole premise ("attribute per-caller work via sql_handle") depends on it, and the first
           cut of this fixture accidentally collided them (review catch). */
        var sqlHandle = "0xSQLH" + Convert.ToHexString(
            System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(queryText)))[..12];

        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name,
                                       query_hash, query_plan_hash, sql_handle, plan_handle, query_text,
                                       query_text_digest, delta_execution_count, delta_worker_time,
                                       delta_elapsed_time, delta_logical_reads, min_dop, max_dop)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, Db,
            queryHash, "0xPLANHASH", sqlHandle, "0xPLANH", queryText,
            (object?)digest ?? DBNull.Value, weight, weight * 1000L, weight * 2000L, weight * 10L, 1, 1);
    }

    private static async Task CleanupAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        /* Interpolated (an int), not parameterized: Npgsql cannot prepare a multi-statement
           command with parameters — the same shape DeleteTestRowsAsync uses elsewhere. */
        await DarlingMcpTestData.ExecAsync(connection, ct,
            $"DELETE FROM query_stats WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId}");
    }
}
