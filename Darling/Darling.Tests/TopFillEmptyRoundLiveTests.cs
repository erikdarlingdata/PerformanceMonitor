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
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Darling.Tests;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #5313: a window whose top groups are ALL WAITFOR shells, more of them than the first round's <c>top + 5</c> candidates.
/// The first round's page is then trimmed to nothing, so it returns no page row. It used to carry no
/// <c>candidate_count</c> either, and an empty round read as "the candidates ran out": the list came back empty although
/// real groups existed below the shells. Each statement now returns its count on a row of its own (<c>page_ord</c> NULL),
/// so the fill asks again.
///
/// <para>Each test seeds twelve shells that outrank six real groups and reads a page of three, so round one (8 candidates)
/// sees only shells and the answer is only right if a later round runs.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class TopFillEmptyRoundLiveTests
{
    private const int Top = 3;
    private const int Shells = 12;
    private const int Real = 6;
    private const string WaitforText = "WAITFOR DELAY '00:00:30'";

    [Fact]
    public Task TopQueries_AFirstRoundOfOnlyShells_StillFillsThePage() =>
        WithSharedStoreAsync("top-fill-empty-round-queries", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            for (var i = 1; i <= Shells + Real; i++)
            {
                var shell = i <= Shells;
                var text = shell ? WaitforText : "SELECT real " + i;
                var weight = shell ? 1_000_000L - i : 10_000L - i * 100;
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO query_stats
                          (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
                           query_text, query_text_digest, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads,
                           delta_logical_writes, delta_physical_reads, min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time,
                           min_dop, max_dop, sample_interval_seconds)
                      VALUES ($1, $2, $3, $4, 'FillDb', $5, $6, $7, $8, $9, $10, 10, $11, $11, 100, 0, 0, 1, $11, 1, $11, 1, 1, 3600)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.TruncateToSeconds(now.AddHours(-2).AddSeconds(i)), serverId, serverName,
                    "0xQ" + i, "0xP" + i, "0xS" + i, "0xL" + i, text, SHA256.HashData(Encoding.UTF8.GetBytes(text)), weight);
            }

            var (start, end) = (now.AddHours(-24), now.AddMinutes(5));
            foreach (var ranking in new[] { TopRanking.Cpu, TopRanking.Duration })
            {
                foreach (var rollUp in new[] { false, true })
                {
                    var result = await DarlingDataReader.GetTopQueriesByCpuRoutedAsync(postgres, serverId, start, end, Top, null, rollUp, 0, ranking, ct);
                    var label = $"{ranking}, roll-up {rollUp}";
                    Assert.True(result.Rows.Count == Top, $"{label}: {result.Rows.Count} rows, expected {Top}");
                    Assert.DoesNotContain(result.Rows, r => (r.QueryText ?? "").StartsWith("WAITFOR", StringComparison.Ordinal));
                    Assert.Equal(new[] { "SELECT real 13", "SELECT real 14", "SELECT real 15" }, result.Rows.Select(r => r.QueryText).ToArray());
                }
            }
        }, "DELETE FROM query_stats WHERE server_id = {0}");

    [Fact]
    public Task QueryStoreTop_Mcp_AFirstRoundOfOnlyShells_StillFillsThePage() =>
        WithSharedStoreAsync("top-fill-empty-round-qs-mcp", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await SeedQueryStoreAsync(connection, serverId, serverName, now, ct);
            var rows = await DarlingDataReader.GetQueryStoreTopAsync(postgres, serverId, now.AddHours(-3), now.AddMinutes(5), Top, null, null, null, ct);
            Assert.Equal(Top, rows.Count);
            Assert.DoesNotContain(rows, r => (r.QueryText ?? "").StartsWith("WAITFOR", StringComparison.Ordinal));
            Assert.Equal(new long[] { 113, 114, 115 }, rows.Select(r => r.QueryId).ToArray());
        }, "DELETE FROM query_store_stats WHERE server_id = {0}");

    [Fact]
    public Task QueryStoreTop_Viewer_AFirstRoundOfOnlyShells_StillFillsThePage() =>
        WithSharedStoreAsync("top-fill-empty-round-qs-viewer", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await SeedQueryStoreAsync(connection, serverId, serverName, now, ct);
            await using var viewer = new ViewerDataService(Environment.GetEnvironmentVariable("DARLING_TEST_PG")!);
            var rows = await viewer.GetQueryStoreTopQueriesAsync(serverId, now.AddHours(-3), now.AddMinutes(5), Top, cancellationToken: ct);
            Assert.Equal(Top, rows.Count);
            Assert.DoesNotContain(rows, r => (r.QueryText ?? "").StartsWith("WAITFOR", StringComparison.Ordinal));
            Assert.Equal(new long[] { 113, 114, 115 }, rows.Select(r => r.QueryId).ToArray());
        }, "DELETE FROM query_store_stats WHERE server_id = {0}");

    /// <summary>Twelve shell plans that outrank six real ones on total duration (executions times average).</summary>
    private static async Task SeedQueryStoreAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime now, CancellationToken ct)
    {
        for (var i = 1; i <= Shells + Real; i++)
        {
            var shell = i <= Shells;
            var queryId = shell ? i : 100 + i;
            var duration = shell ? 50_000_000L - i : 10_000L - i * 10;
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, query_hash, query_plan_hash, query_text, execution_count, avg_duration_us, avg_cpu_time_us, last_execution_time)
                  VALUES ($1, $2, $3, $4, 'FillQsDb', $5, $5, $6, $7, $8, 10, $9, 800, $2)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.TruncateToSeconds(now.AddHours(-2).AddSeconds(i)), serverId, serverName, (long)queryId,
                "0xQ" + queryId, "0xP" + queryId, shell ? WaitforText : "SELECT real " + queryId, duration);
        }
    }

    private static async Task WithSharedStoreAsync(
        string serverName, Func<NpgsqlConnection, NpgsqlDataSource, int, string, DateTime, CancellationToken, Task> body, string cleanupSql)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the live fill tests.");
        var ct = TestContext.Current.CancellationToken;
        var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);
        var cleanup = string.Format(System.Globalization.CultureInfo.InvariantCulture, cleanupSql, serverId)
                      + string.Format(System.Globalization.CultureInfo.InvariantCulture, "; DELETE FROM servers WHERE server_id = {0}", serverId);

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, cleanup);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var succeeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, serverId, serverName, ct);
            await body(connection, postgres, serverId, serverName, DarlingMcpTestData.Naive(DateTime.UtcNow), ct);
            succeeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, succeeded, async (cleanupConnection, cleanupCt) =>
                await DarlingMcpTestData.ExecAsync(cleanupConnection, cleanupCt, cleanup));
        }
    }
}
