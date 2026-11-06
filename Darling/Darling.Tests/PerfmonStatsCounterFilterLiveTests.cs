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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7903 item 1: Darling's <c>get_perfmon_stats</c> <c>counter_name</c> and <c>instance_name</c> match the whole name
/// first, ignoring case, and fall back to a substring match only when no name equals the filter, so 'Transactions/sec'
/// returns that one counter and not Write Transactions/sec beside it. Lite's twin is <c>PerfmonStatsCounterFilterTests</c>.
/// </summary>
[Collection("live-postgres")]
public sealed class PerfmonStatsCounterFilterLiveTests
{
    private const string ServerName = "perfmon-filter-7903-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string[] CounterNames(JsonDocument doc) =>
        doc.RootElement.GetProperty("counters").EnumerateArray().Select(c => c.GetProperty("counter_name").GetString()!).OrderBy(n => n, StringComparer.Ordinal).ToArray();

    [Fact]
    public async Task CounterName_MatchesTheWholeNameFirst_AndFallsBackToSubstringOnlyWhenNothingEqualsIt()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live get_perfmon_stats filter test.");
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
            var at = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-2);
            foreach (var (counter, instance, value) in new[]
            {
                ("Transactions/sec", "UserDb1", 1000L),
                ("Write Transactions/sec", "UserDb1", 400L),
                ("Tracked transactions/sec", "UserDb1", 7L),
                ("Transactions/sec", "UserDb10", 2000L),
            })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                    CollectionIdGenerator.Next(), at, ServerId, ServerName, "SQLServer:Databases", counter, instance, value, 10L, 60);
            }

            /* The exact name, in the stored case and in another: that counter only, on both instances. */
            using (var exact = JsonDocument.Parse(await DarlingMcpDataTools.GetPerfmonStats(postgres, ServerName, counter_name: "Transactions/sec", cancellationToken: ct)))
            {
                Assert.Equal(new[] { "Transactions/sec", "Transactions/sec" }, CounterNames(exact));
            }
            using (var exactOtherCase = JsonDocument.Parse(await DarlingMcpDataTools.GetPerfmonStats(postgres, ServerName, counter_name: "transactions/SEC", cancellationToken: ct)))
            {
                Assert.Equal(new[] { "Transactions/sec", "Transactions/sec" }, CounterNames(exactOtherCase));
            }

            /* A partial name equals nothing, so it is a substring match: every counter carrying it. */
            using (var partial = JsonDocument.Parse(await DarlingMcpDataTools.GetPerfmonStats(postgres, ServerName, counter_name: "transactions", cancellationToken: ct)))
            {
                Assert.Equal(4, partial.RootElement.GetProperty("counters").GetArrayLength());
            }

            /* instance_name follows the same rule: UserDb1 is one instance, not UserDb1 and UserDb10. */
            using (var instance = JsonDocument.Parse(await DarlingMcpDataTools.GetPerfmonStats(postgres, ServerName, counter_name: "Transactions/sec", instance_name: "UserDb1", cancellationToken: ct)))
            {
                var rows = instance.RootElement.GetProperty("counters").EnumerateArray().ToList();
                Assert.Single(rows);
                Assert.Equal("UserDb1", rows[0].GetProperty("instance_name").GetString());
            }

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
            $"DELETE FROM perfmon_stats WHERE server_id = {ServerId}; DELETE FROM collection_log WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
