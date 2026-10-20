/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6177 over a real store: a server whose latest <c>tempdb_stats</c> run was refused (the contained database user on
/// Azure SQL Database) and that holds no tempdb rows answers <c>precondition</c> from every tempdb read, with the stored
/// refusal and the contained-user sentence. A server with tempdb rows still answers with data.
/// </summary>
[Collection("live-postgres")]
public sealed class AzureTempDbRefusalLiveTests
{
    private const string RefusedName = "azure-tempdb-refused-6177";
    private const string DataName = "azure-tempdb-data-6177";
    private static readonly int RefusedId = ServerIdHelper.GetDeterministicHashCode(RefusedName);
    private static readonly int DataId = ServerIdHelper.GetDeterministicHashCode(DataName);

    private const string Refusal = "VIEW DATABASE PERFORMANCE STATE permission denied in database 'tempdb'.";

    private static readonly string[] Tables = { "tempdb_stats", "file_io_stats", "collection_log" };

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ARefusedTempDbCollector_AnswersPrecondition_FromEveryTempDbRead_AndDataStillAnswersWithData()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live tempdb refusal reads.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            var now = DateTime.UtcNow;
            var end = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0).AddMinutes(-1);

            await DarlingMcpTestData.RegisterServerAsync(connection, RefusedId, RefusedName, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, DataId, DataName, ct);

            /* The refused server collects other things (so it is not a collection outage) and its newest tempdb_stats run was refused. */
            var stored = Refusal + AzureDmvPermissionHint.For(262, isAzureSqlDb: true, Refusal);
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status, error_message)
VALUES ($1, $2, $3, 'tempdb_stats', $4, 'PERMISSIONS', $5)",
                61770L, RefusedId, RefusedName, DarlingMcpTestData.Naive(DarlingMcpTestData.TruncateToSeconds(now.AddMinutes(-1))), stored);

            await TrendPayloadBudgetLiveTests.SeedAsync(connection, end, ct, DataId, DataName);

            var reads = new (string Tool, Func<string, Task<string>> Run)[]
            {
                ("get_tempdb_trend", name => DarlingMcpDataTools.GetTempDbTrend(postgres, name, 24)),
                ("get_server_trend tempdb_size", name => DarlingMcpServerTrendTools.GetServerTrend(
                    postgres, "tempdb_size", name, 24, null, null, null, null, TrendBudget.Mcp(DarlingMcpServerTrendTools.MaxPoints), ct)),
                ("get_server_trend tempdb_file_io", name => DarlingMcpServerTrendTools.GetServerTrend(
                    postgres, "tempdb_file_io", name, 24, null, null, null, null, TrendBudget.Mcp(DarlingMcpServerTrendTools.MaxPoints), ct)),
            };

            foreach (var (tool, run) in reads)
            {
                var refused = JsonDocument.Parse(await run(RefusedName)).RootElement;
                Assert.True(refused.GetProperty("status").GetString() == "precondition", $"{tool} on the refused server: {refused}");
                var message = refused.GetProperty("message").GetString()!;
                Assert.Contains("tempdb_stats", message, StringComparison.Ordinal);
                Assert.Contains("contained database user", message, StringComparison.Ordinal);
                Assert.Contains("##MS_ServerStateReader##", message, StringComparison.Ordinal);

                var withData = await run(DataName);
                Assert.DoesNotContain("\"precondition\"", withData, StringComparison.Ordinal);
                Assert.DoesNotContain("\"unavailable\"", withData, StringComparison.Ordinal);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var id in new[] { RefusedId, DataId })
        {
            foreach (var table in Tables)
            {
                using var delete = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = $1", connection);
                delete.Parameters.AddWithValue(id);
                await delete.ExecuteNonQueryAsync(ct);
            }

            using var servers = new NpgsqlCommand("DELETE FROM servers WHERE server_id = $1", connection);
            servers.Parameters.AddWithValue(id);
            await servers.ExecuteNonQueryAsync(ct);
        }
    }
}
