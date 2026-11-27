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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9086, Darling's twin of Lite's <c>CollectionHealthExplainedZeroToolTests</c>: get_collection_health counted a HEALTHY collector that read and
/// found nothing (no job running, no trace flag set) among the ones that need a look. The note's count now agrees with the rows, and the
/// explained-zero row keeps the output_finding that says why.
/// </summary>
[Collection("live-postgres")]
public sealed class CollectionHealthExplainedZeroLiveTests
{
    private const string ServerName = "darling-collection-health-explained-zero-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ExplainedZeroCollector_IsNotCountedAsNeedingALook_AndKeepsItsOutputFinding()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live get_collection_health explained-zero test.");
        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await RegisterServerAsync(connection, ct);
            var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
            for (var i = 0; i < 6; i++)
            {
                await InsertLogRowAsync(connection, "wait_stats", now.AddMinutes(-i * 30 - 2), "SUCCESS", 40, 40, null, ct);
                await InsertLogRowAsync(connection, "running_jobs", now.AddMinutes(-i * 30 - 2), "SUCCESS", 40, 0, null, ct);
                await InsertLogRowAsync(connection, "memory_stats", now.AddMinutes(-i * 30 - 2), "ERROR", 40, null, "simulated failure", ct);
            }

            var json = await DarlingMcpDataTools.GetCollectionHealth(postgres, ServerName);
            using var doc = JsonDocument.Parse(json);
            var rows = doc.RootElement.GetProperty("collectors").EnumerateArray().ToDictionary(r => r.GetProperty("collector").GetString()!);
            var note = doc.RootElement.GetProperty("collector_detail_note").GetString()!;

            var jobs = rows["running_jobs"];
            Assert.Equal("HEALTHY", jobs.GetProperty("status").GetString());
            Assert.False(string.IsNullOrWhiteSpace(jobs.GetProperty("output_finding").GetString()));
            Assert.NotEqual("HEALTHY", rows["memory_stats"].GetProperty("status").GetString());
            Assert.StartsWith("1 of 3 collectors below is HEALTHY", note, StringComparison.Ordinal);
            Assert.Contains(" 1 needs a look", note, StringComparison.Ordinal);
            Assert.Contains(" 1 is HEALTHY and stored nothing", note, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task RegisterServerAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $3, TRUE, 15, $4, $4)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE, sql_major_version = 15;", connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertLogRowAsync(
        NpgsqlConnection connection, string collectorName, DateTime collectionTime, string status,
        int durationMs, int? rowsCollected, string? errorMessage, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO collection_log (log_id, collection_time, server_id, server_name, collector_name, status, duration_ms, rows_collected, error_message)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(collectorName);
        command.Parameters.AddWithValue(status);
        command.Parameters.AddWithValue(durationMs);
        command.Parameters.AddWithValue((object?)rowsCollected ?? DBNull.Value);
        command.Parameters.AddWithValue((object?)errorMessage ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM collection_log WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
