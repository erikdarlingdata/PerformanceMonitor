/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text;
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
/// #4198: get_collection_health has no row to drop (every collector on the server is one row, and a health
/// read must never hide a failing/stale/disabled/erroring one by leaving it off the page), so its default-size
/// cut is per-field instead. This seeds every SQL Server catalog collector - the realistic per-server shape -
/// mostly HEALTHY-and-boring, plus four deliberately NOT-boring rows that must never compact even though three
/// of the four band HEALTHY, and measures the tool method's own UTF-8 byte count. Its own file/seeding per the
/// #4198 common brief: not shared with any other lane's tonight.
/// </summary>
[Collection("live-postgres")]
public sealed class CollectionHealthPayloadBudgetLiveTests
{
    private const string ServerName = "darling-collection-health-budget-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private readonly ITestOutputHelper _output;
    public CollectionHealthPayloadBudgetLiveTests(ITestOutputHelper output) => _output = output;

    [Fact]
    public async Task DefaultCall_StaysUnderBudget_AndNeverCompactsANonBoringCollector()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live get_collection_health budget test.");
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
            var sqlServerCollectors = CollectorCatalog.All
                .Where(d => d.TargetEngine == CollectorTargetEngine.SqlServer)
                .Select(d => d.Name)
                .ToArray();
            Assert.True(sqlServerCollectors.Length > 30, "expected a realistic SQL Server catalog width");

            /* four collectors that must NEVER compact, three of them despite banding HEALTHY: */
            var neverCompact = new[] { "wait_stats", "memory_grant_stats", "query_store_health", "database_scoped_config" };
            Assert.All(neverCompact, name => Assert.Contains(name, sqlServerCollectors));

            foreach (var name in sqlServerCollectors)
            {
                switch (name)
                {
                    case "wait_stats":
                        /* FAILING band: recent ERROR runs, never a success, so HealthStatus itself excludes it. */
                        for (var i = 0; i < 5; i++)
                            await InsertLogRowAsync(connection, name, now.AddHours(-i * 6), "ERROR", 120, null,
                                "Login failed for user 'darling_monitor'.", ct);
                        break;

                    case "memory_grant_stats":
                        /* WARNING band via a 30% error rate - HealthStatus alone would already exclude this
                           one, but it also exercises errors > 0 beside a fresh fresh success. */
                        for (var i = 0; i < 7; i++)
                            await InsertLogRowAsync(connection, name, now.AddHours(-i * 20 - 1), "SUCCESS", 80, 40, null, ct);
                        for (var i = 0; i < 3; i++)
                            await InsertLogRowAsync(connection, name, now.AddHours(-i * 30 - 2), "ERROR", 90, null, "Timeout expired.", ct);
                        break;

                    case "query_store_health":
                        /* HEALTHY band (a fresh success, 0 current errors) but PermissionDeniedCount > 0 from
                           an OLDER denial this window - the case HealthStatus alone would miss and the reason
                           IsCollectionHealthCompactEligible checks PermissionDeniedCount directly. */
                        await InsertLogRowAsync(connection, name, now.AddDays(-6), "PERMISSIONS", 50, null,
                            "permission denied for function pg_read_file", ct);
                        await InsertLogRowAsync(connection, name, now.AddDays(-6).AddHours(-1), "PERMISSIONS", 50, null,
                            "permission denied for function pg_read_file", ct);
                        for (var i = 0; i < 4; i++)
                            await InsertLogRowAsync(connection, name, now.AddHours(-i * 12), "SUCCESS", 60, 12, null, ct);
                        break;

                    case "database_scoped_config":
                        /* HEALTHY band, RowsStored = 0, and NOT an event collector - FormatOutputFinding's
                           "needs a look" reading, which a compact row must never hide. */
                        for (var i = 0; i < 8; i++)
                            await InsertLogRowAsync(connection, name, now.AddHours(-i * 18), "SUCCESS", 30, 0, null, ct);
                        break;

                    case "deadlocks":
                        /* Event collector at rest: RowsStored = 0 but IsEventCollector is true, so this one
                           SHOULD compact - the boring-empty case #1852/#3754 protect deliberately as healthy. */
                        for (var i = 0; i < 8; i++)
                            await InsertLogRowAsync(connection, name, now.AddHours(-i * 18), "SUCCESS", 15, 0, null, ct);
                        break;

                    default:
                        /* The realistic majority: plainly healthy and productive. */
                        for (var i = 0; i < 6; i++)
                            await InsertLogRowAsync(connection, name, now.AddHours(-i * 24 - 1), "SUCCESS", 100 + i * 15, 50 + i * 5, null, ct);
                        break;
                }
            }

            var defaultJson = await DarlingMcpDataTools.GetCollectionHealth(postgres, ServerName);
            var defaultBytes = Encoding.UTF8.GetByteCount(defaultJson);
            var fullJson = await DarlingMcpDataTools.GetCollectionHealth(postgres, ServerName, full_detail: true);
            var fullBytes = Encoding.UTF8.GetByteCount(fullJson);
            _output.WriteLine($"get_collection_health: default {defaultBytes:N0} bytes, full_detail=true {fullBytes:N0} bytes, budget {McpResponseBudget.DefaultBytes:N0}.");

            Assert.True(defaultBytes <= McpResponseBudget.DefaultBytes,
                $"default get_collection_health is {defaultBytes:N0} bytes, over the {McpResponseBudget.DefaultBytes:N0}-byte budget.");
            Assert.True(defaultBytes < fullBytes, "the default call should be smaller than full_detail=true.");

            using var defaultDoc = JsonDocument.Parse(defaultJson);
            var defaultRows = defaultDoc.RootElement.GetProperty("collectors").EnumerateArray()
                .ToDictionary(r => r.GetProperty("collector").GetString()!, r => r);
            Assert.Equal(sqlServerCollectors.Length, defaultRows.Count);

            foreach (var name in neverCompact)
            {
                Assert.True(defaultRows[name].TryGetProperty("errors", out _), $"{name} must keep full detail by default (it is not boring-healthy).");
                Assert.False(defaultRows[name].TryGetProperty("compact", out _), $"{name} must not be marked compact.");
            }
            Assert.True(defaultRows["deadlocks"].TryGetProperty("compact", out var deadlocksCompact) && deadlocksCompact.GetBoolean(),
                "an event collector resting at zero rows should compact.");
            Assert.True(defaultRows.Values.Count(r => r.TryGetProperty("compact", out _)) >= sqlServerCollectors.Length - neverCompact.Length,
                "every boring-healthy collector should compact.");

            using var fullDoc = JsonDocument.Parse(fullJson);
            Assert.All(fullDoc.RootElement.GetProperty("collectors").EnumerateArray(),
                r => Assert.False(r.TryGetProperty("compact", out _), "full_detail=true must serve every field on every row."));

            var note = defaultDoc.RootElement.GetProperty("collector_detail_note").GetString();
            Assert.Contains($"of {sqlServerCollectors.Length} collector", note, StringComparison.Ordinal);

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
