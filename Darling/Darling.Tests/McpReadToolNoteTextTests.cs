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
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The notes the MCP read tools return are sentences a reader takes at face value: a count's verb agrees with the count, the
/// counts in one note agree with the counts beside it, and the dash a note uses is a comma or a colon, never U+2014, the rule
/// the analysis and plan text already follow.
/// </summary>
public sealed class McpReadToolNoteTextTests
{
    [Fact]
    public void TheAlertReadHealthNote_CarriesNoEmDash()
    {
        Assert.DoesNotContain('—', AlertReadFailureCounter.WindowNote);
        Assert.Contains("counting_since", AlertReadFailureCounter.WindowNote, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(1, false, 300)]
    [InlineData(10, true, 300)]
    [InlineData(1440, false, 60)]
    public void EveryBucketedTrendsAggregateNote_CarriesNoEmDash(int bucketMinutes, bool requested, int budgetPoints)
    {
        var note = TrendBuckets.AggregateNote(bucketMinutes, requested, budgetPoints);
        Assert.DoesNotContain('—', note);
        Assert.Contains("never averaged", note, StringComparison.Ordinal);
    }

    [Fact]
    public void TheExtensionMissingCollectorOutcome_CarriesNoEmDash()
    {
        var missing = new PostgresException("extension \"pg_buffercache\" is not created in this database", "ERROR", "ERROR", "42P01");
        var (_, explanation) = DarlingWorker.PostgresFaultOutcome(missing, "pg_buffer_usage", "postgres");
        Assert.Contains("extension this collector reads is not installed on this target", explanation, StringComparison.Ordinal);
        Assert.DoesNotContain('\u2014', explanation);
    }
}

/// <summary>
/// get_collection_health's <c>collector_detail_note</c> over planted collector runs: one collector that needs a look reads
/// "1 needs a look", and the note's collector count is the count the reply's <c>collectors_total</c> carries.
/// </summary>
[Collection("live-postgres")]
public sealed class CollectionHealthDetailNoteLiveTests
{
    private const string ServerName = "darling-collection-health-note-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task OneCollectorThatNeedsALook_ReadsNeeds_AndTheNoteCountsWhatTheReplyCounts()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the collection-health note live test.");

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
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            for (var i = 0; i < 6; i++)
            {
                await InsertLogRowAsync(connection, "wait_stats", now.AddHours(-i * 4 - 1), "SUCCESS", 60, 12, null, ct);
                await InsertLogRowAsync(connection, "deadlocks", now.AddHours(-i * 4 - 1), "ERROR", 90, null, "Timeout expired.", ct);
            }

            using var doc = JsonDocument.Parse(await DarlingMcpDataTools.GetCollectionHealth(postgres, ServerName, cancellationToken: ct));
            var root = doc.RootElement;
            var note = root.GetProperty("collector_detail_note").GetString()!;
            var total = root.GetProperty("collectors_total").GetInt32();

            Assert.Contains("1 needs a look", note, StringComparison.Ordinal);
            Assert.DoesNotContain("1 need a look", note, StringComparison.Ordinal);
            Assert.Contains($"of {CountText.Plural(total, "collector")} below", note, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task InsertLogRowAsync(
        NpgsqlConnection connection, string collectorName, DateTime collectionTime, string status,
        int durationMs, int? rowsCollected, string? errorMessage, CancellationToken ct)
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

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM collection_log WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
