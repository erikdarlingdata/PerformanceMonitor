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
/// #5320: statement previews are filtered, then cut, on the rig. A URI whose secret straddles the preview cut
/// (<see cref="StatementScrubCanary.UriStatement"/>) must reach a client as the marker, never as the first characters
/// of the secret that a cut before the host's sweep leaves behind.
/// </summary>
[Collection("live-postgres")]
public sealed class StatementPreviewCutLiveTests
{
    private const string ServerName = "statement-preview-cut-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task GetActiveQueries_ThroughTheHost_WithholdsAUriSecretThatStraddlesTheFourHundredCharacterCut()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live preview-cut test.");

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
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, status)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(DateTime.UtcNow.AddMinutes(-5)), ServerId, ServerName,
                91, "UriDb", StatementScrubCanary.UriStatement(400), "running");

            string raw = await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName);
            Assert.Contains("query_text", raw, StringComparison.Ordinal);

            using var server = await StatementFilterCensus.BuildHostAsync();
            var (text, isError) = await StatementFilterCensus.FilterThroughHostAsync(server, raw);

            Assert.False(isError);
            Assert.DoesNotContain(StatementScrubCanary.UriSecretPartial, text, StringComparison.Ordinal);
            Assert.Contains(SensitiveStatements.PlaceholderText, text, StringComparison.Ordinal);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task QueryHeatmap_WithholdsAUriSecret_ThatStraddlesThePreviewCut()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live preview-cut test.");

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
            var t = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-20);
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash,
     sample_interval_seconds, delta_execution_count, delta_worker_time, delta_elapsed_time,
     delta_logical_reads, delta_logical_writes, query_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(t), ServerId, ServerName,
                "UriDb", "0xURICUT", 60, 5L, 0L, 250_000L, 0L, 0L, StatementScrubCanary.UriStatement(80));

            var rows = await DarlingQueryHeatmapReader.GetQueryHeatmapAsync(
                postgres, ServerId, HeatmapMetric.Duration, t.AddMinutes(-5), t.AddMinutes(30),
                DatabaseFilter.One(null), bucketMinutes: 5, limit: 50, previewLength: 80, ct);

            var row = Assert.Single(rows);
            Assert.DoesNotContain(StatementScrubCanary.UriSecretPartial, row.TopQueryText, StringComparison.Ordinal);
            Assert.Equal(SensitiveStatements.PlaceholderText, row.TopQueryText);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM query_snapshots WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM query_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
