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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A page cut at <c>limit</c> says so, and the buffer rows that have no name carry a label (#7455).
///
/// <para>Before the fix <c>get_pg_buffer_usage</c> and <c>get_pg_column_stats</c> reported only the count of
/// the rows they returned, so <c>limit: 2</c> over a four-relation snapshot read as a pool of two relations.
/// The column stats reply also carried a sentence saying every table above the floor was represented, which
/// is true of the census and false of the page. These tests run the tools against a real store.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingPgPageTruncatedLiveTests
{
    private const int ServerId = -745_501;
    private const string ServerName = "pg-page-truncated-7455";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task BufferUsage_SaysWhenThePageIsCut_AndLabelsTheRowsThatHaveNoName()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live buffer usage page test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            await RegisterAsync(connection, ct);
            var at = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-5));

            /* One snapshot of four stored relations, 600 buffers in all, in a pool of 1000:
               - the largest row is another database's, with a database name and no relation name
               - one row has neither name (shared catalogs, or a database that has since been dropped)
               - two rows are named relations in the connected database */
            await SeedBufferAsync(connection, ct, at, "appdb", "orders", "r", 150);
            await SeedBufferAsync(connection, ct, at, "otherdb", null, null, 324);
            await SeedBufferAsync(connection, ct, at, null, null, null, 15);
            await SeedBufferAsync(connection, ct, at, "appdb", "orders_pk", "i", 111);

            var cut = JsonDocument.Parse(
                await DarlingMcpPgServerStateTools.GetPgBufferUsage(dataSource, ServerName, limit: 2)).RootElement;

            Assert.True(cut.GetProperty("truncated").GetBoolean(),
                "a page of 2 cut from a snapshot of 4 relations must say it was cut");
            Assert.Equal(2, cut.GetProperty("relation_count").GetInt32());
            Assert.Equal(4, cut.GetProperty("relations_in_snapshot").GetInt64());
            /* The page holds 324 + 150 buffers, so 111 + 15 are left out. */
            Assert.Equal(126, cut.GetProperty("buffers_not_shown").GetInt64());
            Assert.Contains("2 of the 4 relations", cut.GetProperty("note").GetString()!, StringComparison.Ordinal);

            var whole = JsonDocument.Parse(
                await DarlingMcpPgServerStateTools.GetPgBufferUsage(dataSource, ServerName, limit: 25)).RootElement;

            Assert.False(whole.GetProperty("truncated").GetBoolean(),
                "a page that holds every relation in the snapshot must not say it was cut");
            Assert.Equal(4, whole.GetProperty("relation_count").GetInt32());
            Assert.Equal(4, whole.GetProperty("relations_in_snapshot").GetInt64());
            Assert.Equal(0, whole.GetProperty("buffers_not_shown").GetInt64());

            var rows = whole.GetProperty("relations").EnumerateArray().ToArray();

            var otherDatabase = rows.Single(r => r.GetProperty("database_name").GetString() == "otherdb");
            Assert.Equal(JsonValueKind.Null, otherDatabase.GetProperty("relation_name").ValueKind);
            Assert.Equal(DarlingPgBufferUsageReader.OtherDatabaseLabel, otherDatabase.GetProperty("relation_label").GetString());
            Assert.Equal(DarlingPgBufferUsageReader.OtherDatabaseLabel, otherDatabase.GetProperty("relation_display").GetString());

            var sharedOrDropped = rows.Single(r => r.GetProperty("database_name").ValueKind == JsonValueKind.Null);
            Assert.Equal(DarlingPgBufferUsageReader.SharedOrDroppedLabel, sharedOrDropped.GetProperty("relation_label").GetString());
            Assert.Equal(DarlingPgBufferUsageReader.SharedOrDroppedLabel, sharedOrDropped.GetProperty("relation_display").GetString());

            var named = rows.Single(r => r.GetProperty("relation_name").GetString() == "orders");
            Assert.Equal(JsonValueKind.Null, named.GetProperty("relation_label").ValueKind);
            Assert.Equal("orders", named.GetProperty("relation_display").GetString());

            Assert.Contains("not an error", whole.GetProperty("note").GetString()!, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task ColumnStats_SaysWhenThePageIsCut_AndFullCoverageStaysAboutTheCensus()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live column stats page test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            await RegisterAsync(connection, ct);
            var at = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-5));

            for (var i = 1; i <= 4; i++)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_column_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, table_name, column_name,
     n_distinct, null_frac, avg_width, correlation, top_value_frequency, common_value_count)
VALUES ($1, $2, $3, $4, 'appdb', 'public', $5, 'id', -1, 0, 8, 0.1, $6, 3)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName,
                    "t" + i, 0.1 * i);
            }

            var cut = JsonDocument.Parse(
                await DarlingMcpPgIndexTools.GetPgColumnStats(dataSource, ServerName, limit: 2)).RootElement;

            Assert.True(cut.GetProperty("truncated").GetBoolean(),
                "a page of 2 cut from 4 collected columns must say it was cut");
            Assert.Equal(2, cut.GetProperty("column_count").GetInt32());
            var cutNote = cut.GetProperty("note").GetString()!;
            Assert.Contains("cut at limit", cutNote, StringComparison.Ordinal);

            var whole = JsonDocument.Parse(
                await DarlingMcpPgIndexTools.GetPgColumnStats(dataSource, ServerName, limit: 25)).RootElement;

            Assert.False(whole.GetProperty("truncated").GetBoolean(),
                "a page that holds every collected column must not say it was cut");
            Assert.Equal(4, whole.GetProperty("column_count").GetInt32());
            Assert.DoesNotContain("cut at limit", whole.GetProperty("note").GetString()!, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>The wording of the all-clear arm: it describes the census, and says the page may be shorter.</summary>
    [Fact]
    public void FullCoverageSentence_SaysItDescribesTheCensusAndNotThePage()
    {
        var verdict = PgColumnStatsCoverage.Classify(
            evidenceCollectorRan: true, candidateTables: 40, tablesWithVisibleStatistics: 40, storedColumnRows: 3);

        Assert.Equal(PgColumnStatsCoverageArm.FullyMeasured, verdict.Arm);
        Assert.Contains("FULL COVERAGE", verdict.Message, StringComparison.Ordinal);
        Assert.Contains("not about the page", verdict.Message, StringComparison.Ordinal);
    }

    private static async Task RegisterAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "UPDATE servers SET engine_kind = $2, created_date = $3 WHERE server_id = $1",
            ServerId, MonitoredEngineKind.Postgres, DarlingMcpTestData.Naive(DateTime.UtcNow.AddDays(-30)));
    }

    private static Task SeedBufferAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime at,
        string? database, string? relation, string? kind, long buffers) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_buffer_usage
    (collection_id, collection_time, server_id, server_name, database_name, relation_name, relation_kind,
     buffers, dirty_buffers, avg_usage_count, pool_buffers_total, pool_buffers_used)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0, 2.5, 1000, 600)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName,
            database, relation, kind, buffers);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM pg_buffer_usage WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM pg_column_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
