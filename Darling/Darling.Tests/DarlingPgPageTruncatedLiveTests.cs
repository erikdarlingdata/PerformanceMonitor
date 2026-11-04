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
               - the largest row has a database name and no relation name (usually another database's buffers)
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
            Assert.Equal(DarlingPgBufferUsageReader.NotResolvedLabel, otherDatabase.GetProperty("relation_label").GetString());
            Assert.Equal(DarlingPgBufferUsageReader.NotResolvedLabel, otherDatabase.GetProperty("relation_display").GetString());

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

    /// <summary>
    /// #7455: the call site fetches <c>limit + 1</c> so the page can tell a cut from a whole. A site that passed
    /// <c>limit</c> would hand <c>BoundPage</c> exactly <c>limit</c> rows and report <c>truncated = false</c>
    /// forever, so each tool gets a seeded cut and a seeded whole.
    /// </summary>
    [Fact]
    public async Task Blocking_Chains_SayWhenThePageIsCut()
    {
        await RunSeededAsync(async (dataSource, connection, ct, at) =>
        {
            /* Two captures, each one root blocker (pid 200) holding one victim (pid 100): two chains. */
            await SeedBlockingEdgeAsync(connection, ct, at, 1, 100, 2, 200);
            await SeedBlockingEdgeAsync(connection, ct, at.AddMinutes(1), 1, 100, 2, 200);

            var cut = JsonDocument.Parse(
                await DarlingMcpPgBlockingTools.GetPgBlocking(dataSource, ServerName, limit: 1)).RootElement;
            Assert.True(cut.GetProperty("truncated").GetBoolean(),
                "a page of 1 chain cut from 2 must say it was cut");
            Assert.False(cut.GetProperty("cycles_truncated").GetBoolean());

            var whole = JsonDocument.Parse(
                await DarlingMcpPgBlockingTools.GetPgBlocking(dataSource, ServerName, limit: 25)).RootElement;
            Assert.False(whole.GetProperty("truncated").GetBoolean(),
                "a page that holds every chain must not say it was cut");
        });
    }

    [Fact]
    public async Task Blocking_Cycles_SayWhenThePageIsCut()
    {
        await RunSeededAsync(async (dataSource, connection, ct, at) =>
        {
            /* Two captures, each a two-session cycle (100 waits on 200, 200 waits on 100): two cycles and no chain. */
            foreach (var when in new[] { at, at.AddMinutes(1) })
            {
                var collectionId = CollectionIdGenerator.Next();
                await SeedBlockingEdgeAsync(connection, ct, when, 1, 100, 2, 200, collectionId);
                await SeedBlockingEdgeAsync(connection, ct, when, 2, 200, 1, 100, collectionId);
            }

            var cut = JsonDocument.Parse(
                await DarlingMcpPgBlockingTools.GetPgBlocking(dataSource, ServerName, limit: 1)).RootElement;
            Assert.Equal("cycles_only", cut.GetProperty("status").GetString());
            Assert.True(cut.GetProperty("cycles_truncated").GetBoolean(),
                "a page of 1 cycle cut from 2 must say it was cut");

            var whole = JsonDocument.Parse(
                await DarlingMcpPgBlockingTools.GetPgBlocking(dataSource, ServerName, limit: 25)).RootElement;
            Assert.False(whole.GetProperty("cycles_truncated").GetBoolean(),
                "a page that holds every cycle must not say it was cut");
        });
    }

    [Fact]
    public async Task PredicateStats_SayWhenThePageIsCut()
    {
        await RunSeededAsync(async (dataSource, connection, ct, at) =>
        {
            foreach (var column in new[] { "c1", "c2" })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_predicate_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, table_name, column_name,
     operator, query_id, sample_count, rows_evaluated, rows_filtered, worst_estimate_error_ratio, sample_rate)
VALUES ($1, $2, $3, $4, 'app', 'public', 't', $5, '=', 7, 120, 1000, 900, 1.5, 0.01)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, column);
            }

            var cut = JsonDocument.Parse(
                await DarlingMcpPgPredicateTools.GetPgPredicateStats(dataSource, ServerName, limit: 1)).RootElement;
            Assert.True(cut.GetProperty("truncated").GetBoolean(),
                "a page of 1 predicate cut from 2 must say it was cut");

            var whole = JsonDocument.Parse(
                await DarlingMcpPgPredicateTools.GetPgPredicateStats(dataSource, ServerName, limit: 25)).RootElement;
            Assert.False(whole.GetProperty("truncated").GetBoolean(),
                "a page that holds every predicate must not say it was cut");
        });
    }

    [Fact]
    public async Task ReplicationStats_SayWhenThePageIsCut()
    {
        await RunSeededAsync(async (dataSource, connection, ct, at) =>
        {
            foreach (var replica in new[] { "replica1", "replica2" })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_replication_stats
    (collection_id, collection_time, server_id, server_name, application_name, client_addr, state, sync_state,
     sync_priority, sent_bytes_behind, write_bytes_behind, flush_bytes_behind, replay_bytes_behind,
     write_lag_ms, flush_lag_ms, replay_lag_ms, backend_start)
VALUES ($1, $2, $3, $4, $5, '10.0.0.2', 'streaming', 'async', 0, 0, 0, 0, 0, 1.0, 1.0, 1.0, $2)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, replica);
            }

            var cut = JsonDocument.Parse(
                await DarlingMcpPgReplicationStatsTools.GetPgReplicationStats(dataSource, ServerName, limit: 1)).RootElement;
            Assert.True(cut.GetProperty("truncated").GetBoolean(),
                "a page of 1 replica cut from 2 must say it was cut");

            var whole = JsonDocument.Parse(
                await DarlingMcpPgReplicationStatsTools.GetPgReplicationStats(dataSource, ServerName, limit: 25)).RootElement;
            Assert.False(whole.GetProperty("truncated").GetBoolean(),
                "a page that holds every replica must not say it was cut");
        });
    }

    [Fact]
    public async Task DeadlockDetail_SaysWhenThePageIsCut()
    {
        await RunSeededAsync(async (dataSource, connection, ct, at) =>
        {
            foreach (var (hash, minutesAgo) in new[] { ("AAAA0000AAAA0000AAAA0000AAAA0001", 0), ("AAAA0000AAAA0000AAAA0000AAAA0002", 1) })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_deadlocks
    (collection_id, collection_time, server_id, server_name, occurred_at, victim_pid, participant_count, deadlock_hash, lock_modes, resources, victim_statement, graph_text)
VALUES ($1, $2, $3, $4, $2, 4242, 2, $5, 'ShareLock', 'relation', 'UPDATE t SET x = 1', 'graph')",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at.AddMinutes(-minutesAgo)), ServerId, ServerName, hash);
            }

            var cut = JsonDocument.Parse(
                await DarlingMcpPgDeadlockTools.GetPgDeadlockDetail(dataSource, ServerName, limit: 1)).RootElement;
            Assert.True(cut.GetProperty("truncated").GetBoolean(),
                "a page of 1 graph cut from 2 must say it was cut");

            var whole = JsonDocument.Parse(
                await DarlingMcpPgDeadlockTools.GetPgDeadlockDetail(dataSource, ServerName, limit: 5)).RootElement;
            Assert.False(whole.GetProperty("truncated").GetBoolean(),
                "a page that holds every graph must not say it was cut");
        });
    }

    /// <summary>Registers the server, runs the body against a clean slate, and cleans up through the live helper.</summary>
    private static async Task RunSeededAsync(
        Func<NpgsqlDataSource, NpgsqlConnection, CancellationToken, DateTime, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live page truncation tests.");

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
            await body(dataSource, connection, ct, DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-5)));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static Task SeedBlockingEdgeAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime at,
        int blockedBackend, int blockedPid, int blockingBackend, int blockingPid, long? collectionId = null) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_blocking_edges
    (collection_id, collection_time, server_id, server_name, blocked_backend_id, blocked_pid, blocking_backend_id, blocking_pid,
     database_name, blocked_username, blocked_application_name, blocked_client_addr, blocked_state, blocked_wait_event_type,
     blocked_wait_event, blocked_query, blocked_xact_duration_ms, blocked_query_duration_ms, blocking_username,
     blocking_application_name, blocking_client_addr, blocking_state, blocking_wait_event_type, blocking_wait_event,
     blocking_query, blocking_xact_duration_ms, blocking_query_duration_ms, blocked_pid_count,
     blocking_is_idle_in_transaction, query_text_may_be_truncated)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'appdb', 'user1', 'app', '10.0.0.1', 'active', 'Lock', 'relation', 'select 1',
        100, 100, 'user2', 'app', '10.0.0.2', 'idle in transaction', NULL, NULL, 'select 2', 200, 200, 1, TRUE, FALSE)",
            collectionId ?? CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName,
            blockedBackend, blockedPid, blockingBackend, blockingPid);

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
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM pg_blocking_edges WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM pg_predicate_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM pg_replication_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM pg_deadlocks WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
