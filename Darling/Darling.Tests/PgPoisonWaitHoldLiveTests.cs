/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7082 against a real store: the Poison Wait read carries the window before the current one, so an 8 minute
/// episode reads as a held-back Warning and a 20 minute one pages; and the detail reads name the top wait events and
/// the statements, from <c>pg_wait_sampling</c> first and <c>pg_session_states</c> when that holds nothing.
/// <c>DARLING_TEST_PG</c>-gated: runs in CI, skips without a store.
/// </summary>
[Collection("live-postgres")]
public sealed class PgPoisonWaitHoldLiveTests
{
    private const int ServerId = -708201;
    private const string ServerName = "poison-wait-hold-read";

    /* 2.4 backends waiting for one minute, in microseconds. */
    private const long MinuteAtTwoPointFourBackendsUs = 144_000_000;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheReadSeparatesAnEightMinuteEpisodeFromATwentyMinuteOne_AndNamesEventsAndStatements_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Poison Wait hold test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await using var postgres = NpgsqlDataSource.Create(cs!);
            var adapter = new DarlingPostgresAlertReadAdapter(postgres);
            var now = DateTime.UtcNow;

            /* BtreePage: 2.4 backends for 20 minutes (one row a minute, half a minute off the window edge).
               BufferIo: 2.4 backends for the last 8 minutes only. */
            for (var minutesAgo = 19.5; minutesAgo > 0; minutesAgo -= 1)
            {
                await SeedWaitAsync(connection, ct, now.AddMinutes(-minutesAgo), "IPC", "BtreePage", MinuteAtTwoPointFourBackendsUs);
            }

            for (var minutesAgo = 7.5; minutesAgo > 0; minutesAgo -= 1)
            {
                await SeedWaitAsync(connection, ct, now.AddMinutes(-minutesAgo), "IPC", "BufferIo", MinuteAtTwoPointFourBackendsUs);
            }

            /* Idle classes and a quieter event: the top three leaves the idle class out. */
            await SeedWaitAsync(connection, ct, now.AddMinutes(-3.5), "Client", "ClientRead", 50 * MinuteAtTwoPointFourBackendsUs);
            await SeedWaitAsync(connection, ct, now.AddMinutes(-3.5), "LWLock", "BufferMapping", MinuteAtTwoPointFourBackendsUs / 8);

            var window = await adapter.GetPoisonWaitPressureAsync(ServerId, ct);

            var btree = Assert.Single(window.Waits, w => w.WaitEvent == "BtreePage");
            Assert.NotNull(PostgresAlertEvaluator.EvaluatePoisonWait(btree));
            Assert.True(btree.PriorWindowAccumulatedWaitMs >= PoisonWaitEvaluator.WarningAvgWaiters * 600_000);

            var bufferIo = Assert.Single(window.Waits, w => w.WaitEvent == "BufferIo");
            Assert.True(bufferIo.AccumulatedWaitMs >= PoisonWaitEvaluator.WarningAvgWaiters * 600_000);
            Assert.Equal(0, bufferIo.PriorWindowAccumulatedWaitMs);
            Assert.Null(PostgresAlertEvaluator.EvaluatePoisonWait(bufferIo));

            /* Statement side for BtreePage: the profile counts, differenced across the window. */
            await SeedSamplingAsync(connection, ct, now.AddMinutes(-9), 111, 1_000);
            await SeedSamplingAsync(connection, ct, now.AddMinutes(-1), 111, 1_480);
            await SeedSamplingAsync(connection, ct, now.AddMinutes(-9), 222, 100);
            await SeedSamplingAsync(connection, ct, now.AddMinutes(-1), 222, 130);
            await SeedSamplingAsync(connection, ct, now.AddMinutes(-9), 0, 5_000);
            await SeedSamplingAsync(connection, ct, now.AddMinutes(-1), 0, 9_000);
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_statement_text (server_id, queryid, query_text, first_seen, last_seen)
VALUES ($1, 111, 'INSERT INTO orders (id, note) VALUES (7, ''late'')', $2, $2)", ServerId, now.AddDays(-1));

            var full = await adapter.GetPoisonWaitDetailAsync(ServerId, "IPC", "BtreePage", McpDataDetail.Full, ct);
            Assert.Equal(new[] { "BtreePage", "BufferIo", "BufferMapping" }, full.TopEvents.Select(e => e.WaitEvent).ToArray());
            Assert.InRange(full.TopEvents[0].AvgWaiters, 2.3, 2.5);
            Assert.Equal(new long[] { 111, 222 }, full.Statements.Select(s => s.QueryId).ToArray());
            Assert.Equal(480, full.Statements[0].Count);
            Assert.Equal("pg_wait_sampling", full.Statements[0].Source);
            Assert.Contains("VALUES (7, 'late')", full.Statements[0].QueryText, StringComparison.Ordinal);
            Assert.Null(full.Statements[1].QueryText);

            var masked = await adapter.GetPoisonWaitDetailAsync(ServerId, "IPC", "BtreePage", McpDataDetail.MaskLiterals, ct);
            Assert.Contains("VALUES (?, '?')", masked.Statements[0].QueryText, StringComparison.Ordinal);
            Assert.DoesNotContain("late", masked.Statements[0].QueryText, StringComparison.Ordinal);

            var withheld = await adapter.GetPoisonWaitDetailAsync(ServerId, "IPC", "BtreePage", McpDataDetail.MetadataOnly, ct);
            Assert.Equal(2, withheld.Statements.Count);
            Assert.All(withheld.Statements, s => Assert.Null(s.QueryText));

            /* BufferIo has no profile rows, so the statements come from the session captures. */
            for (var i = 0; i < 3; i++)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_session_states (collection_id, collection_time, server_id, server_name, pid, state, wait_event_type, wait_event, query_id)
VALUES ($1, $2, $3, $4, $5, 'active', 'IPC', 'BufferIo', 333)",
                    CollectionIdGenerator.Next(), now.AddMinutes(-2 - i), ServerId, ServerName, 4000 + i);
            }

            var sessions = await adapter.GetPoisonWaitDetailAsync(ServerId, "IPC", "BufferIo", McpDataDetail.Full, ct);
            var statement = Assert.Single(sessions.Statements);
            Assert.Equal(333, statement.QueryId);
            Assert.Equal(3, statement.Count);
            Assert.Equal("pg_session_states", statement.Source);

            /* An event nothing was caught waiting on: no statements, and the text says so. */
            var none = await adapter.GetPoisonWaitDetailAsync(ServerId, "IPC", "ParallelFinish", McpDataDetail.Full, ct);
            Assert.Empty(none.Statements);
            Assert.Contains("none recorded in the window",
                PostgresAlertEvaluator.BuildPoisonWaitDetailText(none, "IPC:ParallelFinish"), StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    private static Task SeedWaitAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc, string waitType, string waitEvent, long deltaWaitTimeUs) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_wait_stats (collection_id, collection_time, server_id, server_name, wait_type, wait_event, delta_waits, delta_wait_time_us)
VALUES ($1, $2, $3, $4, $5, $6, 1000, $7)",
            CollectionIdGenerator.Next(), collectionTimeUtc, ServerId, ServerName, waitType, waitEvent, deltaWaitTimeUs);

    private static Task SeedSamplingAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc, long queryId, long sampleCount) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_wait_sampling (collection_id, collection_time, server_id, server_name, event_type, event, query_id, sample_count, profile_period_ms, backend_count)
VALUES ($1, $2, $3, $4, 'IPC', 'BtreePage', $5, $6, 10, 1)",
            CollectionIdGenerator.Next(), collectionTimeUtc, ServerId, ServerName, queryId, sampleCount);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var table in new[] { "pg_wait_stats", "pg_wait_sampling", "pg_session_states", "pg_statement_text", "servers" })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM {table} WHERE server_id = $1", ServerId);
        }
    }
}
