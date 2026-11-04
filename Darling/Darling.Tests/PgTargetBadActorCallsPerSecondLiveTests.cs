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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7566, the bad-actor fact: the <c>PG_BAD_ACTOR_&lt;queryid&gt;</c> fact's <c>calls_per_sec</c> and the advice that
/// quotes it divide a statement's calls by the seconds the collector's runs covered in the window, the rule the
/// trend uses, not by the seconds of only the snapshots where the statement had a row. The collector skips idle rows
/// at the write, so a statement that runs a batch every 5 minutes has a row in one run of five. The old read gave
/// that statement 29 / 60 = 0.48 per second where the trend charts 29 / 300 = 0.097.
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetBadActorCallsPerSecondLiveTests
{
    private const string ServerName = "pg-bad-actor-calls-per-second";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const long QueryId = 75660;
    private static readonly DateTime T0 = new(2026, 3, 6, 10, 0, 0);
    private static readonly DateTime WindowStart = new(2026, 3, 6, 9, 0, 0);
    private static readonly DateTime WindowEnd = new(2026, 3, 6, 12, 0, 0);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// Runs every minute for an hour from 10:00, and 29 calls every 5 minutes (12 batches, 348 calls). The runs cover
    /// 3,600 s, so the rate is 348 / 3,600 = 0.0967 per second, the number the trend reads for the same hour and the one
    /// the advice quotes. The old denominator was the 12 snapshots' own 720 s: 0.4833.
    /// </summary>
    [Fact]
    public async Task ABatchEveryFiveMinutes_ReadsTheRateOverTheRunsSeconds_InTheFact_TheAdviceAndTheTrend()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            for (var t = T0; t < T0.AddMinutes(60); t = t.AddMinutes(1))
            {
                await SeedRunAsync(connection, ct, t);
            }

            for (var t = T0; t < T0.AddMinutes(60); t = t.AddMinutes(5))
            {
                await SeedStatementAsync(connection, ct, t, deltaCalls: 29, deltaMs: 290.0);
            }

            var fact = await ReadFactAsync(postgres, ct);

            Assert.Equal(348, fact.Metadata["calls"]);
            Assert.Equal(348.0 / 3600.0, fact.Metadata["calls_per_sec"], 9);

            var bucket = Assert.Single(
                await DarlingPgTrendReader.GetQueryDurationBucketsAsync(postgres, ServerId, QueryId, WindowStart, WindowEnd, 60, ct),
                p => p.Calls > 0);
            Assert.Equal(bucket.CallsPerSecond, fact.Metadata["calls_per_sec"], 9);

            var advice = PgTargetAdvice.Compose(fact.Key, new[] { fact }.ToFactLookup())!;
            Assert.Contains("(0.1/s over the time the collector's runs covered)", advice.Investigation, StringComparison.Ordinal);
            Assert.DoesNotContain("0.48/s", advice.Investigation, StringComparison.Ordinal);
        });
    }

    /// <summary>
    /// A restart snapshot (interval 0) is left out of the statement's rate with its calls: runs 10:00 to 10:09 with 10
    /// calls each and 500 at the 10:05 restart. Nine steady runs cover 540 s and carry 90 calls.
    /// </summary>
    [Fact]
    public async Task ARestartSnapshotThatCarriesCalls_IsLeftOutOfTheFactsCallsAndSeconds()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            for (var t = T0; t < T0.AddMinutes(10); t = t.AddMinutes(1))
            {
                await SeedRunAsync(connection, ct, t);
                var restart = t == T0.AddMinutes(5);
                await SeedStatementAsync(connection, ct, t, deltaCalls: restart ? 500 : 10, deltaMs: 100.0, intervalSeconds: restart ? 0 : 60);
            }

            var fact = await ReadFactAsync(postgres, ct);
            Assert.Equal(90.0 / 540.0, fact.Metadata["calls_per_sec"], 9);
        });
    }

    /// <summary>
    /// The first run after an outage past the collector's gap policy has no trustworthy length, so its calls and
    /// seconds are left out. Runs 10:00 to 10:09, an outage, then runs 11:31 to 11:39; 29 calls in the 11:31 run and 7
    /// in the 11:35 run. The rated runs are 10:01 to 10:09 and 11:32 to 11:39, 1,020 s, and the 7 calls.
    /// </summary>
    [Fact]
    public async Task TheFirstRunAfterAnOutage_ThatCarriesCalls_IsLeftOutOfTheFactsCallsAndSeconds()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            for (var t = T0; t < T0.AddMinutes(10); t = t.AddMinutes(1))
            {
                await SeedRunAsync(connection, ct, t);
            }

            for (var t = T0.AddMinutes(91); t < T0.AddMinutes(100); t = t.AddMinutes(1))
            {
                await SeedRunAsync(connection, ct, t);
            }

            await SeedStatementAsync(connection, ct, T0.AddMinutes(91), deltaCalls: 29, deltaMs: 290.0);
            await SeedStatementAsync(connection, ct, T0.AddMinutes(95), deltaCalls: 7, deltaMs: 70.0);

            var fact = await ReadFactAsync(postgres, ct);
            Assert.Equal(7.0 / 1020.0, fact.Metadata["calls_per_sec"], 9);
        });
    }

    /// <summary>A store with no run log rates a snapshot on its own stored interval, the rule before #7566.</summary>
    [Fact]
    public async Task WithNoRunLog_TheFactKeepsTheStoredIntervalRule()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            for (var t = T0; t < T0.AddMinutes(60); t = t.AddMinutes(5))
            {
                await SeedStatementAsync(connection, ct, t, deltaCalls: 29, deltaMs: 290.0, intervalSeconds: 300);
            }

            var fact = await ReadFactAsync(postgres, ct);
            Assert.Equal(348.0 / 3600.0, fact.Metadata["calls_per_sec"], 9);
        });
    }

    private static async Task<Fact> ReadFactAsync(NpgsqlDataSource postgres, CancellationToken ct)
    {
        var context = new AnalysisContext
        {
            ServerId = ServerId,
            ServerName = ServerName,
            TimeRangeStart = WindowStart,
            TimeRangeEnd = WindowEnd,
            ServerUtcOffset = TimeSpan.Zero,
        };
        var facts = await new PgTargetFactCollector(postgres).CollectFactsAsync(context);
        return Assert.Single(facts, f => f.Key == PgTargetFactKeys.BadActorKey(QueryId));
    }

    private static async Task SeedRunAsync(NpgsqlConnection connection, CancellationToken ct, DateTime start) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected) VALUES ($1, $2, $3, 'pg_statement_stats', $4, 1500, 'SUCCESS', 0)",
            CollectionIdGenerator.Next(), ServerId, ServerName, start.AddSeconds(2));

    private static async Task SeedStatementAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime t, long deltaCalls, double deltaMs, int intervalSeconds = 60) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO pg_statement_stats (collection_id, collection_time, server_id, server_name, queryid, database_id, user_id, toplevel, calls, total_exec_time_ms, min_exec_time_ms, max_exec_time_ms, mean_exec_time_ms, rows_returned, shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written, temp_blks_read, temp_blks_written, blk_read_time_ms, blk_write_time_ms, wal_records, wal_fpi, wal_bytes, delta_calls, delta_total_exec_time_ms, delta_rows, sample_interval_seconds) VALUES ($1, $2, $3, $4, $5, 1, 1, TRUE, 10, 100.0, 1.0, 20.0, 10.0, 100, 100, 10, 0, 0, 0, 0, 1.0, 0.0, 0, 0, 0, $6, $7, 50, $8)",
            CollectionIdGenerator.Next(), t, ServerId, ServerName, QueryId, deltaCalls, deltaMs, intervalSeconds);

    private static async Task RunAsync(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live PostgreSQL bad-actor calls-per-second tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, ServerId, ServerName, "postgres", 18, ct);

            /* The coverage witness reads pg_database_stats: one row a minute across the window. */
            for (var t = WindowStart; t <= WindowEnd; t = t.AddMinutes(1))
            {
                await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, ServerId, ServerName, t, ct);
            }

            await body(connection, postgres, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var table in new[] { "collect.collection_log", "pg_statement_stats", "pg_database_stats", "analysis_findings", "analysis_muted", "servers" })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM {table} WHERE server_id = $1", ServerId);
        }
    }
}
