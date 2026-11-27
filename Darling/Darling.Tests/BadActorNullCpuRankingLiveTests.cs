/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9879: the bad actor reads rank by a CPU score, and PostgreSQL sorts a NULL first under DESC. A (database, hash)
/// group whose CPU deltas are all NULL (an unknowable counter, #4394) scores NULL, so without NULLS LAST it outranks
/// every group with a known score and takes a place in the five. Each test plants such a group with 5,000 executions
/// and a high reads average, so it clears every bar of the collector, beside groups whose scores are known.
///
/// <para><b>What the five contain.</b> With five known groups the NULL group is not among the five rows and not among
/// the BAD_ACTOR facts, and the fifth known group keeps its place. With two known groups the NULL group is the last row.
/// The <c>bad_actor_query</c> drill-down of a finding that names no database takes the database with a known CPU sum,
/// not the one whose sum is NULL.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class BadActorNullCpuRankingLiveTests
{
    private const int TestServerId = -987900;
    private const string TestServerName = "BadActorNullCpuSrv";
    private const string KnownDb = "NullCpuKnownDb";
    private const string UnknownDb = "NullCpuUnknownDb";
    private const string UnknownHash = "0x9879UNKNOWN";
    private const string DetailHash = "0x9879DETAIL";

    private const long UnknownExecutions = 5_000L;

    private static string KnownHash(int rank) => $"0x9879KNOWN{rank}";

    private static DateTime TruncateToSeconds(DateTime t) =>
        DateTime.SpecifyKind(new DateTime(t.Ticks - (t.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static async Task<NpgsqlConnection> OpenWithSearchPathAsync(string connectionString, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var setPath = new NpgsqlCommand("SET search_path = " + PgSchemaGenerator.SearchPath, connection);
        await setPath.ExecuteNonQueryAsync(ct);
        return connection;
    }

    /// <summary>One raw row. A null <paramref name="cpuUs"/> is a row whose CPU delta the store holds as NULL.</summary>
    private static async Task SeedAsync(
        NpgsqlConnection c, string database, string hash, DateTime collectionTime,
        long executions, long? cpuUs, long reads, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     sql_handle, plan_handle, creation_time, query_text, delta_execution_count, delta_worker_time,
     delta_elapsed_time, delta_logical_reads, delta_spills, min_dop, max_dop)
VALUES
    ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)", c);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(TestServerId);
        command.Parameters.AddWithValue(TestServerName);
        command.Parameters.AddWithValue(database);
        command.Parameters.AddWithValue(hash);
        command.Parameters.AddWithValue(hash + "P");
        command.Parameters.AddWithValue("0x9879SQLH");
        command.Parameters.AddWithValue(hash + "H");
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime.AddDays(-1), DateTimeKind.Unspecified));
        command.Parameters.AddWithValue("SELECT * FROM NullCpuTable");
        command.Parameters.AddWithValue(executions);
        command.Parameters.AddWithValue(cpuUs is { } cpu ? (object)cpu : DBNull.Value);
        command.Parameters.AddWithValue(executions * 1_000L);
        command.Parameters.AddWithValue(reads);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue(1);
        command.Parameters.AddWithValue(1);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Known group <paramref name="rank"/> (0 is the lowest score): two rows, 200 + 100 * rank executions in all at
    /// (20 + 10 * rank) ms of CPU each, 10 reads an execution. The score is the average CPU times the log of the
    /// executions, so it rises with the rank, and each group clears the collector's 10 ms bar.
    /// </summary>
    private static async Task SeedKnownGroupAsync(NpgsqlConnection c, int rank, DateTime periodEnd, CancellationToken ct)
    {
        var rowExecutions = 100L + 50L * rank;
        var cpuPerExecutionUs = (20L + 10L * rank) * 1_000L;
        await SeedAsync(c, KnownDb, KnownHash(rank), periodEnd.AddMinutes(-90), rowExecutions, rowExecutions * cpuPerExecutionUs, rowExecutions * 10L, ct);
        await SeedAsync(c, KnownDb, KnownHash(rank), periodEnd.AddMinutes(-30), rowExecutions, rowExecutions * cpuPerExecutionUs, rowExecutions * 10L, ct);
    }

    /// <summary>
    /// The group no CPU delta is known for: five rows of 1,000 executions, a NULL CPU delta in each, 2,000 reads an
    /// execution so the fact collector keeps it. Its score is NULL.
    /// </summary>
    private static async Task SeedUnknownGroupAsync(NpgsqlConnection c, string database, string hash, long rowExecutions, DateTime periodEnd, CancellationToken ct)
    {
        for (var minute = 10; minute <= 50; minute += 10)
        {
            await SeedAsync(c, database, hash, periodEnd.AddMinutes(-minute), rowExecutions, null, rowExecutions * 2_000L, ct);
        }
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("DELETE FROM query_stats WHERE server_id = $1", connection);
        command.Parameters.AddWithValue(TestServerId);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Migrates, clears the test server's rows, plants with <paramref name="plant"/>, then runs <paramref name="body"/> with the window.</summary>
    private static async Task RunAsync(
        Func<NpgsqlConnection, DateTime, CancellationToken, Task> plant,
        Func<string, AnalysisContext, Task> body)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #9879 NULL CPU ranking test.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;

        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
        {
            await DeleteTestRowsAsync(connection, ct);
        }

        try
        {
            var periodEnd = TruncateToSeconds(DateTime.UtcNow);
            var context = new AnalysisContext
            {
                ServerId = TestServerId,
                ServerName = TestServerName,
                TimeRangeStart = periodEnd.AddHours(-4),
                TimeRangeEnd = periodEnd,
                ServerUtcOffset = TimeSpan.Zero,
            };

            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                await plant(connection, periodEnd, ct);
            }

            await body(connectionString!, context);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteTestRowsAsync);
        }
    }

    /// <summary>The query hashes <see cref="PgFactCollector.BadActorSql"/> returns, in the order it returns them.</summary>
    private static async Task<List<string>> ReadTheRankingAsync(string connectionString, AnalysisContext context)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenWithSearchPathAsync(connectionString, ct);
        await using var command = new NpgsqlCommand(PgFactCollector.BadActorSql, connection);
        command.Parameters.AddWithValue(context.ServerId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(context.TimeRangeStart, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(context.TimeRangeEnd, DateTimeKind.Unspecified));

        var hashes = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            hashes.Add(reader.GetString(1));
        }

        return hashes;
    }

    [Fact]
    public Task AGroupWithOnlyNullCpuDeltas_IsNotInTheFive_WhenFiveGroupsHaveKnownScores() => RunAsync(
        async (connection, periodEnd, ct) =>
        {
            for (var rank = 0; rank < 5; rank++)
            {
                await SeedKnownGroupAsync(connection, rank, periodEnd, ct);
            }

            await SeedUnknownGroupAsync(connection, KnownDb, UnknownHash, 1_000L, periodEnd, ct);
        },
        async (connectionString, context) =>
        {
            /* The read returns the five known groups, the highest score first. The NULL group has 5,000 executions
               and a score of NULL: it is not one of the five, and the fifth known group keeps its place. */
            var ranked = await ReadTheRankingAsync(connectionString, context);
            Assert.Equal(new[] { KnownHash(4), KnownHash(3), KnownHash(2), KnownHash(1), KnownHash(0) }, ranked);

            /* The collector's facts are the same five: the NULL group's high reads average would clear the
               collector's bars, so the ranking alone keeps it out. */
            await using var postgres = NpgsqlDataSource.Create(connectionString);
            var facts = await new PgFactCollector(postgres).CollectFactsAsync(context);
            var keys = facts
                .Where(f => f.Key.StartsWith("BAD_ACTOR_", StringComparison.Ordinal))
                .Select(f => f.Key)
                .OrderBy(k => k, StringComparer.Ordinal)
                .ToArray();
            var expected = Enumerable.Range(0, 5)
                .Select(rank => "BAD_ACTOR_" + KnownHash(rank))
                .OrderBy(k => k, StringComparer.Ordinal)
                .ToArray();
            Assert.Equal(expected, keys);
        });

    [Fact]
    public Task AGroupWithOnlyNullCpuDeltas_IsLast_WhenFewerThanFiveGroupsHaveKnownScores() => RunAsync(
        async (connection, periodEnd, ct) =>
        {
            await SeedKnownGroupAsync(connection, 0, periodEnd, ct);
            await SeedKnownGroupAsync(connection, 1, periodEnd, ct);
            await SeedUnknownGroupAsync(connection, KnownDb, UnknownHash, 1_000L, periodEnd, ct);
        },
        async (connectionString, context) =>
        {
            var ranked = await ReadTheRankingAsync(connectionString, context);
            Assert.Equal(new[] { KnownHash(1), KnownHash(0), UnknownHash }, ranked);
        });

    [Fact]
    public Task TheDrillDown_WithoutADatabaseOnTheFinding_SkipsTheDatabaseWhoseCpuSumIsNull() => RunAsync(
        async (connection, periodEnd, ct) =>
        {
            /* One hash in two databases: 400 executions at 20 ms in the known database, 5,000 executions with
               NULL CPU deltas in the other. */
            await SeedAsync(connection, KnownDb, DetailHash, periodEnd.AddMinutes(-90), 200L, 200L * 20_000L, 2_000L, ct);
            await SeedAsync(connection, KnownDb, DetailHash, periodEnd.AddMinutes(-30), 200L, 200L * 20_000L, 2_000L, ct);
            await SeedUnknownGroupAsync(connection, UnknownDb, DetailHash, 1_000L, periodEnd, ct);
        },
        async (connectionString, context) =>
        {
            var factKey = "BAD_ACTOR_" + DetailHash;
            var finding = new AnalysisFinding
            {
                RootFactKey = factKey,
                StoryPath = factKey,
                PathKeys = [factKey],
                Severity = 1.0,
                DatabaseName = null,
            };

            await using var postgres = NpgsqlDataSource.Create(connectionString);
            await new PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], context);

            Assert.NotNull(finding.DrillDown);
            Assert.True(finding.DrillDown.TryGetValue("bad_actor_query", out var raw), "bad_actor_query was not collected");
            var row = JsonSerializer.SerializeToElement(raw);
            Assert.Equal(KnownDb, row.GetProperty("database").GetString());
            Assert.Equal(400L, row.GetProperty("execution_count").GetInt64());
            Assert.Equal(20.0, row.GetProperty("avg_cpu_ms").GetDouble());
        });
}
