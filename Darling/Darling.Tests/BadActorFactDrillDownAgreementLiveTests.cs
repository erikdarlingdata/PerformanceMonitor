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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8403: the BAD_ACTOR fact and its <c>bad_actor_query</c> drill-down state the same per-execution averages
/// for the same database, hash, window and execution count. A snapshot in which a long execution accrued CPU
/// and reads but no execution completed has <c>delta_execution_count</c> 0; the drill-down sums that
/// snapshot's work into the query's totals, and the fact must count the same rows, or the finding's headline
/// average sits under the drill-down printed beside it.
///
/// <para>The fixture is one hash in one database: a snapshot with 1,000 completions at 500 ms CPU and 100,000
/// reads each, then a snapshot with no completion and 300 s of CPU and 50 M reads. Over the window the query
/// did 800 s of CPU and 150 M reads across 1,000 executions: 800 ms and 150,000 reads each. A second hash
/// whose only rows carry no completions stays below the fact's execution bar.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class BadActorFactDrillDownAgreementLiveTests
{
    private const int TestServerId = -840300;
    private const string TestServerName = "BadActorAgreeSrv";
    private const string Database = "BadActorAgreeDb";
    private const string Hash = "0x8403HASH";
    private const string Plan = "0x8403PLAN";
    private const string FactKey = "BAD_ACTOR_" + Hash;
    private const string NoCompletionHash = "0x8403NOCOMPLETION";

    private const long Executions = 1_000L;
    private const long CompletedCpuUs = 500_000_000L;
    private const long CompletedReads = 100_000_000L;
    private const long InFlightCpuUs = 300_000_000L;
    private const long InFlightReads = 50_000_000L;

    private const double ExpectedAvgCpuMs = (CompletedCpuUs + InFlightCpuUs) / (double)Executions / 1000.0;
    private const double ExpectedAvgReads = (CompletedReads + InFlightReads) / (double)Executions;
    private const long ExpectedTotalCpuUs = CompletedCpuUs + InFlightCpuUs;
    private const long ExpectedTotalReads = CompletedReads + InFlightReads;

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

    private static async Task SeedAsync(
        NpgsqlConnection c, string hash, DateTime collectionTime, long executions, long cpuUs, long reads, CancellationToken ct)
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
        command.Parameters.AddWithValue(Database);
        command.Parameters.AddWithValue(hash);
        command.Parameters.AddWithValue(Plan);
        command.Parameters.AddWithValue("0x8403SQLH");
        command.Parameters.AddWithValue(Plan + "H");
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime.AddDays(-1), DateTimeKind.Unspecified));
        command.Parameters.AddWithValue("SELECT * FROM BadActorAgreeTable");
        command.Parameters.AddWithValue(executions);
        command.Parameters.AddWithValue(cpuUs);
        command.Parameters.AddWithValue(cpuUs * 2);
        command.Parameters.AddWithValue(reads);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue(1);
        command.Parameters.AddWithValue(1);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("DELETE FROM query_stats WHERE server_id = $1", connection);
        command.Parameters.AddWithValue(TestServerId);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static AnalysisFinding NewFinding() => new()
    {
        RootFactKey = FactKey,
        StoryPath = FactKey,
        PathKeys = [FactKey],
        /* Past the display gate, below which the expensive drill-downs are skipped wholesale. */
        Severity = 1.0,
        /* What InferenceEngine copies from the root fact: the database the fact was scored on. */
        DatabaseName = Database,
    };

    private static async Task<JsonElement> CollectDrillDownAsync(NpgsqlDataSource postgres, AnalysisContext context)
    {
        var finding = NewFinding();
        await new PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], context);

        Assert.NotNull(finding.DrillDown);
        Assert.True(finding.DrillDown.TryGetValue("bad_actor_query", out var raw), "bad_actor_query was not collected");
        return JsonSerializer.SerializeToElement(raw);
    }

    /// <summary>
    /// Migrates, clears the test server's rows, seeds the completed snapshot, the completion-less snapshot and
    /// the completion-less-only hash, then runs <paramref name="body"/> over a four-hour window.
    /// </summary>
    private static async Task RunSeededAsync(Func<string, AnalysisContext, Task> body)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #8403 bad-actor agreement test.");

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
            var periodStart = periodEnd.AddHours(-4);
            var context = new AnalysisContext
            {
                ServerId = TestServerId,
                ServerName = TestServerName,
                TimeRangeStart = periodStart,
                TimeRangeEnd = periodEnd,
                ServerUtcOffset = TimeSpan.Zero,
            };

            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                /* The snapshot in which the executions completed, then the one in which a long execution ran
                   on without completing. */
                await SeedAsync(connection, Hash, periodEnd.AddMinutes(-30), Executions, CompletedCpuUs, CompletedReads, ct);
                await SeedAsync(connection, Hash, periodEnd.AddMinutes(-20), 0L, InFlightCpuUs, InFlightReads, ct);
                /* A hash that never completed in the window: 600 s of CPU, no executions. */
                await SeedAsync(connection, NoCompletionHash, periodEnd.AddMinutes(-20), 0L, 600_000_000L, 1_000_000L, ct);
            }

            await body(connectionString!, context);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteTestRowsAsync);
        }
    }

    [Fact]
    public Task TheFact_CountsTheWorkOfSnapshotsWithoutACompletion() => RunSeededAsync(async (connectionString, context) =>
    {
        await using var postgres = NpgsqlDataSource.Create(connectionString);

        var facts = await new PgFactCollector(postgres).CollectFactsAsync(context);
        var fact = Assert.Single(facts, f => f.Key == FactKey);

        Assert.Equal(Database, fact.DatabaseName);
        Assert.Equal(Executions, (long)fact.Metadata["execution_count"]);
        Assert.Equal(ExpectedAvgCpuMs, fact.Value, 2);
        Assert.Equal(ExpectedAvgCpuMs, fact.Metadata["avg_cpu_ms"], 2);
        Assert.Equal(ExpectedAvgReads, fact.Metadata["avg_reads"], 0);
        Assert.Equal(ExpectedTotalCpuUs, (long)fact.Metadata["total_cpu_us"]);
        Assert.Equal(ExpectedTotalReads, (long)fact.Metadata["total_reads"]);
    });

    [Fact]
    public Task TheFactAndItsDrillDown_StateTheSameFigures() => RunSeededAsync(async (connectionString, context) =>
    {
        await using var postgres = NpgsqlDataSource.Create(connectionString);

        var facts = await new PgFactCollector(postgres).CollectFactsAsync(context);
        var fact = Assert.Single(facts, f => f.Key == FactKey);
        var drillDown = await CollectDrillDownAsync(postgres, context);

        Assert.Equal(fact.DatabaseName, drillDown.GetProperty("database").GetString());
        Assert.Equal((long)fact.Metadata["execution_count"], drillDown.GetProperty("execution_count").GetInt64());
        Assert.Equal(fact.Metadata["avg_cpu_ms"], drillDown.GetProperty("avg_cpu_ms").GetDouble(), 2);
        Assert.Equal(fact.Metadata["avg_reads"], drillDown.GetProperty("avg_reads").GetDouble(), 0);
        Assert.Equal(fact.Metadata["total_cpu_us"] / 1000.0, drillDown.GetProperty("total_cpu_ms").GetDouble(), 3);
        Assert.Equal((long)fact.Metadata["total_reads"], drillDown.GetProperty("total_reads").GetInt64());

        /* And the drill-down's own figures are the window's whole work. */
        Assert.Equal(ExpectedAvgCpuMs, drillDown.GetProperty("avg_cpu_ms").GetDouble(), 2);
        Assert.Equal(ExpectedAvgReads, drillDown.GetProperty("avg_reads").GetDouble(), 0);
    });

    [Fact]
    public Task AHashWithNoCompletionInTheWindow_IsNotAFact() => RunSeededAsync(async (connectionString, context) =>
    {
        await using var postgres = NpgsqlDataSource.Create(connectionString);

        var facts = await new PgFactCollector(postgres).CollectFactsAsync(context);

        /* The execution bar still gates on completions: work with no execution behind it is not a per-execution
           average, however much CPU it spent. */
        Assert.DoesNotContain(facts, f => f.Key == "BAD_ACTOR_" + NoCompletionHash);
    });
}
