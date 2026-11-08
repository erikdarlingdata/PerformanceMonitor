/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
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
/// #8317: one query hash running in two databases. The BAD_ACTOR fact is scored per (database, hash) and its
/// finding carries that database, so the <c>bad_actor_query</c> drill-down must describe the same database's
/// row, not whichever row the hash aggregate happens to emit first. A finding that carries no database takes
/// the database that spent the most CPU on the hash. The fact collector emits one fact per hash, for the
/// highest-scoring database, so two databases' rows for one hash never become two facts under one key.
///
/// <para><b>The fixture is the issue's page.</b> The cheap database is seeded first: 1,327 executions at
/// 8.91 ms, spread over an old parallel plan and a newer serial one. The expensive database is seeded second:
/// 1,186 executions at 710.74 ms on one serial plan. Live rather than a string pin because the claim is which
/// row the engine returns for the hash, and that the DOP history beside it is the same database's.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class BadActorDrillDownDatabaseLiveTests
{
    private const int TestServerId = -831700;
    private const string TestServerName = "BadActorDbSrv";
    private const string CheapDb = "BadActorCheapDb";
    private const string ExpensiveDb = "BadActorExpensiveDb";
    private const string Hash = "0x8317HASH";
    private const string CheapParallelPlan = "0x8317PLANCHEAPPARALLEL";
    private const string CheapSerialPlan = "0x8317PLANCHEAPSERIAL";
    private const string ExpensivePlan = "0x8317PLANEXPENSIVE";
    private const string FactKey = "BAD_ACTOR_" + Hash;

    private const long CheapCpuPerExecUs = 8_910L;
    private const long ExpensiveExecutions = 1_186L;
    private const long ExpensiveCpuPerExecUs = 710_740L;

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
        NpgsqlConnection c, string database, string planHash, DateTime collectionTime, int maxDop,
        long executions, long cpuPerExecUs, CancellationToken ct)
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
        command.Parameters.AddWithValue(Hash);
        command.Parameters.AddWithValue(planHash);
        command.Parameters.AddWithValue("0x8317SQLH");
        command.Parameters.AddWithValue(planHash + "H");
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime.AddDays(-1), DateTimeKind.Unspecified));
        command.Parameters.AddWithValue("SELECT * FROM BadActorTable");
        command.Parameters.AddWithValue(executions);
        command.Parameters.AddWithValue(executions * cpuPerExecUs);
        command.Parameters.AddWithValue(executions * cpuPerExecUs * 2);
        /* 1,000 reads per execution: enough to keep the cheap row past the collector's low-impact skip. */
        command.Parameters.AddWithValue(executions * 1_000L);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue(1);
        command.Parameters.AddWithValue(maxDop);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("DELETE FROM query_stats WHERE server_id = $1", connection);
        command.Parameters.AddWithValue(TestServerId);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static AnalysisFinding NewFinding(string? databaseName) => new()
    {
        RootFactKey = FactKey,
        StoryPath = FactKey,
        PathKeys = [FactKey],
        /* Past the display gate, below which the expensive drill-downs are skipped wholesale. */
        Severity = 1.0,
        /* What InferenceEngine copies from the root fact: the database the fact was scored on. */
        DatabaseName = databaseName,
    };

    /// <summary>Records the plan handles the plan analysis asks for and returns no plan.</summary>
    private sealed class RecordingPlanFetcher : IPlanFetcher
    {
        public List<string> Handles { get; } = [];

        public Task<string?> FetchPlanXmlAsync(int serverId, string planHandle, CancellationToken cancellationToken)
        {
            Handles.Add(planHandle);
            return Task.FromResult<string?>(null);
        }
    }

    private static async Task<JsonElement> CollectBadActorAsync(
        NpgsqlDataSource postgres, AnalysisContext context, string? databaseName)
    {
        var finding = NewFinding(databaseName);

        await new PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], context);

        Assert.NotNull(finding.DrillDown);
        Assert.True(finding.DrillDown.TryGetValue("bad_actor_query", out var raw), "bad_actor_query was not collected");
        return JsonSerializer.SerializeToElement(raw);
    }

    /// <summary>
    /// Migrates, clears the test server's rows, seeds the cheap database first and the expensive one second,
    /// then runs <paramref name="body"/> with the window and the parallel plan's last snapshot time.
    /// </summary>
    private static async Task RunSeededAsync(Func<string, AnalysisContext, DateTime, Task> body)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #8317 bad-actor database test.");

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
            var parallelLastSeen = periodEnd.AddHours(-3);
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
                /* The cheap database first: 327 executions on a parallel plan three hours back, then 1,000 on
                   the serial plan that replaced it. 1,327 executions at 8.91 ms, two plans, a DOP history. */
                await SeedAsync(connection, CheapDb, CheapParallelPlan, parallelLastSeen, 8, 327L, CheapCpuPerExecUs, ct);
                await SeedAsync(connection, CheapDb, CheapSerialPlan, periodEnd.AddMinutes(-30), 1, 1_000L, CheapCpuPerExecUs, ct);
                /* The expensive database second: 1,186 executions at 710.74 ms on one serial plan. */
                await SeedAsync(connection, ExpensiveDb, ExpensivePlan, periodEnd.AddMinutes(-30), 1, ExpensiveExecutions, ExpensiveCpuPerExecUs, ct);
            }

            await body(connectionString!, context, parallelLastSeen);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteTestRowsAsync);
        }
    }

    [Fact]
    public Task TheDrillDown_DescribesTheFindingsOwnDatabase() => RunSeededAsync(async (connectionString, context, parallelLastSeen) =>
    {
        await using var postgres = NpgsqlDataSource.Create(connectionString);

        /* The finding is about the expensive database: its row, its executions, its per-execution CPU, and a
           DOP history with one serial plan and nothing to note. */
        var expensive = await CollectBadActorAsync(postgres, context, ExpensiveDb);
        Assert.Equal(ExpensiveDb, expensive.GetProperty("database").GetString());
        Assert.Equal(ExpensiveExecutions, expensive.GetProperty("execution_count").GetInt64());
        Assert.Equal(710.74, expensive.GetProperty("avg_cpu_ms").GetDouble());
        Assert.Equal(1, expensive.GetProperty("max_dop").GetInt32());
        Assert.Equal(1, expensive.GetProperty("max_dop_any_plan").GetInt32());
        Assert.Equal(1L, expensive.GetProperty("plan_count").GetInt64());
        Assert.Equal(JsonValueKind.Null, expensive.GetProperty("dop_note").ValueKind);

        /* And a finding about the cheap database gets the cheap row, with its own parallel history. */
        var cheap = await CollectBadActorAsync(postgres, context, CheapDb);
        Assert.Equal(CheapDb, cheap.GetProperty("database").GetString());
        Assert.Equal(1_327L, cheap.GetProperty("execution_count").GetInt64());
        Assert.Equal(8.91, cheap.GetProperty("avg_cpu_ms").GetDouble());
        Assert.Equal(8, cheap.GetProperty("max_dop_any_plan").GetInt32());
        Assert.Equal(2L, cheap.GetProperty("plan_count").GetInt64());
        Assert.Equal(
            $"DOP 1 (a parallel plan ran at 8 until {parallelLastSeen:yyyy-MM-dd}; 2 plans in window)",
            cheap.GetProperty("dop_note").GetString());
    });

    [Fact]
    public Task TheDrillDown_WithoutADatabaseOnTheFinding_ReadsTheDatabaseThatSpentTheMostCpu() => RunSeededAsync(async (connectionString, context, _) =>
    {
        await using var postgres = NpgsqlDataSource.Create(connectionString);

        var row = await CollectBadActorAsync(postgres, context, null);
        Assert.Equal(ExpensiveDb, row.GetProperty("database").GetString());
        Assert.Equal(ExpensiveExecutions, row.GetProperty("execution_count").GetInt64());
        Assert.Equal(710.74, row.GetProperty("avg_cpu_ms").GetDouble());
        Assert.Equal(JsonValueKind.Null, row.GetProperty("dop_note").ValueKind);
    });

    [Fact]
    public Task ThePlanAnalysis_FetchesTheFindingsOwnDatabasesPlan() => RunSeededAsync(async (connectionString, context, _) =>
    {
        await using var postgres = NpgsqlDataSource.Create(connectionString);

        /* The cheap database's serial plan and the expensive database's plan sit at the same newest
           collection_time, so a lookup by hash alone has only the aggregate's order to choose by. */
        var expensive = new RecordingPlanFetcher();
        await new PgDrillDownCollector(postgres, planFetcher: expensive).EnrichFindingsAsync([NewFinding(ExpensiveDb)], context);
        Assert.Equal(new[] { ExpensivePlan + "H" }, expensive.Handles);

        var cheap = new RecordingPlanFetcher();
        await new PgDrillDownCollector(postgres, planFetcher: cheap).EnrichFindingsAsync([NewFinding(CheapDb)], context);
        Assert.Equal(new[] { CheapSerialPlan + "H" }, cheap.Handles);
    });

    [Fact]
    public Task TheFactCollector_EmitsOneFactPerHash_ForTheHighestScoringDatabase() => RunSeededAsync(async (connectionString, context, _) =>
    {
        await using var postgres = NpgsqlDataSource.Create(connectionString);

        var facts = await new PgFactCollector(postgres).CollectFactsAsync(context);

        /* Both databases clear the collector's bars, and both rank in its top five; one key, one fact. */
        var badActor = Assert.Single(facts, f => f.Key == FactKey);
        Assert.Equal(ExpensiveDb, badActor.DatabaseName);
        Assert.Equal(710.74, badActor.Value, 2);
        Assert.Equal(ExpensiveExecutions, (long)badActor.Metadata["execution_count"]);
    });
}
