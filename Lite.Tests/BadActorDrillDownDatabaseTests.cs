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
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #8317, Lite's twin of <c>Darling.Tests.BadActorDrillDownDatabaseLiveTests</c>: one query hash running in two
/// databases. The BAD_ACTOR fact is scored per (database, hash) and its finding carries that database, so the
/// <c>bad_actor_query</c> drill-down must describe the same database's row, not whichever row the hash
/// aggregate happens to emit first. A finding that carries no database takes the database that spent the most
/// CPU on the hash. The fact collector emits one fact per hash, for the highest-scoring database.
///
/// <para>The cheap database is seeded first: 1,327 executions at 8.91 ms over an old parallel plan and a newer
/// serial one. The expensive database is seeded second: 1,186 executions at 710.74 ms on one serial plan.</para>
/// </summary>
public sealed class BadActorDrillDownDatabaseTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 83170;
    private const string ServerName = "SynthBadActorDbSrv";
    private const string CheapDb = "SynthBadActorCheapDb";
    private const string ExpensiveDb = "SynthBadActorExpensiveDb";
    private const string Hash = "0x8317HASH";
    private const string CheapParallelPlan = "0x8317PLANCHEAPPARALLEL";
    private const string CheapSerialPlan = "0x8317PLANCHEAPSERIAL";
    private const string ExpensivePlan = "0x8317PLANEXPENSIVE";
    private const string FactKey = "BAD_ACTOR_" + Hash;

    private const long CheapCpuPerExecUs = 8_910L;
    private const long ExpensiveExecutions = 1_186L;
    private const long ExpensiveCpuPerExecUs = 710_740L;

    /* One UtcNow read, truncated to the second, so a round-tripped timestamp compares equal. */
    private static readonly DateTime WindowEnd = TruncateToSeconds(DateTime.UtcNow);

    private static DateTime TruncateToSeconds(DateTime t) =>
        DateTime.SpecifyKind(new DateTime(t.Ticks - (t.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static readonly DateTime WindowStart = WindowEnd.AddHours(-4);
    private static readonly DateTime ParallelLastSeen = WindowEnd.AddHours(-3);

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public BadActorDrillDownDatabaseTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    private static AnalysisContext Context() => new()
    {
        ServerId = ServerId,
        ServerName = ServerName,
        TimeRangeStart = WindowStart,
        TimeRangeEnd = WindowEnd,
    };

    [Fact]
    public async Task TheDrillDown_DescribesTheFindingsOwnDatabase()
    {
        await ClearAsync();
        await SeedBothDatabasesAsync();

        /* The finding is about the expensive database: its row, its executions, its per-execution CPU, and a
           DOP history with one serial plan and nothing to note. */
        var expensive = await CollectBadActorAsync(ExpensiveDb);
        Assert.Equal(ExpensiveDb, expensive.GetProperty("database").GetString());
        Assert.Equal(ExpensiveExecutions, expensive.GetProperty("execution_count").GetInt64());
        Assert.Equal(710.74, expensive.GetProperty("avg_cpu_ms").GetDouble());
        Assert.Equal(1, expensive.GetProperty("max_dop").GetInt32());
        Assert.Equal(1, expensive.GetProperty("max_dop_any_plan").GetInt32());
        Assert.Equal(1L, expensive.GetProperty("plan_count").GetInt64());
        Assert.Equal(JsonValueKind.Null, expensive.GetProperty("dop_note").ValueKind);

        /* And a finding about the cheap database gets the cheap row, with its own parallel history. */
        var cheap = await CollectBadActorAsync(CheapDb);
        Assert.Equal(CheapDb, cheap.GetProperty("database").GetString());
        Assert.Equal(1_327L, cheap.GetProperty("execution_count").GetInt64());
        Assert.Equal(8.91, cheap.GetProperty("avg_cpu_ms").GetDouble());
        Assert.Equal(8, cheap.GetProperty("max_dop_any_plan").GetInt32());
        Assert.Equal(2L, cheap.GetProperty("plan_count").GetInt64());
        Assert.Equal(
            $"DOP 1 (a parallel plan ran at 8 until {ParallelLastSeen:yyyy-MM-dd}; 2 plans in window)",
            cheap.GetProperty("dop_note").GetString());
    }

    [Fact]
    public async Task TheDrillDown_WithoutADatabaseOnTheFinding_ReadsTheDatabaseThatSpentTheMostCpu()
    {
        await ClearAsync();
        await SeedBothDatabasesAsync();

        var row = await CollectBadActorAsync(null);
        Assert.Equal(ExpensiveDb, row.GetProperty("database").GetString());
        Assert.Equal(ExpensiveExecutions, row.GetProperty("execution_count").GetInt64());
        Assert.Equal(710.74, row.GetProperty("avg_cpu_ms").GetDouble());
        Assert.Equal(JsonValueKind.Null, row.GetProperty("dop_note").ValueKind);
    }

    [Fact]
    public async Task ThePlanAnalysis_FetchesTheFindingsOwnDatabasesPlan()
    {
        await ClearAsync();
        await SeedBothDatabasesAsync();

        /* The cheap database's serial plan and the expensive database's plan sit at the same newest
           collection_time, so a lookup by hash alone has only the aggregate's order to choose by. */
        var expensive = new RecordingPlanFetcher();
        await new DrillDownCollector(_duckDb, expensive).EnrichFindingsAsync([NewFinding(ExpensiveDb)], Context());
        Assert.Equal(new[] { ExpensivePlan + "H" }, expensive.Handles);

        var cheap = new RecordingPlanFetcher();
        await new DrillDownCollector(_duckDb, cheap).EnrichFindingsAsync([NewFinding(CheapDb)], Context());
        Assert.Equal(new[] { CheapSerialPlan + "H" }, cheap.Handles);
    }

    [Fact]
    public async Task TheFactCollector_EmitsOneFactPerHash_ForTheHighestScoringDatabase()
    {
        await ClearAsync();
        await SeedBothDatabasesAsync();

        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(Context());

        /* Both databases clear the collector's bars, and both rank in its top five; one key, one fact. */
        var badActor = Assert.Single(facts, f => f.Key == FactKey);
        Assert.Equal(ExpensiveDb, badActor.DatabaseName);
        Assert.Equal(710.74, badActor.Value, 2);
        Assert.Equal(ExpensiveExecutions, (long)badActor.Metadata["execution_count"]);
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

    private async Task<JsonElement> CollectBadActorAsync(string? databaseName)
    {
        var finding = NewFinding(databaseName);

        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], Context());

        Assert.NotNull(finding.DrillDown);
        Assert.True(finding.DrillDown.TryGetValue("bad_actor_query", out var raw), "bad_actor_query was not collected");
        return JsonSerializer.SerializeToElement(raw);
    }

    /// <summary>The cheap database first, then the expensive one, as the issue's store held them.</summary>
    private async Task SeedBothDatabasesAsync()
    {
        /* 327 executions on a parallel plan three hours back, then 1,000 on the serial plan that replaced
           it: 1,327 executions at 8.91 ms, two plans, a DOP history. */
        await SeedAsync(CheapDb, CheapParallelPlan, ParallelLastSeen, maxDop: 8, executions: 327L, cpuPerExecUs: CheapCpuPerExecUs);
        await SeedAsync(CheapDb, CheapSerialPlan, WindowEnd.AddMinutes(-30), maxDop: 1, executions: 1_000L, cpuPerExecUs: CheapCpuPerExecUs);
        /* 1,186 executions at 710.74 ms on one serial plan. */
        await SeedAsync(ExpensiveDb, ExpensivePlan, WindowEnd.AddMinutes(-30), maxDop: 1, executions: ExpensiveExecutions, cpuPerExecUs: ExpensiveCpuPerExecUs);
    }

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task ClearAsync()
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "DELETE FROM query_stats WHERE server_id = $1";
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedAsync(string database, string planHash, DateTime collectionTime, long maxDop,
        long executions, long cpuPerExecUs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     creation_time, max_dop, min_dop, delta_execution_count, delta_worker_time, delta_elapsed_time,
     delta_logical_reads, delta_spills, query_text, plan_handle)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 1, $10, $11, $12, $13, 0, $14, $15)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = database });
        cmd.Parameters.Add(new DuckDBParameter { Value = Hash });
        cmd.Parameters.Add(new DuckDBParameter { Value = planHash });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime.AddDays(-1) });
        cmd.Parameters.Add(new DuckDBParameter { Value = maxDop });
        cmd.Parameters.Add(new DuckDBParameter { Value = executions });
        cmd.Parameters.Add(new DuckDBParameter { Value = executions * cpuPerExecUs });
        cmd.Parameters.Add(new DuckDBParameter { Value = executions * cpuPerExecUs * 2 });
        /* 1,000 reads per execution: enough to keep the cheap row past the collector's low-impact skip. */
        cmd.Parameters.Add(new DuckDBParameter { Value = executions * 1_000L });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SELECT * FROM dbo.SynthBadActorTable" });
        cmd.Parameters.Add(new DuckDBParameter { Value = planHash + "H" });
        await cmd.ExecuteNonQueryAsync();
    }
}
