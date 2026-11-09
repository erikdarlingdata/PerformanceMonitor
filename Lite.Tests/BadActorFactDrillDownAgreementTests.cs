/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #8403, Lite's twin of <c>Darling.Tests.BadActorFactDrillDownAgreementLiveTests</c>: the BAD_ACTOR fact and its
/// <c>bad_actor_query</c> drill-down state the same per-execution averages for the same database, hash, window
/// and execution count. A snapshot in which a long execution accrued CPU and reads but no execution completed
/// has <c>delta_execution_count</c> 0; the drill-down sums that snapshot's work into the query's totals, and
/// the fact must count the same rows.
///
/// <para>One hash in one database: a snapshot with 1,000 completions at 500 ms CPU and 100,000 reads each, then
/// a snapshot with no completion and 300 s of CPU and 50 M reads. Over the window the query did 800 s of CPU
/// and 150 M reads across 1,000 executions: 800 ms and 150,000 reads each. A second hash whose only rows carry
/// no completions stays below the fact's execution bar.</para>
/// </summary>
public sealed class BadActorFactDrillDownAgreementTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 84030;
    private const string ServerName = "SynthBadActorAgreeSrv";
    private const string Database = "SynthBadActorAgreeDb";
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

    /* One UtcNow read, truncated to the second, so a round-tripped timestamp compares equal. */
    private static readonly DateTime WindowEnd = TruncateToSeconds(DateTime.UtcNow);

    private static DateTime TruncateToSeconds(DateTime t) =>
        DateTime.SpecifyKind(new DateTime(t.Ticks - (t.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static readonly DateTime WindowStart = WindowEnd.AddHours(-4);

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public BadActorFactDrillDownAgreementTests(SharedDuckDbFixture fixture)
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
    public async Task TheFact_CountsTheWorkOfSnapshotsWithoutACompletion()
    {
        await ClearAsync();
        await SeedWindowAsync();

        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(Context());
        var fact = Assert.Single(facts, f => f.Key == FactKey);

        Assert.Equal(Database, fact.DatabaseName);
        Assert.Equal(Executions, (long)fact.Metadata["execution_count"]);
        Assert.Equal(ExpectedAvgCpuMs, fact.Value, 2);
        Assert.Equal(ExpectedAvgCpuMs, fact.Metadata["avg_cpu_ms"], 2);
        Assert.Equal(ExpectedAvgReads, fact.Metadata["avg_reads"], 0);
        Assert.Equal(ExpectedTotalCpuUs, (long)fact.Metadata["total_cpu_us"]);
        Assert.Equal(ExpectedTotalReads, (long)fact.Metadata["total_reads"]);
    }

    [Fact]
    public async Task TheFactAndItsDrillDown_StateTheSameFigures()
    {
        await ClearAsync();
        await SeedWindowAsync();

        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(Context());
        var fact = Assert.Single(facts, f => f.Key == FactKey);
        var drillDown = await CollectDrillDownAsync();

        Assert.Equal(fact.DatabaseName, drillDown.GetProperty("database").GetString());
        Assert.Equal((long)fact.Metadata["execution_count"], drillDown.GetProperty("execution_count").GetInt64());
        Assert.Equal(fact.Metadata["avg_cpu_ms"], drillDown.GetProperty("avg_cpu_ms").GetDouble(), 2);
        Assert.Equal(fact.Metadata["avg_reads"], drillDown.GetProperty("avg_reads").GetDouble(), 0);
        Assert.Equal(fact.Metadata["total_cpu_us"] / 1000.0, drillDown.GetProperty("total_cpu_ms").GetDouble(), 3);
        Assert.Equal((long)fact.Metadata["total_reads"], drillDown.GetProperty("total_reads").GetInt64());

        /* And the drill-down's own figures are the window's whole work. */
        Assert.Equal(ExpectedAvgCpuMs, drillDown.GetProperty("avg_cpu_ms").GetDouble(), 2);
        Assert.Equal(ExpectedAvgReads, drillDown.GetProperty("avg_reads").GetDouble(), 0);
    }

    [Fact]
    public async Task AHashWithNoCompletionInTheWindow_IsNotAFact()
    {
        await ClearAsync();
        await SeedWindowAsync();

        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(Context());

        /* The execution bar still gates on completions: work with no execution behind it is not a per-execution
           average, however much CPU it spent. */
        Assert.DoesNotContain(facts, f => f.Key == "BAD_ACTOR_" + NoCompletionHash);
    }

    private async Task<JsonElement> CollectDrillDownAsync()
    {
        var finding = new AnalysisFinding
        {
            RootFactKey = FactKey,
            StoryPath = FactKey,
            PathKeys = [FactKey],
            /* Past the display gate, below which the expensive drill-downs are skipped wholesale. */
            Severity = 1.0,
            /* What InferenceEngine copies from the root fact: the database the fact was scored on. */
            DatabaseName = Database,
        };

        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], Context());

        Assert.NotNull(finding.DrillDown);
        Assert.True(finding.DrillDown.TryGetValue("bad_actor_query", out var raw), "bad_actor_query was not collected");
        return JsonSerializer.SerializeToElement(raw);
    }

    /// <summary>The completed snapshot, the completion-less snapshot, and the hash that never completed.</summary>
    private async Task SeedWindowAsync()
    {
        await SeedAsync(Hash, WindowEnd.AddMinutes(-30), Executions, CompletedCpuUs, CompletedReads);
        await SeedAsync(Hash, WindowEnd.AddMinutes(-20), 0L, InFlightCpuUs, InFlightReads);
        /* 600 s of CPU, no executions. */
        await SeedAsync(NoCompletionHash, WindowEnd.AddMinutes(-20), 0L, 600_000_000L, 1_000_000L);
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

    private async Task SeedAsync(string hash, DateTime collectionTime, long executions, long cpuUs, long reads)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     creation_time, max_dop, min_dop, delta_execution_count, delta_worker_time, delta_elapsed_time,
     delta_logical_reads, delta_spills, query_text, plan_handle)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 1, 1, $9, $10, $11, $12, 0, $13, $14)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = Database });
        cmd.Parameters.Add(new DuckDBParameter { Value = hash });
        cmd.Parameters.Add(new DuckDBParameter { Value = Plan });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime.AddDays(-1) });
        cmd.Parameters.Add(new DuckDBParameter { Value = executions });
        cmd.Parameters.Add(new DuckDBParameter { Value = cpuUs });
        cmd.Parameters.Add(new DuckDBParameter { Value = cpuUs * 2 });
        cmd.Parameters.Add(new DuckDBParameter { Value = reads });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SELECT * FROM dbo.SynthBadActorAgreeTable" });
        cmd.Parameters.Add(new DuckDBParameter { Value = Plan + "H" });
        await cmd.ExecuteNonQueryAsync();
    }
}
