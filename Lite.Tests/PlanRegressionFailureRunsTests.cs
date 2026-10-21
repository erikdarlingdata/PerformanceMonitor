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
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #6267 (Erik, 2026-10-21): the PLAN_REGRESSION fact counted Regular runs only, so a query whose runs turned into timeouts
/// (Aborted) or errors (Exception) while its Regular cost stayed flat was invisible. Lite's twin of Darling's
/// <c>PlanRegressionFailureRunsLiveTests</c>, through the real collector on a DuckDB store: a query whose timeouts or errors
/// rise past the shared gates (<see cref="PlanRegressionFailureRule.MinRecentRuns"/> runs, <see cref="PlanRegressionFailureRule.RateMultiple"/>
/// times the baseline's per-hour rate or a baseline of none) raises the fact with the figures; a falling rate and a count below
/// the minimum do not; no cost figure appears because no plan's cost regressed.
/// </summary>
public sealed class PlanRegressionFailureRunsTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 62670;
    private const string ServerName = "FailRunsSrv";
    private const string Database = "FailRunsDb";

    private static readonly DateTime PeriodEnd =
        DateTime.SpecifyKind(new DateTime(DateTime.UtcNow.Ticks - (DateTime.UtcNow.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public PlanRegressionFailureRunsTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    [Fact]
    public async Task ARisingTimeoutOrErrorRate_RaisesTheFact_WhenCostIsFlat()
    {
        await SeedScenarioAsync(costFactor: null);

        var pass = new AnalysisContext
        {
            ServerId = ServerId,
            ServerName = ServerName,
            TimeRangeStart = PeriodEnd.AddHours(-1),
            TimeRangeEnd = PeriodEnd,
        };
        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(pass);
        var fact = Assert.Single(facts, f => f.Key == "PLAN_REGRESSION");

        Assert.Equal(PlanRegressionFailureRule.ValueHigh, fact.Value);
        Assert.Equal(2, fact.Metadata["failure_offender_count"]);
        Assert.Equal(1, fact.Metadata["timeout_offender_count"]);
        Assert.Equal(10, fact.Metadata["worst_timeout_query_id"]);
        Assert.Equal(4, fact.Metadata["worst_timeout_recent_runs"]);
        Assert.Equal(0, fact.Metadata["worst_timeout_baseline_runs"]);
        Assert.Equal(1, fact.Metadata["error_offender_count"]);
        Assert.Equal(13, fact.Metadata["worst_error_query_id"]);
        Assert.Equal(5, fact.Metadata["worst_error_recent_runs"]);

        /* No plan's cost regressed, so the fact carries no cost figure and names no cost offender. */
        Assert.Equal(0, fact.Metadata["offender_count"]);
        Assert.False(fact.Metadata.ContainsKey("worst_regression_factor"));
        Assert.Empty(pass.PlanRegressionOffenders ?? []);
    }

    /// <summary>A fact raised by timeouts alone carries no cost figures. The advice must say the timeouts, with the figures,
    /// and must not claim a cost regression or tell the reader to force the cheaper plan (#6267).</summary>
    [Fact]
    public void AFailureOnlyFact_IsDescribedAsTimeouts_NotAsACostRegression()
    {
        var facts = new System.Collections.Generic.Dictionary<string, Fact>
        {
            ["PLAN_REGRESSION"] = new Fact
            {
                Source = "queries",
                Key = "PLAN_REGRESSION",
                Value = PlanRegressionFailureRule.ValueHigh,
                Metadata = new System.Collections.Generic.Dictionary<string, double>
                {
                    ["offender_count"] = 0,
                    ["failure_offender_count"] = 1,
                    ["timeout_offender_count"] = 1,
                    ["failure_recent_hours"] = 24,
                    ["worst_timeout_query_id"] = 10,
                    ["worst_timeout_recent_runs"] = 4,
                    ["worst_timeout_baseline_runs"] = 0,
                },
            },
        };

        var block = FactAdvice.Compose("PLAN_REGRESSION", facts);

        Assert.NotNull(block);
        Assert.Contains("timeouts", block!.Headline, StringComparison.Ordinal);
        Assert.DoesNotContain("regressed to", block.Headline, StringComparison.Ordinal);
        Assert.Contains("query 10", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("4 timed-out runs", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("force the historically", block.Remediation, StringComparison.Ordinal);
    }

    /// <summary>The scenario of the tests above: queries 10 and 13 with a timeout or error rise, 11, 12 and 16 near misses, every
    /// query on one plan at one cost. With <paramref name="costFactor"/>, query 20 also runs a cheap plan in the baseline and a
    /// plan that costs that many times more now.</summary>
    private async Task SeedScenarioAsync(double? costFactor)
    {
        var b1 = PeriodEnd.AddDays(-5);
        var b2 = PeriodEnd.AddDays(-3);
        var r2 = PeriodEnd.AddHours(-5);
        var r3 = PeriodEnd.AddHours(-3);

        /* Every query runs the SAME plan at the SAME cost before and during the recent window: no plan's cost regressed. */
        foreach (var q in new long[] { 10, 11, 12, 13, 16 })
        {
            await SeedAsync("Regular", b1, q, 100);
            await SeedAsync("Regular", r2, q, 100);
        }

        /* 10: timeouts 0 -> 4 recent: flagged, HIGH (the baseline had none). */
        await SeedAsync("Aborted", r2, 10, 2);
        await SeedAsync("Aborted", r3, 10, 2);
        /* 11: 80 baseline timeouts (about 0.26 an hour) and 3 recent (0.125 an hour): the rate fell, not flagged. */
        await SeedAsync("Aborted", b1, 11, 40);
        await SeedAsync("Aborted", b2, 11, 40);
        await SeedAsync("Aborted", r2, 11, 3);
        /* 12: two recent timeouts, below the minimum: not flagged. */
        await SeedAsync("Aborted", r2, 12, 2);
        /* 13: errors 0 -> 5 recent: flagged. */
        await SeedAsync("Exception", r2, 13, 2);
        await SeedAsync("Exception", r3, 13, 3);
        /* 16: 168 baseline errors (about 0.54 an hour) and 5 recent (0.21 an hour): not flagged. */
        await SeedAsync("Exception", b1, 16, 84);
        await SeedAsync("Exception", b2, 16, 84);
        await SeedAsync("Exception", r2, 16, 5);

        if (costFactor is { } factor)
        {
            await SeedAsync("Regular", b1, 20, 100, planId: 201, avgCpuUs: 100_000);
            await SeedAsync("Regular", r2, 20, 100, planId: 202, avgCpuUs: (long)(100_000 * factor));
        }
    }

    private static AnalysisContext NewPass() => new()
    {
        ServerId = ServerId,
        ServerName = ServerName,
        TimeRangeStart = PeriodEnd.AddHours(-1),
        TimeRangeEnd = PeriodEnd,
    };

    private async Task<AnalysisFinding> DrillDownAsync(AnalysisContext pass)
    {
        var finding = new AnalysisFinding
        {
            RootFactKey = "PLAN_REGRESSION",
            StoryPath = "PLAN_REGRESSION",
            PathKeys = ["PLAN_REGRESSION"],
            Severity = 1.0,
        };
        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], pass);
        return finding;
    }

    [Theory]
    [InlineData(3.0, 6.0)]
    [InlineData(12.0, 12.0)]
    public async Task ACostRegressionAndAFailureRise_KeepBothSetsOfMetadata_AndTheLargerValue(double costFactor, double expectedValue)
    {
        await SeedScenarioAsync(costFactor);

        var pass = NewPass();
        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(pass);
        var fact = Assert.Single(facts, f => f.Key == "PLAN_REGRESSION");

        /* The fact keeps the larger of the cost factor and the failures' value (6), and carries both sets of figures. */
        Assert.Equal(expectedValue, fact.Value);
        Assert.Equal(1, fact.Metadata["offender_count"]);
        Assert.Equal(costFactor, fact.Metadata["worst_regression_factor"], 3);
        Assert.Equal(20, fact.Metadata["worst_query_id"]);
        Assert.Equal(2, fact.Metadata["failure_offender_count"]);
        Assert.Equal(10, fact.Metadata["worst_timeout_query_id"]);
        Assert.Equal(13, fact.Metadata["worst_error_query_id"]);

        /* The drill-down lists the cost offender and the failing queries side by side. */
        var finding = await DrillDownAsync(pass);
        Assert.NotNull(finding.DrillDown);
        var cost = Assert.Single(JsonSerializer.SerializeToElement(finding.DrillDown!["regressed_queries"]).EnumerateArray());
        Assert.Equal(20, cost.GetProperty("query_id").GetInt64());
        Assert.Equal([10L, 13L], FailureRows(finding).Select(r => r.GetProperty("query_id").GetInt64()).Order());
    }

    /// <summary>A fact raised by failures alone: the drill-down names the queries whose timeouts or errors rose, with their
    /// recent and baseline counts, and does not fall back to listing cost rows for "every query" (#6267). A drill-down that
    /// lists cost offenders only leaves both of these queries out.</summary>
    [Fact]
    public async Task TheDrillDown_ListsTheFailureOnlyQueries_WithTheirRecentAndBaselineCounts()
    {
        await SeedScenarioAsync(costFactor: null);

        var pass = NewPass();
        await new DuckDbFactCollector(_duckDb).CollectFactsAsync(pass);
        var finding = await DrillDownAsync(pass);

        Assert.NotNull(finding.DrillDown);
        var rows = FailureRows(finding);
        Assert.Equal([10L, 13L], rows.Select(r => r.GetProperty("query_id").GetInt64()).Order());

        var timeouts = rows.Single(r => r.GetProperty("query_id").GetInt64() == 10);
        Assert.Equal(Database, timeouts.GetProperty("database").GetString());
        Assert.True(timeouts.GetProperty("timeouts_rose").GetBoolean());
        Assert.False(timeouts.GetProperty("errors_rose").GetBoolean());
        Assert.Equal(4, timeouts.GetProperty("recent_timeouts").GetInt64());
        Assert.Equal(0, timeouts.GetProperty("baseline_timeouts").GetInt64());

        var errors = rows.Single(r => r.GetProperty("query_id").GetInt64() == 13);
        Assert.True(errors.GetProperty("errors_rose").GetBoolean());
        Assert.Equal(5, errors.GetProperty("recent_errors").GetInt64());
        Assert.Equal(0, errors.GetProperty("baseline_errors").GetInt64());

        Assert.False(finding.DrillDown!.ContainsKey("regressed_queries"));
    }

    private static List<JsonElement> FailureRows(AnalysisFinding finding) =>
        [.. JsonSerializer.SerializeToElement(finding.DrillDown![PlanRegressionFailureRule.DrillDownKey]).EnumerateArray()];

    /// <summary>One interval of one query and outcome, one snapshot. Its last execution is 30 minutes after its first.</summary>
    private async Task SeedAsync(string type, DateTime firstExec, long queryId, long executions, long? planId = null, long avgCpuUs = 1000)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        var id = _nextId++;
        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
     query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
     query_plan_hash, is_forced_plan, force_failure_count,
     runtime_stats_interval_id, interval_start_time_utc, replica_role, query_text, avg_rowcount)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $15, 2000, $13, false, 0, $14, $9, NULL, 'SELECT 1', NULL)";
        cmd.Parameters.Add(new DuckDBParameter { Value = id });
        cmd.Parameters.Add(new DuckDBParameter { Value = firstExec.AddMinutes(40) });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = Database });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryId });
        cmd.Parameters.Add(new DuckDBParameter { Value = planId ?? queryId * 10 });
        cmd.Parameters.Add(new DuckDBParameter { Value = type });
        cmd.Parameters.Add(new DuckDBParameter { Value = firstExec });
        cmd.Parameters.Add(new DuckDBParameter { Value = firstExec.AddMinutes(30) });
        cmd.Parameters.Add(new DuckDBParameter { Value = "0xQH" + queryId });
        cmd.Parameters.Add(new DuckDBParameter { Value = executions });
        cmd.Parameters.Add(new DuckDBParameter { Value = "0xPH" + (planId ?? queryId) });
        cmd.Parameters.Add(new DuckDBParameter { Value = id });
        cmd.Parameters.Add(new DuckDBParameter { Value = avgCpuUs });
        await cmd.ExecuteNonQueryAsync();
    }
}
