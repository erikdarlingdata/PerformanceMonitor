/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
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
/// #5630, Lite's side: PLAN_REGRESSION must not compare two plans that were compiled for different inputs.
/// A query with OPTION (RECOMPILE), or a parameter-sensitive query whose plans were compiled for different
/// parameter values, looks like a plan flip when its "latest" plan serves a bigger input than its "best" one,
/// and forcing the "best" plan makes the big case slower.
///
/// <para>Lite stores no Query Store plan XML, so the detector takes the two plans from a plan source it is
/// given. These tests seed Query Store rows in DuckDB and feed small ShowPlan fixtures through a fixture
/// source; no SQL Server is involved.</para>
/// </summary>
public sealed class PlanRegressionInputsTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 56300;
    private const string ServerName = "RegrInputsSrv";
    private const string Database = "RegrInputsDb";

    private const string PlainText = "SELECT m.manifest_id FROM dbo.manifest AS m WHERE m.location_id = @location_id;";
    private const string RecompileText = "SELECT i.item_id FROM dbo.item AS i WHERE i.package_id = @package_id OPTION (RECOMPILE);";

    private static readonly DateTime PeriodEnd =
        DateTime.SpecifyKind(new DateTime(DateTime.UtcNow.Ticks - (DateTime.UtcNow.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);
    private static readonly DateTime PeriodStart = PeriodEnd.AddHours(-4);

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public PlanRegressionInputsTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    private static AnalysisContext NewContext() => new()
    {
        ServerId = ServerId,
        ServerName = ServerName,
        TimeRangeStart = PeriodStart,
        TimeRangeEnd = PeriodEnd,
    };

    /// <summary>A plan source over fixtures: plan_id to plan XML, recording every fetch.</summary>
    private sealed class FixturePlanSource : IQueryStorePlanSource
    {
        private readonly Dictionary<long, string> _plans = [];

        public ConcurrentQueue<long> Fetched { get; } = new();

        /// <summary>When set, every fetch calls it first (to throw, or to hang).</summary>
        public Func<long, CancellationToken, Task>? Behavior { get; set; }

        public void Add(long planId, string? compiledValue) => _plans[planId] = Plan(compiledValue);

        public async Task<string?> FetchQueryStorePlanXmlAsync(
            int serverId, string databaseName, long planId, CancellationToken cancellationToken)
        {
            Fetched.Enqueue(planId);
            if (Behavior is not null) await Behavior(planId, cancellationToken);
            return _plans.TryGetValue(planId, out var xml) ? xml : null;
        }
    }

    /// <summary>A small ShowPlan carrying the compiled value of one parameter (none when null).</summary>
    private static string Plan(string? compiledValue) =>
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.564\" Build=\"16.0.1000.6\">" +
        "<BatchSequence><Batch><Statements><StmtSimple StatementText=\"SELECT 1\" StatementType=\"SELECT\">" +
        "<QueryPlan CachedPlanSize=\"16\">" +
        (compiledValue is null
            ? string.Empty
            : $"<ParameterList><ColumnReference Column=\"@location_id\" ParameterDataType=\"int\" ParameterCompiledValue=\"({compiledValue})\" /></ParameterList>") +
        "</QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>";

    private static long CheapPlanId(long queryId) => (queryId * 10) + 1;
    private static long CostlyPlanId(long queryId) => (queryId * 10) + 2;

    /// <summary>The six queries of the issue's reproduction, then the controls.</summary>
    private const long RecompileQuery = 1;      // lookup_items: OPTION (RECOMPILE), a plan per call
    private const long ManifestsB = 2;          // two hashes, compiled for different @location_id
    private const long ManifestsA = 3;          // the same, covering index
    private const long ManifestsC = 4;          // one hash: no regression to report at all
    private const long RealRegression = 5;      // two hashes, SAME compiled values, latest 3x the best

    private async Task SeedIssueCasesAsync(FixturePlanSource source)
    {
        /* Highest factor first, so the excluded rows head the detector's ranking: if the checks were skipped
           the worst offender would be one of them. */
        await SeedRegressionAsync(RecompileQuery, RecompileText, factor: 40);
        source.Add(CheapPlanId(RecompileQuery), "1");
        source.Add(CostlyPlanId(RecompileQuery), "1");

        await SeedRegressionAsync(ManifestsB, PlainText, factor: 30);
        source.Add(CheapPlanId(ManifestsB), "7");
        source.Add(CostlyPlanId(ManifestsB), "1");

        await SeedRegressionAsync(ManifestsA, PlainText, factor: 20);
        source.Add(CheapPlanId(ManifestsA), "7");
        source.Add(CostlyPlanId(ManifestsA), "1");

        await SeedPlanAsync(ManifestsC, CheapPlanId(ManifestsC), "0xONE" + ManifestsC, PlainText,
            cpuUs: 100_000, firstIntervalId: 1, lastExec: PeriodEnd);

        await SeedRegressionAsync(RealRegression, PlainText, factor: 3);
        source.Add(CheapPlanId(RealRegression), "7");
        source.Add(CostlyPlanId(RealRegression), "7");
    }

    [Fact]
    public async Task TheIssueCases_AreDropped_AndARealRegressionStillReports_AllReadersAgree()
    {
        var source = new FixturePlanSource();
        await SeedIssueCasesAsync(source);

        var pass = NewContext();
        var fact = (await new DuckDbFactCollector(_duckDb, queryStorePlanSource: source).CollectFactsAsync(pass))
            .Single(f => f.Key == "PLAN_REGRESSION");

        Assert.Equal(1, fact.Metadata["offender_count"]);
        Assert.Equal(RealRegression, (long)fact.Metadata["worst_query_id"]);
        Assert.Equal(3, fact.Metadata["worst_regression_factor"], precision: 6);
        Assert.Equal(3, fact.Metadata["cross_input_excluded_count"]);
        Assert.Equal(0, fact.Metadata["inputs_unverified_count"]);

        /* Reader two: the offenders the drill-down follows. */
        Assert.Equal([new PlanRegressionOffender(Database, RealRegression)], pass.PlanRegressionOffenders);

        /* Reader three: the regressed-queries drill-down, which also feeds the force-plan targets. */
        var rows = await DrillDownRowsAsync(pass);
        Assert.Equal([RealRegression], rows.Select(r => r.GetProperty("query_id").GetInt64()));

        /* The OPTION (RECOMPILE) statement is dropped on its text alone: neither of its plans was fetched. */
        Assert.DoesNotContain(CheapPlanId(RecompileQuery), source.Fetched);
        Assert.DoesNotContain(CostlyPlanId(RecompileQuery), source.Fetched);
        Assert.Equal(
            [CostlyPlanId(ManifestsB), CheapPlanId(ManifestsB), CostlyPlanId(ManifestsA), CheapPlanId(ManifestsA),
             CostlyPlanId(RealRegression), CheapPlanId(RealRegression)],
            source.Fetched.ToArray());
    }

    [Fact]
    public async Task EveryCandidateDropped_RaisesNoFact_AndNoOffenders()
    {
        var source = new FixturePlanSource();
        await SeedRegressionAsync(ManifestsB, PlainText, factor: 30);
        source.Add(CheapPlanId(ManifestsB), "7");
        source.Add(CostlyPlanId(ManifestsB), "1");
        await SeedRegressionAsync(RecompileQuery, RecompileText, factor: 40);

        var pass = NewContext();
        var facts = await new DuckDbFactCollector(_duckDb, queryStorePlanSource: source).CollectFactsAsync(pass);

        Assert.DoesNotContain(facts, f => f.Key == "PLAN_REGRESSION");
        Assert.Empty(pass.PlanRegressionOffenders!);
    }

    [Fact]
    public async Task APlanSourceThatThrows_KeepsTheCandidate_AndCountsItUnverified()
    {
        var source = new FixturePlanSource
        {
            Behavior = (_, _) => throw new InvalidOperationException("the server is unreachable")
        };
        await SeedRegressionAsync(ManifestsB, PlainText, factor: 30);

        var pass = NewContext();
        var fact = (await new DuckDbFactCollector(_duckDb, queryStorePlanSource: source).CollectFactsAsync(pass))
            .Single(f => f.Key == "PLAN_REGRESSION");

        Assert.Equal(1, fact.Metadata["offender_count"]);
        Assert.Equal(0, fact.Metadata["cross_input_excluded_count"]);
        Assert.Equal(1, fact.Metadata["inputs_unverified_count"]);
        Assert.Equal([new PlanRegressionOffender(Database, ManifestsB)], pass.PlanRegressionOffenders);
    }

    [Fact]
    public async Task WithNoPlanSource_EveryCandidateStays_UnverifiedExceptTheRecompileText()
    {
        await SeedRegressionAsync(ManifestsB, PlainText, factor: 30);
        await SeedRegressionAsync(RecompileQuery, RecompileText, factor: 40);

        var fact = (await new DuckDbFactCollector(_duckDb).CollectFactsAsync(NewContext()))
            .Single(f => f.Key == "PLAN_REGRESSION");

        Assert.Equal(1, fact.Metadata["offender_count"]);
        Assert.Equal(ManifestsB, (long)fact.Metadata["worst_query_id"]);
        Assert.Equal(1, fact.Metadata["cross_input_excluded_count"]);
        Assert.Equal(1, fact.Metadata["inputs_unverified_count"]);
    }

    [Fact]
    public async Task APlanTheSourceDoesNotHave_KeepsTheCandidate_AndCountsItUnverified()
    {
        var source = new FixturePlanSource();
        await SeedRegressionAsync(ManifestsB, PlainText, factor: 30);

        var fact = (await new DuckDbFactCollector(_duckDb, queryStorePlanSource: source).CollectFactsAsync(NewContext()))
            .Single(f => f.Key == "PLAN_REGRESSION");

        Assert.Equal(1, fact.Metadata["offender_count"]);
        Assert.Equal(1, fact.Metadata["inputs_unverified_count"]);
    }

    [Fact]
    public async Task APlanSourceThatHangs_IsAbandonedAtTheFetchTimeout_AndTheCandidateStays()
    {
        var source = new FixturePlanSource
        {
            /* Ignores its token on purpose: the collector must still walk away. */
            Behavior = (_, _) => Task.Delay(TimeSpan.FromMinutes(5))
        };
        await SeedRegressionAsync(ManifestsB, PlainText, factor: 30);

        var started = DateTime.UtcNow;
        var fact = (await new DuckDbFactCollector(_duckDb, queryStorePlanSource: source).CollectFactsAsync(NewContext()))
            .Single(f => f.Key == "PLAN_REGRESSION");
        var elapsed = DateTime.UtcNow - started;

        Assert.Equal(1, fact.Metadata["inputs_unverified_count"]);
        Assert.True(elapsed < DuckDbFactCollector.PlanInputFetchTimeout + TimeSpan.FromSeconds(10), $"took {elapsed}");
        /* The first plan hung, so the second was never asked for. */
        Assert.Single(source.Fetched);
    }

    [Fact]
    public async Task AtMostTwentyFetchesPerPass_TheRestStayUnverified()
    {
        var source = new FixturePlanSource();
        const int queries = 14;
        for (long q = 1; q <= queries; q++)
        {
            await SeedRegressionAsync(q, PlainText, factor: 3 + q);
            source.Add(CheapPlanId(q), "7");
            source.Add(CostlyPlanId(q), "7");
        }

        var fact = (await new DuckDbFactCollector(_duckDb, queryStorePlanSource: source).CollectFactsAsync(NewContext()))
            .Single(f => f.Key == "PLAN_REGRESSION");

        Assert.Equal(DuckDbFactCollector.MaxPlanInputFetches, source.Fetched.Count);
        Assert.Equal(queries, fact.Metadata["offender_count"]);
        Assert.Equal(queries - (DuckDbFactCollector.MaxPlanInputFetches / 2), fact.Metadata["inputs_unverified_count"]);
    }

    [Fact]
    public async Task TheFactKeepsTwentyOffenders_AfterDroppingSomeOfTheHighestRanked()
    {
        var source = new FixturePlanSource();
        const int dropped = 5;
        const int kept = 25;
        for (long q = 1; q <= dropped + kept; q++)
        {
            var isDropped = q > kept;
            await SeedRegressionAsync(q, isDropped ? RecompileText : PlainText, factor: 2 + q);
        }

        var pass = NewContext();
        var fact = (await new DuckDbFactCollector(_duckDb, queryStorePlanSource: source).CollectFactsAsync(pass))
            .Single(f => f.Key == "PLAN_REGRESSION");

        Assert.Equal(20, fact.Metadata["offender_count"]);
        Assert.Equal(dropped, fact.Metadata["cross_input_excluded_count"]);
        Assert.Equal(20, pass.PlanRegressionOffenders!.Count);
        /* The five dropped are the five highest factors: the worst reported one is the best of the rest. */
        Assert.Equal(kept, (long)fact.Metadata["worst_query_id"]);
    }

    private async Task<List<JsonElement>> DrillDownRowsAsync(AnalysisContext context)
    {
        var finding = new AnalysisFinding
        {
            RootFactKey = "PLAN_REGRESSION",
            StoryPath = "PLAN_REGRESSION",
            PathKeys = ["PLAN_REGRESSION"],
            Severity = 1.0,
        };

        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], context);

        if (finding.DrillDown is null || !finding.DrillDown.TryGetValue("regressed_queries", out var raw))
            return [];

        return [.. JsonSerializer.SerializeToElement(raw).EnumerateArray()];
    }

    /// <summary>A cheap plan that ran five days back and a costlier one still running at the window's end.</summary>
    private async Task SeedRegressionAsync(long queryId, string text, double factor)
    {
        await SeedPlanAsync(queryId, CheapPlanId(queryId), "0xCHEAP" + queryId, text, cpuUs: 100_000,
            firstIntervalId: 1, lastExec: PeriodStart.AddDays(-5));
        await SeedPlanAsync(queryId, CostlyPlanId(queryId), "0xCOSTLY" + queryId, text, cpuUs: 100_000 * factor,
            firstIntervalId: 11, lastExec: PeriodEnd);
    }

    private async Task SeedPlanAsync(
        long queryId, long planId, string planHash, string text, double cpuUs, long firstIntervalId, DateTime lastExec)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        for (var interval = 0; interval < 2; interval++)
        {
            var firstExec = lastExec.AddHours(-(2 - interval));
            for (var collection = 1; collection <= 2; collection++)
            {
                using var cmd = _seedConn.CreateCommand();
                cmd.CommandText = @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
     query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
     query_plan_hash, is_forced_plan, force_failure_count,
     runtime_stats_interval_id, interval_start_time_utc, replica_role, query_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'Regular', $8, $9, $10, $11, $12, $13, $14, false, 0, $15, $16, NULL, $17)";
                cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
                cmd.Parameters.Add(new DuckDBParameter { Value = PeriodEnd.AddMinutes(-10 + collection) });
                cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
                cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
                cmd.Parameters.Add(new DuckDBParameter { Value = Database });
                cmd.Parameters.Add(new DuckDBParameter { Value = queryId });
                cmd.Parameters.Add(new DuckDBParameter { Value = planId });
                cmd.Parameters.Add(new DuckDBParameter { Value = firstExec });
                cmd.Parameters.Add(new DuckDBParameter { Value = lastExec.AddHours(-(1 - interval)) });
                cmd.Parameters.Add(new DuckDBParameter { Value = "0xQH" + queryId });
                cmd.Parameters.Add(new DuckDBParameter { Value = 50L * collection });
                cmd.Parameters.Add(new DuckDBParameter { Value = (long)cpuUs });
                cmd.Parameters.Add(new DuckDBParameter { Value = (long)cpuUs + 20_000 });
                cmd.Parameters.Add(new DuckDBParameter { Value = planHash });
                cmd.Parameters.Add(new DuckDBParameter { Value = firstIntervalId + interval });
                cmd.Parameters.Add(new DuckDBParameter { Value = firstExec });
                cmd.Parameters.Add(new DuckDBParameter { Value = text });
                await cmd.ExecuteNonQueryAsync();
            }
        }
    }
}
