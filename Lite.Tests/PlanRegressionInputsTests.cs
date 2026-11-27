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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #5630/#5631, Lite's side: PLAN_REGRESSION judges each candidate by its output, its compiled values and its
/// statistics. A statement with OPTION (RECOMPILE), a newer plan that only returns far more rows, and a plan that
/// differs from the best one in compiled values alone look like a plan flip but are not; a plan compiled for the same
/// values, or for different values after a statistics update, is kept.
///
/// <para>Lite stores no Query Store plan XML, so the detector takes the two plans from a plan source it is
/// given. These tests seed Query Store rows in DuckDB and feed small ShowPlan fixtures through a fixture
/// source; no SQL Server is involved.</para>
///
/// <para>Collection <c>app-logger-statics</c>: the slow-check line test drains the process-wide log buffer, which the other
/// tests that do the same share.</para>
/// </summary>
[Collection("app-logger-statics")]
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

        /// <summary>
        /// Passes every plan through the statement filter before it is returned, as <c>SqlPlanFetcher</c> does for the
        /// live read (#4348). <c>StatementScrubLivePlanDisplayTests</c> pins that call in the fetcher itself.
        /// </summary>
        public bool AppliesStatementFilter { get; set; }

        /// <summary>One parameter compiled for <paramref name="compiledValue"/> (none when null), over the default statistics.</summary>
        public void Add(long planId, string? compiledValue, string statementText = "SELECT 1") =>
            _plans[planId] = Plan(
                compiledValue is null ? [] : [("@location_id", compiledValue)], DefaultStatistics, statementText);

        public void AddXml(long planId, string planXml) => _plans[planId] = planXml;

        public void Remove(long planId) => _plans.Remove(planId);

        public async Task<string?> FetchQueryStorePlanXmlAsync(
            int serverId, string databaseName, long planId, CancellationToken cancellationToken)
        {
            Fetched.Enqueue(planId);
            if (Behavior is not null) await Behavior(planId, cancellationToken);
            if (!_plans.TryGetValue(planId, out var xml)) return null;
            return AppliesStatementFilter ? LivePlanDisplay.Filter(xml) : xml;
        }
    }

    /// <summary>The statistics every plan carries unless a fixture says otherwise: one, updated on one fixed date.</summary>
    private static readonly (string Statistics, string LastUpdate)[] DefaultStatistics = [("[ix_location]", "2026-01-01T00:00:00.00")];

    /// <summary>
    /// A small ShowPlan: the compiled values of its parameters (a null value is a parameter with no compiled value),
    /// the statistics it used (null: no <c>OptimizerStatsUsage</c> at all), and the estimator version when given.
    /// </summary>
    private static string Plan(
        IReadOnlyList<(string Name, string? Value)> parameters,
        IReadOnlyList<(string Statistics, string LastUpdate)>? statistics,
        string statementText = "SELECT 1", string? estimatorVersion = null)
    {
        var xml = new System.Text.StringBuilder();
        xml.Append("<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.564\" Build=\"16.0.1000.6\">");
        xml.Append("<BatchSequence><Batch><Statements><StmtSimple StatementText=\"").Append(statementText).Append("\" StatementType=\"SELECT\"");
        if (estimatorVersion is not null) xml.Append(" CardinalityEstimationModelVersion=\"").Append(estimatorVersion).Append('"');
        xml.Append("><QueryPlan CachedPlanSize=\"16\">");
        if (statistics is not null)
        {
            xml.Append("<OptimizerStatsUsage>");
            foreach (var (name, lastUpdate) in statistics)
                xml.Append("<StatisticsInfo Database=\"[RegrInputsDb]\" Schema=\"[dbo]\" Table=\"[manifest]\" Statistics=\"")
                   .Append(name).Append("\" ModificationCount=\"5\" SamplingPercent=\"100\" LastUpdate=\"").Append(lastUpdate).Append("\" />");
            xml.Append("</OptimizerStatsUsage>");
        }

        if (parameters.Count > 0)
        {
            xml.Append("<ParameterList>");
            foreach (var (name, value) in parameters)
            {
                xml.Append("<ColumnReference Column=\"").Append(name).Append("\" ParameterDataType=\"int\"");
                if (value is not null) xml.Append(" ParameterCompiledValue=\"(").Append(value).Append(")\"");
                xml.Append(" />");
            }

            xml.Append("</ParameterList>");
        }

        xml.Append("</QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>");
        return xml.ToString();
    }

    private static long CheapPlanId(long queryId) => (queryId * 10) + 1;
    private static long CostlyPlanId(long queryId) => (queryId * 10) + 2;

    /// <summary>
    /// The five invented fixtures. Two are false alarms the old rule kept (a statement that recompiles, and a plan that
    /// only returns far more rows), one is a false alarm the compiled values alone expose (different values over the same
    /// statistics), and two are real flips (the same values; different values after a statistics update).
    /// </summary>
    private const long RecompileQuery = 1;      // OPTION (RECOMPILE): a plan per call
    private const long OutputGrewQuery = 2;     // the newer plan returns 26,000 rows per call, the older one 50
    private const long ValuesOnlyQuery = 3;     // one compiled value differs, the statistics are the same
    private const long StatisticsFlipQuery = 4; // three compiled values differ, 14 statistics were all updated, no rows either side
    private const long SameValuesQuery = 5;     // the same three compiled values, 996 rows either side
    private const long SingleHashQuery = 6;     // one hash: no regression to report at all

    /// <summary>The names the plan-source, replica and cut tests below use: a query to drop, and a query that stays.</summary>
    private const long ManifestsB = ValuesOnlyQuery;
    private const long RealRegression = SameValuesQuery;

    private static readonly (string Name, string? Value)[] ThreeValuesA = [("@a", "1"), ("@b", "2"), ("@c", "3")];
    private static readonly (string Name, string? Value)[] ThreeValuesB = [("@a", "1"), ("@b", "9"), ("@c", "3")];

    private static (string Statistics, string LastUpdate)[] FourteenStatistics(string lastUpdate) =>
        [.. Enumerable.Range(1, 14).Select(n => ($"[st_{n}]", lastUpdate))];

    private static long?[] Rows(long? rows) => [rows, rows];

    /// <summary>Highest factor first, so the dropped rows head the detector's ranking: were the checks skipped, the worst offender would be one of them.</summary>
    private async Task SeedIssueCasesAsync(FixturePlanSource source)
    {
        await SeedOutputGrewAsync(source, factor: 187);
        await SeedRegressionAsync(RecompileQuery, RecompileText, factor: 40);
        await SeedValuesOnlyAsync(source, factor: 30);
        await SeedStatisticsFlipAsync(source, factor: 6);
        await SeedSameValuesAsync(source, factor: 3);

        await SeedPlanAsync(SingleHashQuery, CheapPlanId(SingleHashQuery), "0xONE" + SingleHashQuery, PlainText,
            cpuUs: 100_000, firstIntervalId: 1, lastExec: PeriodEnd);
    }

    private async Task SeedOutputGrewAsync(FixturePlanSource source, double factor)
    {
        await SeedRegressionAsync(OutputGrewQuery, PlainText, factor, cheapRows: Rows(50), costlyRows: Rows(26_000));
        source.Add(CheapPlanId(OutputGrewQuery), "7");
        source.Add(CostlyPlanId(OutputGrewQuery), "1");
    }

    private async Task SeedValuesOnlyAsync(FixturePlanSource source, double factor)
    {
        await SeedRegressionAsync(ValuesOnlyQuery, PlainText, factor, cheapRows: Rows(10), costlyRows: Rows(10));
        source.Add(CheapPlanId(ValuesOnlyQuery), "7");
        source.Add(CostlyPlanId(ValuesOnlyQuery), "1");
    }

    private async Task SeedStatisticsFlipAsync(FixturePlanSource source, double factor)
    {
        await SeedRegressionAsync(StatisticsFlipQuery, PlainText, factor, cheapRows: Rows(0), costlyRows: Rows(0));
        source.AddXml(CheapPlanId(StatisticsFlipQuery), Plan(ThreeValuesA, FourteenStatistics("2026-01-01T00:00:00.00")));
        source.AddXml(CostlyPlanId(StatisticsFlipQuery), Plan(ThreeValuesB, FourteenStatistics("2026-02-01T00:00:00.00")));
    }

    private async Task SeedSameValuesAsync(FixturePlanSource source, double factor)
    {
        await SeedRegressionAsync(SameValuesQuery, PlainText, factor, cheapRows: Rows(996), costlyRows: Rows(996));
        source.AddXml(CheapPlanId(SameValuesQuery), Plan(ThreeValuesA, FourteenStatistics("2026-01-01T00:00:00.00")));
        source.AddXml(CostlyPlanId(SameValuesQuery), Plan(ThreeValuesA, FourteenStatistics("2026-02-01T00:00:00.00")));
    }

    private async Task<(Fact Fact, AnalysisContext Pass)> RunAsync(FixturePlanSource? source)
    {
        var pass = NewContext();
        var fact = (await new DuckDbFactCollector(_duckDb, queryStorePlanSource: source).CollectFactsAsync(pass))
            .Single(f => f.Key == "PLAN_REGRESSION");
        return (fact, pass);
    }

    [Fact]
    public async Task TheFalseAlarms_AreDropped_TheTwoRealFlips_Stay_AllReadersAgree()
    {
        var source = new FixturePlanSource();
        await SeedIssueCasesAsync(source);

        var (fact, pass) = await RunAsync(source);

        /* The statistics flip is the worst of the two that stay (factor 6 against 3), and its basis is 2. */
        Assert.Equal(2, fact.Metadata["offender_count"]);
        Assert.Equal(StatisticsFlipQuery, (long)fact.Metadata["worst_query_id"]);
        Assert.Equal(6, fact.Metadata["lead_regression_factor"], precision: 6);
        Assert.Equal(3, fact.Metadata["cross_input_excluded_count"]);
        Assert.Equal(0, fact.Metadata["inputs_unverified_count"]);
        Assert.Equal(2, fact.Metadata["worst_input_basis"]);

        /* Reader two: the offenders the drill-down follows. */
        Assert.Equal(
            [new PlanRegressionOffender(Database, StatisticsFlipQuery), new PlanRegressionOffender(Database, SameValuesQuery)],
            pass.PlanRegressionOffenders);

        /* Reader three: the regressed-queries drill-down, which also feeds the force-plan targets. */
        var rows = await DrillDownRowsAsync(pass);
        Assert.Equal([StatisticsFlipQuery, SameValuesQuery], rows.Select(r => r.GetProperty("query_id").GetInt64()));

        /* The recompile statement and the plan that only returns more are dropped from the read alone: no plan of either
           was fetched. The other three were compared, the newer plan first. */
        Assert.Equal(
            [CostlyPlanId(ValuesOnlyQuery), CheapPlanId(ValuesOnlyQuery),
             CostlyPlanId(StatisticsFlipQuery), CheapPlanId(StatisticsFlipQuery),
             CostlyPlanId(SameValuesQuery), CheapPlanId(SameValuesQuery)],
            source.Fetched.ToArray());
    }

    [Fact]
    public async Task ARecompileStatement_IsDroppedWithoutAFetch()
    {
        var source = new FixturePlanSource();
        await SeedRegressionAsync(RecompileQuery, RecompileText, factor: 40);
        await SeedSameValuesAsync(source, factor: 3);

        var (fact, _) = await RunAsync(source);

        Assert.Equal(1, fact.Metadata["cross_input_excluded_count"]);
        Assert.DoesNotContain(CheapPlanId(RecompileQuery), source.Fetched);
        Assert.DoesNotContain(CostlyPlanId(RecompileQuery), source.Fetched);
    }

    [Fact]
    public async Task APlanThatOnlyReturnsMoreRows_IsDroppedWithZeroFetches()
    {
        /* Factor 187 with 26,000 rows against 50: the cost per row went down. The plans are not even in the source. */
        var source = new FixturePlanSource();
        await SeedRegressionAsync(OutputGrewQuery, PlainText, factor: 187, cheapRows: Rows(50), costlyRows: Rows(26_000));
        await SeedSameValuesAsync(source, factor: 3);

        var (fact, pass) = await RunAsync(source);

        Assert.Equal(1, fact.Metadata["offender_count"]);
        Assert.Equal(SameValuesQuery, (long)fact.Metadata["worst_query_id"]);
        Assert.Equal(1, fact.Metadata["cross_input_excluded_count"]);
        Assert.DoesNotContain(CheapPlanId(OutputGrewQuery), source.Fetched);
        Assert.DoesNotContain(CostlyPlanId(OutputGrewQuery), source.Fetched);
        Assert.DoesNotContain(pass.PlanRegressionOffenders!, o => o.QueryId == OutputGrewQuery);
    }

    [Fact]
    public async Task DifferentValuesOverTheSameStatistics_AreDropped()
    {
        var source = new FixturePlanSource();
        await SeedValuesOnlyAsync(source, factor: 30);
        await SeedSameValuesAsync(source, factor: 3);

        var (fact, _) = await RunAsync(source);

        Assert.Equal(1, fact.Metadata["offender_count"]);
        Assert.Equal(SameValuesQuery, (long)fact.Metadata["worst_query_id"]);
        Assert.Equal(1, fact.Metadata["cross_input_excluded_count"]);
    }

    [Fact]
    public async Task ADifferentValueAfterAStatisticsUpdate_Stays_WithBasisTwo()
    {
        var source = new FixturePlanSource();
        await SeedStatisticsFlipAsync(source, factor: 6);

        var (fact, _) = await RunAsync(source);

        Assert.Equal(1, fact.Metadata["offender_count"]);
        Assert.Equal(StatisticsFlipQuery, (long)fact.Metadata["worst_query_id"]);
        Assert.Equal(0, fact.Metadata["cross_input_excluded_count"]);
        Assert.Equal(0, fact.Metadata["inputs_unverified_count"]);
        Assert.Equal(2, fact.Metadata["worst_input_basis"]);
    }

    [Fact]
    public async Task TheSameCompiledValues_Stay_WithBasisOne()
    {
        var source = new FixturePlanSource();
        await SeedSameValuesAsync(source, factor: 3);

        var (fact, _) = await RunAsync(source);

        Assert.Equal(1, fact.Metadata["offender_count"]);
        Assert.Equal(0, fact.Metadata["cross_input_excluded_count"]);
        Assert.Equal(0, fact.Metadata["inputs_unverified_count"]);
        Assert.Equal(1, fact.Metadata["worst_input_basis"]);
    }

    [Fact]
    public async Task APlanTheSourceCannotRead_Stays_Unverified_WithBasisZero()
    {
        var source = new FixturePlanSource();
        await SeedRegressionAsync(ValuesOnlyQuery, PlainText, factor: 30, cheapRows: Rows(10), costlyRows: Rows(10));
        source.AddXml(CheapPlanId(ValuesOnlyQuery), "<ShowPlanXML><not closed");
        source.AddXml(CostlyPlanId(ValuesOnlyQuery), Plan([("@a", "1")], DefaultStatistics));

        var (fact, _) = await RunAsync(source);

        Assert.Equal(1, fact.Metadata["offender_count"]);
        Assert.Equal(0, fact.Metadata["cross_input_excluded_count"]);
        Assert.Equal(1, fact.Metadata["inputs_unverified_count"]);
        Assert.Equal(0, fact.Metadata["worst_input_basis"]);
    }

    [Fact]
    public async Task DifferentValuesWithNoStatisticsInEitherPlan_Stay_Unverified()
    {
        var source = new FixturePlanSource();
        await SeedRegressionAsync(ValuesOnlyQuery, PlainText, factor: 30, cheapRows: Rows(10), costlyRows: Rows(10));
        source.AddXml(CheapPlanId(ValuesOnlyQuery), Plan([("@a", "1")], null));
        source.AddXml(CostlyPlanId(ValuesOnlyQuery), Plan([("@a", "2")], null));

        var (fact, _) = await RunAsync(source);

        Assert.Equal(1, fact.Metadata["offender_count"]);
        Assert.Equal(1, fact.Metadata["inputs_unverified_count"]);
        Assert.Equal(0, fact.Metadata["worst_input_basis"]);
    }

    [Theory]
    [InlineData(6_000L, 18_000L, true)]
    [InlineData(6_000L, 10_000L, false)]
    [InlineData(null, 18_000L, false)]
    public async Task RowsPerCall_AreTheExecutionWeightedAverageOfTheIntervals_AndNullWhenAnyIntervalHasNone(
        long? firstInterval, long? secondInterval, bool dropped)
    {
        /* The older plan returns 1,000 rows per call. The newer plan's two intervals run the same number of executions,
           so its rows per call are their mean: 12,000 (12x, dropped) or 8,000 (8x, plans needed). An interval with no
           avg_rowcount makes the whole figure NULL, so the output rule cannot fire and the plans are fetched. */
        var source = new FixturePlanSource();
        await SeedRegressionAsync(ValuesOnlyQuery, PlainText, factor: 3, cheapRows: Rows(1_000), costlyRows: [firstInterval, secondInterval]);
        source.Add(CheapPlanId(ValuesOnlyQuery), "7");
        source.Add(CostlyPlanId(ValuesOnlyQuery), "7");

        var facts = await new DuckDbFactCollector(_duckDb, queryStorePlanSource: source).CollectFactsAsync(NewContext());

        Assert.Equal(dropped, facts.All(f => f.Key != "PLAN_REGRESSION"));
        Assert.Equal(dropped ? 0 : 2, source.Fetched.Count);
    }

    [Theory]
    [InlineData(4_000L, true)]
    [InlineData(null, false)]
    public async Task APlanGroupOfTwoPlanIds_AveragesTheirRows_AndIsNullWhenEitherHasNone(long? secondPlanRows, bool dropped)
    {
        /* Two plan_ids share one query_plan_hash, so they are one plan group: 20,000 and 4,000 rows over equal executions
           is 12,000 against the older plan's 1,000. */
        var source = new FixturePlanSource();
        await SeedPlanAsync(ValuesOnlyQuery, 11, "0xCHEAP", PlainText, cpuUs: 100_000, firstIntervalId: 1,
            lastExec: PeriodStart.AddDays(-5), rowsByInterval: Rows(1_000));
        await SeedPlanAsync(ValuesOnlyQuery, 91, "0xGROUP", PlainText, cpuUs: 300_000, firstIntervalId: 11,
            lastExec: PeriodEnd, rowsByInterval: Rows(20_000));
        await SeedPlanAsync(ValuesOnlyQuery, 92, "0xGROUP", PlainText, cpuUs: 300_000, firstIntervalId: 11,
            lastExec: PeriodEnd.AddMinutes(-1), rowsByInterval: Rows(secondPlanRows));
        source.Add(11, "7");
        source.Add(91, "7");

        var facts = await new DuckDbFactCollector(_duckDb, queryStorePlanSource: source).CollectFactsAsync(NewContext());

        Assert.Equal(dropped, facts.All(f => f.Key != "PLAN_REGRESSION"));
        Assert.Equal(dropped ? 0 : 2, source.Fetched.Count);
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
        Assert.Equal(0, fact.Metadata["worst_input_basis"]);
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

        var pass = new DuckDbFactCollector(_duckDb, queryStorePlanSource: source).CollectFactsAsync(NewContext());

        /* The source would hold the pass for five minutes. A pass still running after a minute did not walk away
           at the fetch timeout (five seconds); the test stops waiting, it does not time anything. */
        var finished = await Task.WhenAny(pass, Task.Delay(TimeSpan.FromSeconds(60)));
        Assert.Same(pass, finished);

        var fact = (await pass).Single(f => f.Key == "PLAN_REGRESSION");
        Assert.Equal(1, fact.Metadata["inputs_unverified_count"]);
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

    /// <summary>The statement filter withholds this one (a credential in a login DDL), so its plan comes back as a placeholder.</summary>
    private const string WithheldStatement = "CREATE LOGIN [example_login] WITH PASSWORD = N'example'";

    [Fact]
    public async Task APlanTheStatementFilterWithholds_ComparesUnknown_TheCandidateStaysUnverified()
    {
        /* Both plans carry the same compiled value, so unfiltered they would compare Same and count as checked. The
           filter withholds the statement and its values: nothing is left to compare, and the candidate stays. */
        var source = new FixturePlanSource { AppliesStatementFilter = true };
        await SeedRegressionAsync(ManifestsB, PlainText, factor: 30);
        source.Add(CheapPlanId(ManifestsB), "7", WithheldStatement);
        source.Add(CostlyPlanId(ManifestsB), "7", WithheldStatement);

        var pass = NewContext();
        var fact = (await new DuckDbFactCollector(_duckDb, queryStorePlanSource: source).CollectFactsAsync(pass))
            .Single(f => f.Key == "PLAN_REGRESSION");

        Assert.Equal(1, fact.Metadata["offender_count"]);
        Assert.Equal(0, fact.Metadata["cross_input_excluded_count"]);
        Assert.Equal(1, fact.Metadata["inputs_unverified_count"]);
        Assert.Equal([new PlanRegressionOffender(Database, ManifestsB)], pass.PlanRegressionOffenders);
    }

    [Fact]
    public async Task AQueryOneReplicaExcludes_IsOutForEveryReplica_InTheFactTheOffendersAndTheDrillDown()
    {
        /* One query, two replica rows: the primary's two plans were compiled for the same value, the secondary's for
           different values. The drill-down names a query by (database, query_id), so the query must leave whole. */
        var source = new FixturePlanSource();
        await SeedRegressionAsync(ManifestsB, PlainText, factor: 30, replicaRole: "PRIMARY", cheapPlanId: 71, costlyPlanId: 72);
        source.Add(71, "7");
        source.Add(72, "7");
        await SeedRegressionAsync(ManifestsB, PlainText, factor: 30, replicaRole: "SECONDARY", cheapPlanId: 73, costlyPlanId: 74);
        source.Add(73, "7");
        source.Add(74, "1");
        await SeedRegressionAsync(RealRegression, PlainText, factor: 3);
        source.Add(CheapPlanId(RealRegression), "7");
        source.Add(CostlyPlanId(RealRegression), "7");

        var pass = NewContext();
        var fact = (await new DuckDbFactCollector(_duckDb, queryStorePlanSource: source).CollectFactsAsync(pass))
            .Single(f => f.Key == "PLAN_REGRESSION");

        Assert.Equal(1, fact.Metadata["offender_count"]);
        Assert.Equal(RealRegression, (long)fact.Metadata["worst_query_id"]);
        Assert.Equal(1, fact.Metadata["cross_input_excluded_count"]);
        Assert.Equal([new PlanRegressionOffender(Database, RealRegression)], pass.PlanRegressionOffenders);
        var rows = await DrillDownRowsAsync(pass);
        Assert.Equal([RealRegression], rows.Select(r => r.GetProperty("query_id").GetInt64()).Distinct());
    }

    [Fact]
    public async Task ARecompileQueryOnTwoReplicas_CountsOnce()
    {
        var source = new FixturePlanSource();
        await SeedRegressionAsync(RecompileQuery, RecompileText, factor: 40, replicaRole: "PRIMARY");
        await SeedRegressionAsync(RecompileQuery, RecompileText, factor: 40, replicaRole: "SECONDARY");
        await SeedRegressionAsync(RealRegression, PlainText, factor: 3);
        source.Add(CheapPlanId(RealRegression), "7");
        source.Add(CostlyPlanId(RealRegression), "7");

        var fact = (await new DuckDbFactCollector(_duckDb, queryStorePlanSource: source).CollectFactsAsync(NewContext()))
            .Single(f => f.Key == "PLAN_REGRESSION");

        Assert.Equal(1, fact.Metadata["cross_input_excluded_count"]);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("   ")]
    [InlineData(SensitiveStatements.PlaceholderText)]
    public async Task PlansThatCompareSame_WithTheStatementTextMissingOrWithheld_AreCountedUnverified(string? text)
    {
        /* The OPTION (RECOMPILE) check reads the text; without it the Same verdict does not make the candidate checked. */
        var source = new FixturePlanSource();
        await SeedRegressionAsync(ManifestsB, text, factor: 30);
        source.Add(CheapPlanId(ManifestsB), "7");
        source.Add(CostlyPlanId(ManifestsB), "7");

        var fact = (await new DuckDbFactCollector(_duckDb, queryStorePlanSource: source).CollectFactsAsync(NewContext()))
            .Single(f => f.Key == "PLAN_REGRESSION");

        Assert.Equal(1, fact.Metadata["offender_count"]);
        Assert.Equal(1, fact.Metadata["inputs_unverified_count"]);
    }

    [Fact]
    public async Task TiedPlanIdsInAGroup_PickTheHigherPlanId_OnEveryPass()
    {
        /* The latest group holds two plan_ids with the same executions and the same last run, compiled for different
           values. The pick breaks the tie on plan_id (the higher one, 82, compiled for 1), so the verdict is the same on
           every pass, and the same as Darling's. */
        var source = new FixturePlanSource();
        await SeedPlanAsync(ManifestsB, 83, "0xCHEAP83", PlainText, cpuUs: 100_000, firstIntervalId: 1, lastExec: PeriodStart.AddDays(-5));
        await SeedPlanAsync(ManifestsB, 81, "0xTIED", PlainText, cpuUs: 3_000_000, firstIntervalId: 11, lastExec: PeriodEnd);
        await SeedPlanAsync(ManifestsB, 82, "0xTIED", PlainText, cpuUs: 3_000_000, firstIntervalId: 11, lastExec: PeriodEnd);
        source.Add(83, "7");
        source.Add(81, "7");
        source.Add(82, "1");

        for (var pass = 0; pass < 2; pass++)
        {
            source.Fetched.Clear();
            var facts = await new DuckDbFactCollector(_duckDb, queryStorePlanSource: source).CollectFactsAsync(NewContext());

            Assert.DoesNotContain(facts, f => f.Key == "PLAN_REGRESSION");
            Assert.Equal([82L, 83L], source.Fetched.ToArray());
        }
    }

    [Fact]
    public async Task TheCutToTwenty_CountsOnlyTheRowsThatStay_AReplicaRowBelowTheCutStillExcludes()
    {
        /* 22 queries that stay, then query 99 on two replicas: its primary row (factor 90) is kept and fills a place, its
           secondary row (factor 4) is below the cut and compares Different. The query must leave and a 21st-ranked query
           must take its place, so the fact reports 20 offenders, none of them query 99. */
        var source = new FixturePlanSource();
        const long Split = 99;
        /* The 22 share one pair of plans: one comparison, so the pass's fetch budget is not what this test measures. */
        source.Add(1001, "7");
        source.Add(1002, "7");
        for (long q = 1; q <= 22; q++)
        {
            await SeedRegressionAsync(q, PlainText, factor: 10 + q, cheapPlanId: 1001, costlyPlanId: 1002);
        }

        await SeedRegressionAsync(Split, PlainText, factor: 90, replicaRole: "PRIMARY", cheapPlanId: 991, costlyPlanId: 992);
        source.Add(991, "7");
        source.Add(992, "7");
        await SeedRegressionAsync(Split, PlainText, factor: 4, replicaRole: "SECONDARY", cheapPlanId: 993, costlyPlanId: 994);
        source.Add(993, "7");
        source.Add(994, "1");

        var pass = NewContext();
        var fact = (await new DuckDbFactCollector(_duckDb, queryStorePlanSource: source).CollectFactsAsync(pass))
            .Single(f => f.Key == "PLAN_REGRESSION");

        Assert.Equal(20, fact.Metadata["offender_count"]);
        Assert.Equal(1, fact.Metadata["cross_input_excluded_count"]);
        Assert.Equal(20, pass.PlanRegressionOffenders!.Count);
        Assert.DoesNotContain(pass.PlanRegressionOffenders, o => o.QueryId == Split);
        Assert.Equal(22, (long)fact.Metadata["worst_query_id"]);
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
    private async Task SeedRegressionAsync(
        long queryId, string? text, double factor, string? replicaRole = null, long? cheapPlanId = null, long? costlyPlanId = null,
        long?[]? cheapRows = null, long?[]? costlyRows = null)
    {
        var cheap = cheapPlanId ?? CheapPlanId(queryId);
        var costly = costlyPlanId ?? CostlyPlanId(queryId);
        await SeedPlanAsync(queryId, cheap, "0xCHEAP" + cheap, text, cpuUs: 100_000,
            firstIntervalId: 1, lastExec: PeriodStart.AddDays(-5), replicaRole, rowsByInterval: cheapRows);
        await SeedPlanAsync(queryId, costly, "0xCOSTLY" + costly, text, cpuUs: 100_000 * factor,
            firstIntervalId: 11, lastExec: PeriodEnd, replicaRole, rowsByInterval: costlyRows);
    }

    private async Task SeedPlanAsync(
        long queryId, long planId, string planHash, string? text, double cpuUs, long firstIntervalId, DateTime lastExec,
        string? replicaRole = null, long?[]? rowsByInterval = null)
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
     runtime_stats_interval_id, interval_start_time_utc, replica_role, query_text, avg_rowcount)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'Regular', $8, $9, $10, $11, $12, $13, $14, false, 0, $15, $16, $18, $17, $19)";
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
                cmd.Parameters.Add(new DuckDBParameter { Value = (object?)text ?? DBNull.Value });
                cmd.Parameters.Add(new DuckDBParameter { Value = (object?)replicaRole ?? DBNull.Value });
                cmd.Parameters.Add(new DuckDBParameter { Value = (object?)rowsByInterval?[interval] ?? DBNull.Value });
                await cmd.ExecuteNonQueryAsync();
            }
        }
    }

    /* #9195: the judged plan pairs are kept across passes, so the same plans are not fetched from the monitored server
       every pass. Each pass below is a new collector over one shared cache, as the scheduler's passes are. */

    private async Task<(Fact? Fact, AnalysisContext Pass)> RunSharedAsync(
        FixturePlanSource source, PlanInputOutcomeCache cache, bool interactive = false)
    {
        var pass = NewContext();
        pass.InteractiveRead = interactive;
        var fact = (await new DuckDbFactCollector(_duckDb, queryStorePlanSource: source, planInputCache: cache).CollectFactsAsync(pass))
            .SingleOrDefault(f => f.Key == "PLAN_REGRESSION");
        return (fact, pass);
    }

    [Fact]
    public async Task ASecondPassOverTheSameCandidates_MakesNoFetch_AndKeepsTheFactsAndTheOffenders()
    {
        var source = new FixturePlanSource();
        await SeedIssueCasesAsync(source);
        var cache = new PlanInputOutcomeCache();

        var (first, firstPass) = await RunSharedAsync(source, cache);
        var firstFetches = source.Fetched.Count;
        Assert.True(firstFetches > 0);

        var (second, secondPass) = await RunSharedAsync(source, cache);

        Assert.Equal(firstFetches, source.Fetched.Count);
        Assert.Equal(first!.Metadata["offender_count"], second!.Metadata["offender_count"]);
        Assert.Equal(first.Metadata["cross_input_excluded_count"], second.Metadata["cross_input_excluded_count"]);
        Assert.Equal(first.Metadata["inputs_unverified_count"], second.Metadata["inputs_unverified_count"]);
        Assert.Equal(first.Metadata["worst_input_basis"], second.Metadata["worst_input_basis"]);
        Assert.Equal(first.Metadata["worst_query_id"], second.Metadata["worst_query_id"]);
        Assert.Equal(firstPass.PlanRegressionOffenders, secondPass.PlanRegressionOffenders);
    }

    /* #9195: an interactive read (get_analysis_facts, compare_analysis) answers from the outcomes a background pass cached, and for the
       pairs it lacks makes at most 3 live plan fetches in 3 s for the whole call. A candidate the allowance does not reach stays in the
       fact, counted unverified. */

    [Fact]
    public async Task AnInteractiveRead_OverAnEmptyCache_MakesAtMostThreeFetches_AndKeepsTheUnreachedCandidatesUnverified()
    {
        var source = new FixturePlanSource();
        await SeedIssueCasesAsync(source);
        var cache = new PlanInputOutcomeCache();

        var (fact, pass) = await RunSharedAsync(source, cache, interactive: true);

        /* The values-only pair (the worst of the three that need plans) takes two fetches and is dropped. The next pair's newer plan
           takes the third, and its older plan and the last pair are past the allowance. */
        Assert.Equal(3, source.Fetched.Count);
        Assert.Equal(3, Convert.ToInt32(fact!.Metadata["cross_input_excluded_count"]));
        Assert.Equal(2, Convert.ToInt32(fact.Metadata["offender_count"]));
        Assert.Equal(2, Convert.ToInt32(fact.Metadata["inputs_unverified_count"]));
        Assert.Equal(2, pass.PlanRegressionOffenders!.Count);
        Assert.Equal(1, cache.Count);
    }

    [Fact]
    public async Task AnInteractiveRead_FillsTheCacheLikeABackgroundPass_SoASecondIdenticalCallFetchesNothing()
    {
        var source = new FixturePlanSource();
        await SeedValuesOnlyAsync(source, factor: 30);
        await SeedPlanAsync(SingleHashQuery, CheapPlanId(SingleHashQuery), "0xONE" + SingleHashQuery, PlainText,
            cpuUs: 100_000, firstIntervalId: 1, lastExec: PeriodEnd);
        var cache = new PlanInputOutcomeCache();

        await RunSharedAsync(source, cache, interactive: true);
        Assert.Equal(2, source.Fetched.Count);
        Assert.Equal(1, cache.Count);

        var (second, _) = await RunSharedAsync(source, cache, interactive: true);

        Assert.Equal(2, source.Fetched.Count);
        Assert.Null(second);
    }

    [Fact]
    public async Task RepeatedInteractiveCalls_CheckTheRestOfTheUncachedPairs_UntilNoneNeedsAFetch()
    {
        var source = new FixturePlanSource();
        await SeedIssueCasesAsync(source);
        var cache = new PlanInputOutcomeCache();

        /* Three pairs need their plans (two fetches each). Each call finishes what the allowance reaches and caches it. */
        await RunSharedAsync(source, cache, interactive: true);
        Assert.Equal(3, source.Fetched.Count);
        await RunSharedAsync(source, cache, interactive: true);
        Assert.Equal(6, source.Fetched.Count);
        await RunSharedAsync(source, cache, interactive: true);
        Assert.Equal(8, source.Fetched.Count);

        var (fourth, _) = await RunSharedAsync(source, cache, interactive: true);

        Assert.Equal(8, source.Fetched.Count);
        Assert.Equal(0, Convert.ToInt32(fourth!.Metadata["inputs_unverified_count"]));
    }

    [Fact]
    public async Task AnInteractiveRead_WithASlowPlanSource_StopsAtTheCallsClock_NoFetchOutlivesIt_AndTheRestAreUnreached()
    {
        var source = new FixturePlanSource();
        await SeedIssueCasesAsync(source);
        source.Behavior = (_, token) => Task.Delay(TimeSpan.FromSeconds(30), token);
        var cache = new PlanInputOutcomeCache();
        var pass = NewContext();
        pass.InteractiveRead = true;
        pass.InteractiveFetches = new InteractivePlanFetchAllowance(3, TimeSpan.FromMilliseconds(400));

        var fact = (await new DuckDbFactCollector(_duckDb, queryStorePlanSource: source, planInputCache: cache).CollectFactsAsync(pass))
            .Single(f => f.Key == "PLAN_REGRESSION");

        /* The first fetch is cut at what the clock allows (400 ms here), not at the 5 s a fetch may otherwise take. Its pair fails, and
           the pairs after it find the clock spent, so they never fetch. All three stay unverified. No assert is made on the read's own
           time, so load cannot fail it. A fetch not cut to the clock would run its full 5 s, and the plan would then be set aside for
           the hour as a fault of its own, which the last assert catches. */
        Assert.Single(source.Fetched);
        Assert.Equal(3, Convert.ToInt32(fact.Metadata["inputs_unverified_count"]));
        /* The clock cut that fetch, not a fault of the plan, so the plan is not set aside for the hour. */
        Assert.False(cache.IsSkipped(new PlanInputOutcomeCache.PlanKey(ServerId, Database, CostlyPlanId(ValuesOnlyQuery))));
    }

    [Fact]
    public async Task AnInteractiveRead_WhoseClockIsAllButSpent_MakesNoFetch_AndTheCandidatesStayUnverified()
    {
        var source = new FixturePlanSource();
        await SeedIssueCasesAsync(source);
        source.Behavior = (_, token) => Task.Delay(TimeSpan.FromSeconds(30), token);
        var cache = new PlanInputOutcomeCache();
        var pass = NewContext();
        pass.InteractiveRead = true;
        /* #9312: 20 ms is less than a fetch needs to run. The first attempt starts the clock, so this holds with no wait. */
        pass.InteractiveFetches = new InteractivePlanFetchAllowance(3, TimeSpan.FromMilliseconds(20));

        var fact = (await new DuckDbFactCollector(_duckDb, queryStorePlanSource: source, planInputCache: cache).CollectFactsAsync(pass))
            .Single(f => f.Key == "PLAN_REGRESSION");

        /* No fetch is granted, so none is made and none is counted as a failed one. The slow-check line that names the cause logs only
           past one second, which this pass never reaches, so the made-no-fetch assert stands for "unreached" here. */
        Assert.Empty(source.Fetched);
        Assert.Equal(3, Convert.ToInt32(fact.Metadata["inputs_unverified_count"]));
        Assert.Equal(3, pass.InteractiveFetches.Left);
        Assert.Equal(0, pass.InteractiveFetches.Fetches);
        Assert.Equal(0, cache.SkipCount);
    }

    [Fact]
    public void AnAllowanceWithLessThanTheMinimumTimeLeft_GrantsNoFetch_AndKeepsItsCount()
    {
        var allowance = new InteractivePlanFetchAllowance(3, InteractivePlanFetchAllowance.MinimumFetchTime / 2);

        Assert.False(allowance.TryTake(out var remaining));
        Assert.True(remaining < InteractivePlanFetchAllowance.MinimumFetchTime);
        Assert.Equal(3, allowance.Left);
        Assert.Equal(0, allowance.Fetches);
    }

    [Fact]
    public async Task ABackgroundPass_StillFetches_AndFillsTheCacheAnInteractiveReadAnswersFrom()
    {
        var source = new FixturePlanSource();
        await SeedIssueCasesAsync(source);
        var cache = new PlanInputOutcomeCache();

        var (background, backgroundPass) = await RunSharedAsync(source, cache);
        var fetches = source.Fetched.Count;
        /* The background pass keeps its own budget: the three pairs need more fetches than an interactive call's three. */
        Assert.True(fetches > InteractivePlanFetchAllowance.DefaultFetches, $"the background pass made {fetches} fetches");
        Assert.True(cache.Count > 0);

        var (interactive, interactivePass) = await RunSharedAsync(source, cache, interactive: true);

        Assert.Equal(fetches, source.Fetched.Count);
        Assert.Equal(background!.Metadata["offender_count"], interactive!.Metadata["offender_count"]);
        Assert.Equal(background.Metadata["cross_input_excluded_count"], interactive.Metadata["cross_input_excluded_count"]);
        Assert.Equal(background.Metadata["inputs_unverified_count"], interactive.Metadata["inputs_unverified_count"]);
        Assert.Equal(background.Metadata["worst_input_basis"], interactive.Metadata["worst_input_basis"]);
        Assert.Equal(background.Metadata["worst_query_id"], interactive.Metadata["worst_query_id"]);
        Assert.Equal(backgroundPass.PlanRegressionOffenders, interactivePass.PlanRegressionOffenders);
    }

    /// <summary>The plan source handed to an <see cref="AnalysisService"/>, which takes it as its plan fetcher.</summary>
    private sealed class ServiceFetcher(FixturePlanSource source) : IPlanFetcher, IQueryStorePlanSource
    {
        public Task<string?> FetchPlanXmlAsync(int serverId, string planHandle, CancellationToken cancellationToken) =>
            Task.FromResult<string?>(null);

        public Task<string?> FetchStatementPlanXmlAsync(
            int serverId, string planHandle, int statementStartOffset, int statementEndOffset, CancellationToken cancellationToken) =>
            Task.FromResult<string?>(null);

        public Task<string?> FetchQueryStorePlanXmlAsync(int serverId, string databaseName, long planId, CancellationToken cancellationToken) =>
            source.FetchQueryStorePlanXmlAsync(serverId, databaseName, planId, cancellationToken);
    }

    [Fact]
    public async Task TheFactsRead_AndTheCompareRead_EachMakeAtMostThreeFetches_AndTheCompareSharesOneAllowanceAcrossItsTwoWindows()
    {
        var source = new FixturePlanSource();
        await SeedIssueCasesAsync(source);
        var hoursBack = 4;

        var facts = new AnalysisService(_duckDb, new ServiceFetcher(source)) { MinimumDataHours = 0 };
        var (read, _, _) = await facts.CollectAndScoreFactsAsync(ServerId, ServerName, hoursBack, PeriodEnd);
        Assert.Equal(3, source.Fetched.Count);
        Assert.Contains(read, f => f.Key == "PLAN_REGRESSION");

        /* Both windows hold the same uncached pairs. A compare that gave each window its own allowance would fetch more than three. */
        var compare = new AnalysisService(_duckDb, new ServiceFetcher(source)) { MinimumDataHours = 0 };
        var before = source.Fetched.Count;
        var interactive = await compare.ComparePeriodsAsync(
            ServerId, ServerName, PeriodEnd.AddHours(-hoursBack), PeriodEnd, PeriodEnd.AddHours(-hoursBack), PeriodEnd, interactive: true);
        Assert.Equal(3, source.Fetched.Count - before);
        Assert.NotEmpty(interactive.ComparisonFacts);
    }

    private sealed class ManualTime : TimeProvider
    {
        private DateTimeOffset _now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        public override DateTimeOffset GetUtcNow() => _now;
        public void Advance(TimeSpan by) => _now += by;
    }

    /// <summary>Makes a fetch fail the way <paramref name="failure"/> says, for the plans <paramref name="failing"/> accepts.</summary>
    private static void Break(FixturePlanSource source, string failure, Func<long, bool> failing) =>
        source.Behavior = (planId, _) =>
        {
            if (!failing(planId)) return Task.CompletedTask;
            switch (failure)
            {
                case "timeout": throw new OperationCanceledException();
                case "fault": throw new InvalidOperationException("the server is unreachable");
                default: source.Remove(planId); return Task.CompletedTask;
            }
        };

    [Theory]
    [InlineData("timeout")]
    [InlineData("fault")]
    [InlineData("none")]
    public async Task ATimeoutAFaultOrNoPlan_IsNotAnOutcome_ButIsNotFetchedAgainForAnHour(string failure)
    {
        var broken = new FixturePlanSource();
        broken.Add(CheapPlanId(ManifestsB), "7");
        broken.Add(CostlyPlanId(ManifestsB), "1");
        Break(broken, failure, _ => true);
        await SeedRegressionAsync(ManifestsB, PlainText, factor: 30, cheapRows: Rows(10), costlyRows: Rows(10));
        var time = new ManualTime();
        var cache = new PlanInputOutcomeCache(time);

        var (first, _) = await RunSharedAsync(broken, cache);
        Assert.Equal(1, first!.Metadata["inputs_unverified_count"]);
        Assert.Equal(0, cache.Count);
        Assert.Single(broken.Fetched);

        /* The server answers on the next pass, but the plan failed a moment ago: no fetch, and the candidate stays unverified. */
        var healthy = new FixturePlanSource();
        healthy.Add(CheapPlanId(ManifestsB), "7");
        healthy.Add(CostlyPlanId(ManifestsB), "1");
        time.Advance(TimeSpan.FromMinutes(30));
        var (second, _) = await RunSharedAsync(healthy, cache);

        Assert.Empty(healthy.Fetched);
        Assert.Equal(1, second!.Metadata["inputs_unverified_count"]);

        /* After the hour it is tried again: both plans are fetched and compared, and the values-only plan leaves. */
        time.Advance(PlanInputOutcomeCache.SkipLifetime);
        var (third, _) = await RunSharedAsync(healthy, cache);

        Assert.Null(third);
        Assert.Equal([CostlyPlanId(ManifestsB), CheapPlanId(ManifestsB)], healthy.Fetched.ToArray());
    }

    [Theory]
    [InlineData("timeout")]
    [InlineData("fault")]
    [InlineData("none")]
    public async Task AFailedPlan_UsesNoBudgetOnTheNextPass_SoTheBudgetReachesTheCandidatesItWouldHaveStarved(string failure)
    {
        /* Twenty candidates, the six worst fail on their second plan (two fetches each). Without a skip entry the next
           pass spends those twelve fetches again and reaches fewer of the rest. */
        const int queries = 20;
        const int failingQueries = 6;
        var source = new FixturePlanSource();
        for (long q = 1; q <= queries; q++)
        {
            await SeedRegressionAsync(q, PlainText, factor: 3 + q);
            source.Add(CheapPlanId(q), "7");
            source.Add(CostlyPlanId(q), "7");
        }

        bool Fails(long planId) => planId / 10 > queries - failingQueries && planId % 10 == 1;
        Break(source, failure, Fails);
        var time = new ManualTime();
        var cache = new PlanInputOutcomeCache(time);

        var (first, _) = await RunSharedAsync(source, cache);
        Assert.Equal(DuckDbFactCollector.MaxPlanInputFetches, source.Fetched.Count);
        var judgedFirst = (DuckDbFactCollector.MaxPlanInputFetches - (failingQueries * 2)) / 2;
        Assert.Equal(queries - judgedFirst, first!.Metadata["inputs_unverified_count"]);

        source.Fetched.Clear();
        time.Advance(TimeSpan.FromMinutes(30));
        var (second, _) = await RunSharedAsync(source, cache);

        /* No failing query's plan is fetched, and the twenty fetches all go to queries not judged before. */
        Assert.DoesNotContain(source.Fetched, p => p / 10 > queries - failingQueries);
        Assert.Equal(DuckDbFactCollector.MaxPlanInputFetches, source.Fetched.Count);
        Assert.Equal(failingQueries, second!.Metadata["inputs_unverified_count"]);
        Assert.Equal(queries, second.Metadata["offender_count"]);

        /* After the hour the failing queries are tried again, and the server answers. */
        source.Fetched.Clear();
        time.Advance(PlanInputOutcomeCache.SkipLifetime);
        source.Behavior = null;
        for (var q = queries - failingQueries + 1; q <= queries; q++) source.Add(CheapPlanId(q), "7");
        var (third, _) = await RunSharedAsync(source, cache);

        Assert.Equal(failingQueries * 2, source.Fetched.Count);
        Assert.Equal(0, third!.Metadata["inputs_unverified_count"]);
    }

    [Fact]
    public async Task AnUncomparablePairOnFullPlans_IsNotFetchedAgainBeforeTheOutcomeLifetime_AndIsAfterIt()
    {
        /* Different values and no statistics in either plan, and nothing withheld in either: the stored plans never change, so
           the pair is uncomparable every time and is answered from the cache, not fetched, for the outcome lifetime. */
        var source = new FixturePlanSource();
        await SeedRegressionAsync(ValuesOnlyQuery, PlainText, factor: 30, cheapRows: Rows(10), costlyRows: Rows(10));
        source.AddXml(CheapPlanId(ValuesOnlyQuery), Plan([("@a", "1")], null));
        source.AddXml(CostlyPlanId(ValuesOnlyQuery), Plan([("@a", "2")], null));
        var time = new ManualTime();
        var cache = new PlanInputOutcomeCache(time);

        var (first, _) = await RunSharedAsync(source, cache);
        Assert.Equal(2, source.Fetched.Count);
        Assert.Equal(1, cache.Count);
        Assert.Equal(1, first!.Metadata["inputs_unverified_count"]);

        time.Advance(TimeSpan.FromMinutes(30));
        var (second, _) = await RunSharedAsync(source, cache);
        Assert.Equal(2, source.Fetched.Count);
        Assert.Equal(1, second!.Metadata["inputs_unverified_count"]);

        /* An hour and a half in: past the skip hour, where an unverified pair used to be fetched again. */
        time.Advance(PlanInputOutcomeCache.SkipLifetime);
        var (third, _) = await RunSharedAsync(source, cache);
        Assert.Equal(2, source.Fetched.Count);
        Assert.Equal(1, third!.Metadata["inputs_unverified_count"]);

        /* A minute short of six hours: still answered from the cache. */
        time.Advance(PlanInputOutcomeCache.Lifetime - TimeSpan.FromMinutes(91));
        var (fourth, _) = await RunSharedAsync(source, cache);
        Assert.Equal(2, source.Fetched.Count);
        Assert.Equal(1, fourth!.Metadata["inputs_unverified_count"]);

        /* Past six hours it is fetched and compared again, and a candidate that stays unverified is still reported as one. */
        time.Advance(TimeSpan.FromMinutes(2));
        var (fifth, _) = await RunSharedAsync(source, cache);
        Assert.Equal(4, source.Fetched.Count);
        Assert.Equal(1, fifth!.Metadata["inputs_unverified_count"]);
    }

    [Fact]
    public async Task APairWhosePlanTheFilterWithheld_IsFetchedAgainAfterTheSkipHour_NotHeldForTheOutcomeLifetime()
    {
        /* The statement filter withholds under a time budget, and a withheld plan reads the same whatever withheld it. The
           first pass reads the whole plan withheld, as a spent budget returns it: that compare says nothing about the
           plans, so it is skipped for an hour and not remembered as an outcome. */
        var withheld = new FixturePlanSource();
        withheld.Add(CheapPlanId(ManifestsB), "7");
        withheld.AddXml(CostlyPlanId(ManifestsB), SensitiveStatements.PlaceholderText);
        await SeedRegressionAsync(ManifestsB, PlainText, factor: 30, cheapRows: Rows(10), costlyRows: Rows(10));
        var time = new ManualTime();
        var cache = new PlanInputOutcomeCache(time);

        var (first, _) = await RunSharedAsync(withheld, cache);
        Assert.Equal(2, withheld.Fetched.Count);
        Assert.Equal(1, first!.Metadata["inputs_unverified_count"]);
        Assert.Equal(0, cache.Count);
        Assert.Equal(1, cache.SkipCount);

        var healthy = new FixturePlanSource();
        healthy.Add(CheapPlanId(ManifestsB), "7");
        healthy.Add(CostlyPlanId(ManifestsB), "7");
        time.Advance(TimeSpan.FromMinutes(30));
        var (second, _) = await RunSharedAsync(healthy, cache);
        Assert.Empty(healthy.Fetched);
        Assert.Equal(1, second!.Metadata["inputs_unverified_count"]);

        /* After the hour both plans are read again, in full this time, and the pair compares. */
        time.Advance(PlanInputOutcomeCache.SkipLifetime);
        var (third, _) = await RunSharedAsync(healthy, cache);
        Assert.Equal([CostlyPlanId(ManifestsB), CheapPlanId(ManifestsB)], healthy.Fetched.ToArray());
        Assert.Equal(0, third!.Metadata["inputs_unverified_count"]);
        Assert.Equal(1, cache.Count);
    }

    /* #9195: the one Information line a slow check logs names why each unverified candidate stays unverified. */
    private static readonly Regex SlowCheckLine = new(
        @"input check took (?<ms>\d+) ms for " + ServerName + @" \(server " + ServerId + @"\): (?<candidates>\d+) candidates, " +
        @"(?<fetches>\d+) fetches \((?<plans>\d+) plans, (?<none>\d+) none, (?<timeouts>\d+) timeouts, (?<faults>\d+) faults, \d+ ms, " +
        @"slowest plan fetch (?<slowest>\d+) ms, largest plan (?<largest>\d+) characters\), " +
        @"(?<hits>\d+) answered from an earlier pass, (?<skippedPairs>\d+) skipped after an earlier failure, " +
        @"(?<unverified>\d+) unverified \((?<unreached>\d+) unreached, (?<skipped>\d+) skipped, (?<noPlan>\d+) without a plan to fetch, " +
        @"(?<failed>\d+) fetch failed, (?<uncomparable>\d+) uncomparable, (?<withheld>\d+) withheld by the filter, " +
        @"(?<threw>\d+) compare threw, (?<text>\d+) text unknown\)",
        RegexOptions.CultureInvariant);

    private static int Num(Match line, string group) => int.Parse(line.Groups[group].Value, System.Globalization.CultureInfo.InvariantCulture);

    [Fact]
    public async Task TheSlowCheckLine_CountsEveryUnverifiedCandidateUnderOneCause_AndNamesTheSlowestFetchAndLargestPlan()
    {
        var source = new FixturePlanSource { AppliesStatementFilter = true };

        /* One query per cause, the highest factor first. Query 1: uncomparable (different values, no statistics, nothing
           withheld), its newer plan the slowest fetch and the largest plan. */
        var bigPlan = Plan([("@a", "2")], null, new string('x', 5_000));
        await SeedRegressionAsync(1, PlainText, factor: 119, cheapRows: Rows(10), costlyRows: Rows(10));
        source.AddXml(CheapPlanId(1), Plan([("@a", "1")], null));
        source.AddXml(CostlyPlanId(1), bigPlan);
        source.Behavior = async (planId, token) =>
        {
            if (planId == CostlyPlanId(1)) await Task.Delay(1_100, token);
        };

        /* 2: the filter withheld a statement and its values, so the pair is uncomparable with withheld text. */
        await SeedRegressionAsync(2, PlainText, factor: 118, cheapRows: Rows(10), costlyRows: Rows(10));
        source.Add(CheapPlanId(2), "7", WithheldStatement);
        source.Add(CostlyPlanId(2), "7", WithheldStatement);

        /* 3: the newer plan has no plan to give. */
        await SeedRegressionAsync(3, PlainText, factor: 117, cheapRows: Rows(10), costlyRows: Rows(10));
        source.Add(CheapPlanId(3), "7");

        /* 4: its newer plan failed a moment ago, so the pair is answered from a skip entry. */
        await SeedRegressionAsync(4, PlainText, factor: 116, cheapRows: Rows(10), costlyRows: Rows(10));
        source.Add(CheapPlanId(4), "7");
        source.Add(CostlyPlanId(4), "7");

        /* 5: the plans compare the same, and the statement text is unknown. */
        await SeedRegressionAsync(5, null, factor: 115, cheapRows: Rows(10), costlyRows: Rows(10));
        source.Add(CheapPlanId(5), "7");
        source.Add(CostlyPlanId(5), "7");

        /* 6 to 13: the same values, so six of them are checked with the fetches left. The twelfth query gets its newer plan
           and the budget is spent, and the thirteenth is not reached. */
        for (long q = 6; q <= 13; q++)
        {
            await SeedRegressionAsync(q, PlainText, factor: 120 - q, cheapRows: Rows(10), costlyRows: Rows(10));
            source.Add(CheapPlanId(q), "7");
            source.Add(CostlyPlanId(q), "7");
        }

        var cache = new PlanInputOutcomeCache();
        cache.Skip(new PlanInputOutcomeCache.PlanKey(ServerId, Database, CostlyPlanId(4)));

        AppLogger.DrainBufferedLines();
        var (fact, _) = await RunSharedAsync(source, cache);
        var lines = AppLogger.DrainBufferedLines();

        var line = SlowCheckLine.Match(string.Join('\n', lines.Where(l => l.Contains("input check took", StringComparison.Ordinal))));
        Assert.True(line.Success, "the slow-check line was not logged");

        Assert.Equal(13, Num(line, "candidates"));
        Assert.Equal(DuckDbFactCollector.MaxPlanInputFetches, Num(line, "fetches"));
        Assert.Equal(19, Num(line, "plans"));
        Assert.Equal(1, Num(line, "none"));

        /* Each cause shows on the pass that exercises it, and the causes add up to the unverified total. */
        Assert.Equal(2, Num(line, "unreached"));
        Assert.Equal(1, Num(line, "skipped"));
        Assert.Equal(0, Num(line, "noPlan"));
        Assert.Equal(1, Num(line, "failed"));
        Assert.Equal(1, Num(line, "uncomparable"));
        Assert.Equal(1, Num(line, "withheld"));
        Assert.Equal(0, Num(line, "threw"));
        Assert.Equal(1, Num(line, "text"));
        Assert.Equal(7, Num(line, "unverified"));
        Assert.Equal(
            Num(line, "unverified"),
            new[] { "unreached", "skipped", "noPlan", "failed", "uncomparable", "withheld", "threw", "text" }.Sum(g => Num(line, g)));
        Assert.Equal(fact!.Metadata["inputs_unverified_count"], Num(line, "unverified"));

        /* The slowest fetch that returned a plan, and the largest plan, for choosing a per-fetch timeout from the logs. */
        Assert.True(Num(line, "slowest") >= 1_000);
        Assert.Equal(bigPlan.Length, Num(line, "largest"));
    }

    [Fact]
    public async Task ThePassesOwnCancellation_LeavesNoSkipEntry()
    {
        using var cancel = new CancellationTokenSource();
        var source = new FixturePlanSource
        {
            Behavior = (_, _) =>
            {
                cancel.Cancel();
                throw new OperationCanceledException(cancel.Token);
            }
        };
        await SeedRegressionAsync(ManifestsB, PlainText, factor: 30, cheapRows: Rows(10), costlyRows: Rows(10));
        var cache = new PlanInputOutcomeCache();
        var pass = NewContext();
        pass.CancellationToken = cancel.Token;

        await Record.ExceptionAsync(() =>
            new DuckDbFactCollector(_duckDb, queryStorePlanSource: source, planInputCache: cache).CollectFactsAsync(pass));

        Assert.Single(source.Fetched);
        Assert.Equal(0, cache.SkipCount);
    }

    [Fact]
    public async Task APassWhoseBudgetRunsOut_LeavesNoSkipEntry_AndTheNextPassReachesTheRest()
    {
        var source = new FixturePlanSource();
        const int queries = 14;
        for (long q = 1; q <= queries; q++)
        {
            await SeedRegressionAsync(q, PlainText, factor: 3 + q);
            source.Add(CheapPlanId(q), "7");
            source.Add(CostlyPlanId(q), "7");
        }

        var cache = new PlanInputOutcomeCache();
        await RunSharedAsync(source, cache);
        Assert.Equal(0, cache.SkipCount);

        source.Fetched.Clear();
        var (second, _) = await RunSharedAsync(source, cache);

        Assert.Equal(0, second!.Metadata["inputs_unverified_count"]);
        Assert.Equal(8, source.Fetched.Count);
    }

    [Fact]
    public async Task ANewPlanPairForTheSameQuery_IsFetched()
    {
        var source = new FixturePlanSource();
        await SeedPlanAsync(ManifestsB, 83, "0xCHEAP83", PlainText, cpuUs: 100_000, firstIntervalId: 1, lastExec: PeriodStart.AddDays(-5));
        await SeedPlanAsync(ManifestsB, 82, "0xTIED", PlainText, cpuUs: 3_000_000, firstIntervalId: 11, lastExec: PeriodEnd);
        source.Add(83, "7");
        source.Add(82, "7");
        source.Add(84, "1");
        var cache = new PlanInputOutcomeCache();

        var (first, _) = await RunSharedAsync(source, cache);
        Assert.Equal([82L, 83L], source.Fetched.ToArray());
        Assert.Equal(0, first!.Metadata["inputs_unverified_count"]);

        /* The query compiles a newer plan (the higher plan_id wins the tie, as in the tie test above) compiled for another value. */
        await SeedPlanAsync(ManifestsB, 84, "0xTIED", PlainText, cpuUs: 3_000_000, firstIntervalId: 11, lastExec: PeriodEnd);
        source.Fetched.Clear();

        var (second, _) = await RunSharedAsync(source, cache);

        Assert.Equal([84L, 83L], source.Fetched.ToArray());
        Assert.Null(second);
    }

    [Fact]
    public async Task CachedPairsUseNoFetchBudget_SoTheCandidatesPastTheBudgetAreJudgedOnTheNextPass()
    {
        var source = new FixturePlanSource();
        const int queries = 14;
        for (long q = 1; q <= queries; q++)
        {
            await SeedRegressionAsync(q, PlainText, factor: 3 + q);
            source.Add(CheapPlanId(q), "7");
            source.Add(CostlyPlanId(q), "7");
        }

        var cache = new PlanInputOutcomeCache();
        var (first, _) = await RunSharedAsync(source, cache);
        Assert.Equal(DuckDbFactCollector.MaxPlanInputFetches, source.Fetched.Count);
        Assert.Equal(queries - (DuckDbFactCollector.MaxPlanInputFetches / 2), first!.Metadata["inputs_unverified_count"]);

        source.Fetched.Clear();
        var (second, _) = await RunSharedAsync(source, cache);

        /* The ten judged queries cost nothing this time, so the budget reaches the other four (two plans each). */
        Assert.Equal((queries - (DuckDbFactCollector.MaxPlanInputFetches / 2)) * 2, source.Fetched.Count);
        Assert.Equal(0, second!.Metadata["inputs_unverified_count"]);
        Assert.Equal(queries, second.Metadata["offender_count"]);
    }
}
