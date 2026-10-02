/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
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
/// #3902, Lite's twin of Darling's <c>PlanRegressionDrillDownReuseLiveTests</c>: the regressed-queries
/// drill-down follows the PLAN_REGRESSION fact instead of re-running the whole detection behind it.
///
/// <para>Both reads deduplicated the server's entire 14-day Query Store slice, and the drill-down's five
/// rows are the head of the ranking the fact had just computed. The fact now stamps the queries it reported
/// on the pass's <see cref="AnalysisContext"/>, and the drill-down computes its rows for those queries only
/// — through DuckDB list parameters here, which nothing else in Lite's analysis binds, so the round trip is
/// part of what is under test.</para>
///
/// <para>Driven through the two collectors in the order the analysis service runs them. The seed carries
/// more regressions than either read keeps (thirty, against the fact's twenty and the drill-down's five),
/// one of them on two replicas, and a crowd of one-plan queries.</para>
/// </summary>
public sealed class PlanRegressionDrillDownReuseTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 39020;
    private const string ServerName = "RegrReuseSrv";

    /// <summary>Queries with a regression, more than the fact keeps.</summary>
    private const int RegressedQueries = 30;

    /// <summary>One-plan queries: rows in the slice that can never regress.</summary>
    private const int SteadyQueries = 60;

    /// <summary>The fact's own cap (<c>LIMIT 20</c>).</summary>
    private const int FactCap = 20;

    /// <summary>The query that regressed on both replicas of an AG (two fact rows, one offender).</summary>
    private const long TwoReplicaQueryId = RegressedQueries;

    private const int IntervalsPerPlan = 2;
    private const int CollectionsPerInterval = 2;

    private static readonly DateTime PeriodEnd =
        DateTime.SpecifyKind(new DateTime(DateTime.UtcNow.Ticks - (DateTime.UtcNow.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);
    private static readonly DateTime PeriodStart = PeriodEnd.AddHours(-4);

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public PlanRegressionDrillDownReuseTests(SharedDuckDbFixture fixture)
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

    [Fact]
    public async Task TheDrillDown_ReadsTheFactsOffendersOnly_AndReturnsWhatTheWholeSliceReturns()
    {
        await SeedAsync();

        /* ── The pass: facts, then the drill-down, on ONE context — AnalysisService's order. ── */
        var pass = NewContext();
        var fact = (await new DuckDbFactCollector(_duckDb).CollectFactsAsync(pass))
            .Single(f => f.Key == "PLAN_REGRESSION");

        Assert.Equal(FactCap, fact.Metadata["offender_count"]);

        /* The stamp names the fact's twenty rows: nineteen queries on one replica and the two-replica query
           once — two rows, one offender. */
        var expectedOffenders = Enumerable.Range(RegressedQueries - FactCap + 2, FactCap - 1)
            .Select(q => new PlanRegressionOffender(DatabaseFor(q), q))
            .ToHashSet();
        Assert.NotNull(pass.PlanRegressionOffenders);
        Assert.Equal(expectedOffenders, pass.PlanRegressionOffenders!.ToHashSet());
        Assert.Equal(expectedOffenders.Count, pass.PlanRegressionOffenders!.Count);

        var followed = await DrillDownRowsAsync(pass);

        /* ── The same drill-down with no fact to follow: the whole slice, as it always read. ── */
        var standalone = await DrillDownRowsAsync(NewContext());

        Assert.Equal(5, standalone.Count);
        Assert.Equal(standalone.Select(r => r.GetRawText()), followed.Select(r => r.GetRawText()));

        /* The head of the ranking is the two-replica query, once per replica. */
        Assert.Equal(
            ["PRIMARY", "SECONDARY"],
            followed.Take(2).Select(r => r.GetProperty("replica_role").GetString()).OrderBy(r => r, StringComparer.Ordinal));
        Assert.All(followed.Take(2), r => Assert.Equal(TwoReplicaQueryId, r.GetProperty("query_id").GetInt64()));

        /* ── The stamp is what the drill-down reads: name two queries from the MIDDLE of the ranking and it
              returns those two and nothing else — a collector that dropped the stamp returns the top five. ── */
        var pinned = NewContext();
        pinned.PlanRegressionOffenders = [new(DatabaseFor(27), 27), new(DatabaseFor(28), 28)];
        Assert.Equal([28L, 27L], (await DrillDownRowsAsync(pinned)).Select(r => r.GetProperty("query_id").GetInt64()));

        /* An empty stamp — a fact that found nothing, so no finding to follow — reads the whole slice. */
        var emptied = NewContext();
        emptied.PlanRegressionOffenders = [];
        Assert.Equal(standalone.Select(r => r.GetRawText()), (await DrillDownRowsAsync(emptied)).Select(r => r.GetRawText()));
    }

    private async Task<List<JsonElement>> DrillDownRowsAsync(AnalysisContext context)
    {
        var finding = new AnalysisFinding
        {
            RootFactKey = "PLAN_REGRESSION",
            StoryPath = "PLAN_REGRESSION",
            /* #3859: the collector matches on PathKeys, not on a split of the rendered path. */
            PathKeys = ["PLAN_REGRESSION"],
            /* Past the 0.5 display gate — below it the expensive drill-downs are skipped wholesale. */
            Severity = 1.0,
        };

        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], context);

        if (finding.DrillDown is null || !finding.DrillDown.TryGetValue("regressed_queries", out var raw))
            return [];

        return [.. JsonSerializer.SerializeToElement(raw).EnumerateArray()];
    }

    private static string DatabaseFor(long queryId) => "RegrReuseDb" + (queryId % 3);

    /* Regressed query q (1..30): a cheap plan that ran six-to-five days back and a costlier plan still
       running at the end of the window, two intervals each, every interval collected TWICE. The costly plan
       is (2 + q/2)x the cheap one, so the ranking is exactly q descending and every cut is unambiguous;
       query 30's secondary is a shade milder (16.9x against 17x) so its two rows cannot tie. Steady query s:
       one plan, the same shape, never a regression. */
    private async Task SeedAsync()
    {
        for (long q = 1; q <= RegressedQueries; q++)
        {
            foreach (var role in q == TwoReplicaQueryId ? new[] { "PRIMARY", "SECONDARY" } : new string?[] { null })
            {
                await SeedPlanAsync(q, planId: (q * 10) + 1, "0xCHEAP" + q, cpuUs: 100_000,
                    firstIntervalId: 1, lastExec: PeriodStart.AddDays(-5), role);
                var factor = 2 + (q / 2.0) - (role == "SECONDARY" ? 0.1 : 0);
                await SeedPlanAsync(q, planId: (q * 10) + 2, "0xCOSTLY" + q, cpuUs: 100_000 * factor,
                    firstIntervalId: 11, lastExec: PeriodEnd, role);
            }
        }

        for (long s = 1; s <= SteadyQueries; s++)
        {
            await SeedPlanAsync(1_000 + s, planId: 50_000 + s, "0xSTEADY" + s, cpuUs: 200_000,
                firstIntervalId: 21, lastExec: PeriodEnd.AddHours(-1), role: null);
        }
    }

    private async Task SeedPlanAsync(
        long queryId, long planId, string planHash, double cpuUs, long firstIntervalId, DateTime lastExec, string? role)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        for (var interval = 0; interval < IntervalsPerPlan; interval++)
        {
            var firstExec = lastExec.AddHours(-(IntervalsPerPlan - interval));
            for (var collection = 1; collection <= CollectionsPerInterval; collection++)
            {
                using var cmd = _seedConn.CreateCommand();
                cmd.CommandText = @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
     query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
     query_plan_hash, is_forced_plan, force_failure_count,
     runtime_stats_interval_id, interval_start_time_utc, replica_role)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'Regular', $8, $9, $10, $11, $12, $13, $14, false, 0, $15, $16, $17)";
                cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
                /* Collected after the window's last execution, the second collection a minute after the first
                   and carrying the interval's final (cumulative) count. */
                cmd.Parameters.Add(new DuckDBParameter { Value = PeriodEnd.AddMinutes(-10 + collection) });
                cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
                cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
                cmd.Parameters.Add(new DuckDBParameter { Value = DatabaseFor(queryId) });
                cmd.Parameters.Add(new DuckDBParameter { Value = queryId });
                cmd.Parameters.Add(new DuckDBParameter { Value = planId });
                cmd.Parameters.Add(new DuckDBParameter { Value = firstExec });
                cmd.Parameters.Add(new DuckDBParameter { Value = lastExec.AddHours(-(IntervalsPerPlan - 1 - interval)) });
                cmd.Parameters.Add(new DuckDBParameter { Value = "0xQH" + queryId });
                cmd.Parameters.Add(new DuckDBParameter { Value = 50L * collection });
                cmd.Parameters.Add(new DuckDBParameter { Value = (long)cpuUs });
                cmd.Parameters.Add(new DuckDBParameter { Value = (long)cpuUs + 20_000 });
                cmd.Parameters.Add(new DuckDBParameter { Value = planHash });
                cmd.Parameters.Add(new DuckDBParameter { Value = firstIntervalId + interval });
                cmd.Parameters.Add(new DuckDBParameter { Value = firstExec });
                cmd.Parameters.Add(new DuckDBParameter { Value = (object?)role ?? DBNull.Value });
                await cmd.ExecuteNonQueryAsync();
            }
        }
    }
}
