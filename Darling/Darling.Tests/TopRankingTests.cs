/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #5226 pins that need no database: the order choice is a whitelist (a request value is parsed to an enum, never
/// spliced), each choice swaps the one rank-metric anchor in every one of the five top-N statements, the hourly rollup answers
/// every choice but reads, the web catalog and dispatch carry <c>order_by</c> for the two reads, and the MCP tools
/// keep their parameters (no <c>order_by</c>) and so keep ranking by CPU.
/// </summary>
public sealed class TopRankingTests
{
    [Theory]
    [InlineData(null, TopRanking.Cpu)]
    [InlineData("", TopRanking.Cpu)]
    [InlineData("  ", TopRanking.Cpu)]
    [InlineData("cpu", TopRanking.Cpu)]
    [InlineData("CPU", TopRanking.Cpu)]
    [InlineData("duration", TopRanking.Duration)]
    [InlineData(" Duration ", TopRanking.Duration)]
    [InlineData("reads", TopRanking.Reads)]
    [InlineData("executions", TopRanking.Executions)]
    public void TryParse_AcceptsTheFourSpellings_AndAbsentMeansCpu(string? value, TopRanking expected)
    {
        Assert.True(TopRankings.TryParse(value, out var ranking));
        Assert.Equal(expected, ranking);
    }

    [Theory]
    [InlineData("elapsed")]
    [InlineData("cpu; DROP TABLE query_stats")]
    [InlineData("delta_worker_time")]
    [InlineData("1")]
    [InlineData("duration,reads")]
    public void TryParse_RefusesAnythingElse_NeverCarriesItAlong(string value)
    {
        Assert.False(TopRankings.TryParse(value, out var ranking));
        Assert.Equal(TopRanking.Cpu, ranking);
    }

    [Fact]
    public void WireName_RoundTripsEveryChoice_AndTheChoiceListMatchesTheEnum()
    {
        foreach (var ranking in Enum.GetValues<TopRanking>())
        {
            Assert.True(TopRankings.TryParse(TopRankings.WireName(ranking), out var back));
            Assert.Equal(ranking, back);
        }

        Assert.Equal(
            new[] { "cpu", "duration", "reads", "executions" },
            TopRankings.Choices.Select(c => c.Value).ToArray());
    }

    /// <summary>
    /// The CPU choice is the default read. Its statement is the const with the anchor replaced by the worker-time sum
    /// and nothing else changed (the const is never run unexpanded, so even the default goes through Apply).
    /// </summary>
    [Fact]
    public void Apply_Cpu_ExpandsEveryConstToTheWorkerTimeSum_AndNothingElseChanges()
    {
        foreach (var raw in new[] { DarlingDataReader.TopQueriesSql, DarlingDataReader.TopQueriesByHostObjectSql, DarlingDataReader.TopProceduresSql })
        {
            Assert.Equal(raw.Replace("$RANK$", "SUM(delta_worker_time)", StringComparison.Ordinal), TopRankings.Apply(raw, TopRanking.Cpu, hourly: false));
        }

        foreach (var hourly in new[] { DarlingDataReader.TopQueriesHourlySql, DarlingDataReader.TopProceduresHourlySql })
        {
            Assert.Equal(hourly.Replace("$RANK$", "SUM(worker_time_sum)", StringComparison.Ordinal), TopRankings.Apply(hourly, TopRanking.Cpu, hourly: true));
        }
    }

    /// <summary>
    /// Each choice swaps ONE thing in each raw statement, the ranking pass's metric. The ORDER BYs, the caps and the
    /// filters are the const's own text, so they are the same for every choice.
    /// </summary>
    [Theory]
    [InlineData(TopRanking.Duration, "SUM(delta_elapsed_time)")]
    [InlineData(TopRanking.Reads, "SUM(delta_logical_reads)")]
    [InlineData(TopRanking.Executions, "SUM(delta_execution_count)")]
    public void Apply_Raw_SwapsTheRankMetricInEveryRawStatement(TopRanking ranking, string metric)
    {
        foreach (var cpuSql in new[] { DarlingDataReader.TopQueriesSql, DarlingDataReader.TopQueriesByHostObjectSql, DarlingDataReader.TopProceduresSql })
        {
            var sql = TopRankings.Apply(cpuSql, ranking, hourly: false);
            Assert.Contains(metric + " AS rank_metric", sql, StringComparison.Ordinal);
            Assert.Contains("SUM(delta_worker_time) AS rank_cpu", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("$RANK$", sql, StringComparison.Ordinal);
            Assert.Equal(cpuSql.Replace("$RANK$", metric, StringComparison.Ordinal), sql);
        }

        Assert.Contains("LIMIT $7", TopRankings.Apply(DarlingDataReader.TopQueriesSql, ranking, hourly: false), StringComparison.Ordinal);
        Assert.Contains("NOT LIKE 'WAITFOR%'", TopRankings.Apply(DarlingDataReader.TopQueriesByHostObjectSql, ranking, hourly: false), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(TopRanking.Duration, "SUM(elapsed_time_sum)")]
    [InlineData(TopRanking.Executions, "SUM(execution_count_sum)")]
    public void Apply_Hourly_SwapsTheRankMetricInBothRollupStatements(TopRanking ranking, string metric)
    {
        foreach (var cpuSql in new[] { DarlingDataReader.TopQueriesHourlySql, DarlingDataReader.TopProceduresHourlySql })
        {
            var sql = TopRankings.Apply(cpuSql, ranking, hourly: true);
            Assert.Contains(metric + " AS rank_metric", sql, StringComparison.Ordinal);
            Assert.Contains("SUM(worker_time_sum) AS rank_cpu", sql, StringComparison.Ordinal);
            Assert.Contains(DarlingDataReader.TopQueriesHourlyFromPlaceholder, sql, StringComparison.Ordinal);
            Assert.DoesNotContain("$RANK$", sql, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The rollups keep worker time, elapsed time and execution counts and no logical reads, so a reads ranking has
    /// no hourly statement: the reader routes it to raw, and asking Apply for one is a programming error.
    /// </summary>
    [Fact]
    public void Reads_HasNoHourlyStatement_BecauseTheRollupHasNoReadsColumn()
    {
        Assert.False(TopRankings.HourlyCarries(TopRanking.Reads));
        Assert.True(TopRankings.HourlyCarries(TopRanking.Cpu));
        Assert.True(TopRankings.HourlyCarries(TopRanking.Duration));
        Assert.True(TopRankings.HourlyCarries(TopRanking.Executions));
        Assert.Throws<InvalidOperationException>(() => TopRankings.Apply(DarlingDataReader.TopQueriesHourlySql, TopRanking.Reads, hourly: true));

        var js = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "TimescaleSupport.cs");
        var at = js.IndexOf("[TimescaleSupport.QueryStatsHourlyView] = new[]", StringComparison.Ordinal);
        Assert.True(at > 0, "the stitch column list moved — remap this test");
        var list = js[at..js.IndexOf("[TimescaleSupport.QueryStatsDbHourlyView]", at, StringComparison.Ordinal)];
        Assert.DoesNotContain("logical_reads_sum", list, StringComparison.Ordinal);
    }

    /// <summary>
    /// A swap that did not land would silently rank by CPU; Apply refuses a statement whose anchor is absent or doubled
    /// (the full per-const check is in <see cref="TopRankingTwoPassTests"/>).
    /// </summary>
    [Fact]
    public void Apply_ThrowsWhenTheStatementLacksItsAnchor_OrCarriesItTwice()
    {
        foreach (var ranking in new[] { TopRanking.Cpu, TopRanking.Duration })
        {
            Assert.Throws<InvalidOperationException>(() => TopRankings.Apply("SELECT 1", ranking, hourly: false));
            Assert.Throws<InvalidOperationException>(() => TopRankings.Apply("SELECT $RANK$, $RANK$", ranking, hourly: false));
        }
    }

    [Theory]
    [InlineData("get_top_queries_by_cpu")]
    [InlineData("get_top_procedures_by_cpu")]
    public void TheWebCatalog_AdvertisesOrderBy_WithCpuAsTheDefault(string read)
    {
        var p = Assert.Single(DarlingWebEndpoints.CatalogDescriptors[read].Params, x => x.Name == "order_by");
        Assert.Equal("text", p.Type);
        Assert.False(p.Required);
        Assert.Equal("cpu", p.Default);
        Assert.True(DarlingWebEndpoints.BuildReadDispatch().ContainsKey(read));
    }

    [Fact]
    public void TheWebDispatch_PassesOrderBy_ToTheRankedSiblings_AndTheMcpToolsKeepTheirParameters()
    {
        var web = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs");
        Assert.Matches(@"GetTopQueriesRanked\([^\n]*order_by: Str\(c, ""order_by""\)", web);
        Assert.Matches(@"GetTopProceduresRanked\([^\n]*order_by: Str\(c, ""order_by""\)", web);

        /* The MCP surface does not change (#5226): neither tool has an order parameter, so both still rank by CPU. */
        foreach (var name in new[] { nameof(DarlingMcpDataTools.GetTopQueriesByCpu), nameof(DarlingMcpDataTools.GetTopProceduresByCpu) })
        {
            var method = typeof(DarlingMcpDataTools).GetMethod(name)!;
            Assert.DoesNotContain(method.GetParameters(), p => p.Name == "order_by");
        }
    }
}
