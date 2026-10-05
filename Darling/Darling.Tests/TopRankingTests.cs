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
/// spliced), each choice swaps the CPU ORDER BY in every one of the five top-N statements, the hourly rollup answers
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

    /// <summary>The CPU choice is the default read, byte for byte: the const comes back untouched.</summary>
    [Fact]
    public void Apply_Cpu_ReturnsEveryConstUnchanged()
    {
        Assert.Same(DarlingDataReader.TopQueriesSql, TopRankings.Apply(DarlingDataReader.TopQueriesSql, TopRanking.Cpu, hourly: false));
        Assert.Same(DarlingDataReader.TopQueriesHourlySql, TopRankings.Apply(DarlingDataReader.TopQueriesHourlySql, TopRanking.Cpu, hourly: true));
        Assert.Same(DarlingDataReader.TopProceduresSql, TopRankings.Apply(DarlingDataReader.TopProceduresSql, TopRanking.Cpu, hourly: false));
    }

    [Theory]
    [InlineData(TopRanking.Duration, "SUM(delta_elapsed_time) DESC, SUM(delta_worker_time) DESC", "r.total_elapsed_us DESC, r.total_cpu_us DESC")]
    [InlineData(TopRanking.Reads, "SUM(delta_logical_reads) DESC, SUM(delta_worker_time) DESC", "r.total_reads DESC, r.total_cpu_us DESC")]
    [InlineData(TopRanking.Executions, "SUM(delta_execution_count) DESC, SUM(delta_worker_time) DESC", "r.total_executions DESC, r.total_cpu_us DESC")]
    public void Apply_Raw_SwapsBothOrderBysOnTheQueryStatements_AndTheOneOnProcedures(TopRanking ranking, string inner, string outer)
    {
        foreach (var cpuSql in new[] { DarlingDataReader.TopQueriesSql, DarlingDataReader.TopQueriesByHostObjectSql })
        {
            var sql = TopRankings.Apply(cpuSql, ranking, hourly: false);
            Assert.Contains("ORDER BY " + inner, sql, StringComparison.Ordinal);
            Assert.Contains("ORDER BY " + outer, sql, StringComparison.Ordinal);
            Assert.DoesNotContain("ORDER BY SUM(delta_worker_time) DESC\n", sql.Replace("\r\n", "\n"), StringComparison.Ordinal);
            Assert.DoesNotContain("ORDER BY r.total_cpu_us DESC\n", sql.Replace("\r\n", "\n"), StringComparison.Ordinal);
            /* Everything but the two ORDER BY lines is untouched: the same columns, filters and caps. */
            Assert.Contains("LIMIT $4 + 5", sql, StringComparison.Ordinal);
            Assert.Contains("NOT LIKE 'WAITFOR%'", sql, StringComparison.Ordinal);
        }

        var proc = TopRankings.Apply(DarlingDataReader.TopProceduresSql, ranking, hourly: false);
        Assert.Contains("ORDER BY " + inner, proc, StringComparison.Ordinal);
        Assert.DoesNotContain("ORDER BY SUM(delta_worker_time) DESC\n", proc.Replace("\r\n", "\n"), StringComparison.Ordinal);
        Assert.Contains("LIMIT $4", proc, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(TopRanking.Duration, "SUM(elapsed_time_sum) DESC, SUM(worker_time_sum) DESC", "r.total_elapsed_us DESC, r.total_cpu_us DESC")]
    [InlineData(TopRanking.Executions, "SUM(execution_count_sum) DESC, SUM(worker_time_sum) DESC", "r.total_executions DESC, r.total_cpu_us DESC")]
    public void Apply_Hourly_SwapsBothOrderBysOnBothRollupStatements(TopRanking ranking, string inner, string outer)
    {
        foreach (var cpuSql in new[] { DarlingDataReader.TopQueriesHourlySql, DarlingDataReader.TopProceduresHourlySql })
        {
            var sql = TopRankings.Apply(cpuSql, ranking, hourly: true);
            Assert.Contains("ORDER BY " + inner, sql, StringComparison.Ordinal);
            Assert.Contains("ORDER BY " + outer, sql, StringComparison.Ordinal);
            Assert.Contains(DarlingDataReader.TopQueriesHourlyFromPlaceholder, sql, StringComparison.Ordinal);
            Assert.DoesNotContain("ORDER BY SUM(worker_time_sum) DESC\n", sql.Replace("\r\n", "\n"), StringComparison.Ordinal);
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

    /// <summary>A swap that did not land would silently rank by CPU; Apply refuses to run on a statement without its anchor.</summary>
    [Fact]
    public void Apply_ThrowsWhenTheStatementLacksItsCpuOrderBy()
    {
        Assert.Throws<InvalidOperationException>(() => TopRankings.Apply("SELECT 1", TopRanking.Duration, hourly: false));
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
