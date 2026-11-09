/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Reflection;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8471: the fact thresholds the two stores share are <c>PerformanceMonitor.Common.FactThresholds</c>, one name per number,
/// and the PARAMETER_SENSITIVITY firing signature is emitted by <c>ParameterSensitivitySignature.PredicateSql</c>, the
/// QUERY_HIGH_DOP count by <c>QueryHighDopSignature.HighDopQueriesCountSql</c>. The numbers are the ones the facts applied,
/// the builder's block is the text the six sites carried, and Darling's reads splice the shared text rather than spelling
/// a copy.
///
/// <para>The constants are reached by name, so this class compiles against a Common that lacks them and fails at its first
/// assert naming what is missing.</para>
/// </summary>
public sealed class FactThresholdsTests
{
    private const string DarlingCompiledBeforeWindow = "creation_time - make_interval(mins => svr.offset_minutes) <= $2 + interval '1 hour'";

    private static Type CommonType(string name)
    {
        var type = typeof(QueryDopProvenance).Assembly.GetType("PerformanceMonitor.Common." + name);
        Assert.True(type is not null, $"PerformanceMonitor.Common has no {name}: the fact thresholds are not written once in Common.");
        return type!;
    }

    private static object Threshold(string group, string name)
    {
        var field = CommonType("FactThresholds+" + group).GetField(name, BindingFlags.Public | BindingFlags.Static);
        Assert.True(field is { IsLiteral: true }, $"FactThresholds.{group}.{name} is not a public const.");
        return field!.GetRawConstantValue()!;
    }

    private static string PredicateSql(string compiledBeforeWindow, string indent)
    {
        var method = CommonType("ParameterSensitivitySignature").GetMethod("PredicateSql", BindingFlags.Public | BindingFlags.Static);
        Assert.True(method is not null, "ParameterSensitivitySignature has no PredicateSql.");
        return (string)method!.Invoke(null, [compiledBeforeWindow, indent])!;
    }

    private static string HighDopQueriesCountSql()
    {
        var field = CommonType("QueryHighDopSignature").GetField("HighDopQueriesCountSql", BindingFlags.Public | BindingFlags.Static);
        Assert.True(field is not null, "QueryHighDopSignature has no HighDopQueriesCountSql.");
        return (string)field!.GetValue(null)!;
    }

    [Fact]
    public void TheNumbers_AreTheOnesTheFactsApply()
    {
        Assert.Equal(10000, Threshold("ParameterSensitivity", "MinWorkerTime"));
        Assert.Equal(250000, Threshold("ParameterSensitivity", "MaxWorkerTime"));
        Assert.Equal(20, Threshold("ParameterSensitivity", "MinExecutions"));
        Assert.Equal(10, Threshold("ParameterSensitivity", "WorkerTimeRatio"));
        Assert.Equal(5.0, Threshold("ParameterSensitivity", "GrantDivergenceRatio"));
        Assert.Equal(20, Threshold("ParameterSensitivity", "OffenderCap"));
        Assert.Equal(8, Threshold("QueryHighDop", "DopFloor"));
        Assert.Equal(0, Threshold("QueryHighDop", "MaxDopUnlimited"));
    }

    [Theory]
    [InlineData("creation_time_utc <= $4", "")]
    [InlineData("creation_time_utc <= $7", "    ")]
    [InlineData(DarlingCompiledBeforeWindow, "")]
    [InlineData(DarlingCompiledBeforeWindow, "    ")]
    public void TheSignatureBuilder_EmitsTheFiringSignature_TheSitesCarried(string compiledBeforeWindow, string indent)
    {
        var expected =
            $"{indent}AND   min_worker_time >= 10000\n" +
            $"{indent}AND   max_worker_time >= 250000\n" +
            $"{indent}AND   execution_count >= 20\n" +
            $"{indent}AND   {compiledBeforeWindow}\n" +
            $"{indent}AND   max_worker_time::DOUBLE PRECISION / NULLIF(min_worker_time, 0) >= 10";

        Assert.Equal(expected, PredicateSql(compiledBeforeWindow, indent));
    }

    [Fact]
    public void Darling_SplicesTheBuilder_IntoItsThreeParameterSensitivityReads()
    {
        Assert.Contains(PredicateSql(DarlingCompiledBeforeWindow, ""), PgFactCollector.ParameterSensitivitySql, StringComparison.Ordinal);
        Assert.Contains(PredicateSql(DarlingCompiledBeforeWindow, "    "), PgDrillDownCollector.ParameterSensitiveSql, StringComparison.Ordinal);
        Assert.Contains(PredicateSql(DarlingCompiledBeforeWindow.Replace("$2", "$3", StringComparison.Ordinal), "    "), PgDrillDownCollector.RegressedQueriesSql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheHighDopCount_IsTheTextTheReadsCarried_AndDarlingSplicesIt()
    {
        var expected =
            "    COUNT(DISTINCT CASE WHEN v.max_dop > 8\n" +
            "                         AND (m.value_in_use IS NULL OR m.value_in_use = 0 OR v.max_dop <= m.value_in_use)\n" +
            "                        THEN COALESCE(v.database_name, '') || '/' || COALESCE(v.query_hash, '') END) AS high_dop_queries,";

        Assert.Equal(expected, HighDopQueriesCountSql());
        Assert.Contains(expected, PgFactCollector.QueryStatsSql, StringComparison.Ordinal);
    }
}
