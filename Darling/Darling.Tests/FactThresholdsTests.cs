/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8471: the fact thresholds the two stores share are <see cref="FactThresholds"/>, one name per number, and the
/// PARAMETER_SENSITIVITY firing signature is emitted by <see cref="ParameterSensitivitySignature.PredicateSql"/>, the
/// QUERY_HIGH_DOP count by <see cref="QueryHighDopSignature.HighDopQueriesCountSql"/>. The numbers are the ones the facts
/// apply, the builder's block is the text the six sites carry, and Darling's reads splice the shared text rather than
/// spelling a copy. Lite's reads are held to the same builder by its half of <c>ParameterSensitivityFiringSignatureParityTests</c>
/// and <c>QueryHighDopStaleMaxDopParityTests</c>.
/// </summary>
public sealed class FactThresholdsTests
{
    private const string DarlingCompiledBeforeWindow = "creation_time - make_interval(mins => svr.offset_minutes) <= $2 + interval '1 hour'";

    [Fact]
    public void TheNumbers_AreTheOnesTheFactsApply()
    {
        Assert.Equal(10000, FactThresholds.ParameterSensitivity.MinWorkerTime);
        Assert.Equal(250000, FactThresholds.ParameterSensitivity.MaxWorkerTime);
        Assert.Equal(20, FactThresholds.ParameterSensitivity.MinExecutions);
        Assert.Equal(10, FactThresholds.ParameterSensitivity.WorkerTimeRatio);
        Assert.Equal(5.0, FactThresholds.ParameterSensitivity.GrantDivergenceRatio);
        Assert.Equal(20, FactThresholds.ParameterSensitivity.OffenderCap);
        Assert.Equal(8, FactThresholds.QueryHighDop.DopFloor);
        Assert.Equal(0, FactThresholds.QueryHighDop.MaxDopUnlimited);
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

        Assert.Equal(expected, ParameterSensitivitySignature.PredicateSql(compiledBeforeWindow, indent));
    }

    [Fact]
    public void Darling_SplicesTheBuilder_IntoItsThreeParameterSensitivityReads()
    {
        Assert.Contains(ParameterSensitivitySignature.PredicateSql(DarlingCompiledBeforeWindow), PgFactCollector.ParameterSensitivitySql, StringComparison.Ordinal);
        Assert.Contains(ParameterSensitivitySignature.PredicateSql(DarlingCompiledBeforeWindow, "    "), PgDrillDownCollector.ParameterSensitiveSql, StringComparison.Ordinal);
        Assert.Contains(
            ParameterSensitivitySignature.PredicateSql(DarlingCompiledBeforeWindow.Replace("$2", "$3", StringComparison.Ordinal), "    "),
            PgDrillDownCollector.RegressedQueriesSql,
            StringComparison.Ordinal);
    }

    [Fact]
    public void TheHighDopCount_IsTheTextTheReadsCarried_AndDarlingSplicesIt()
    {
        var expected =
            "    COUNT(DISTINCT CASE WHEN v.max_dop > 8\n" +
            "                         AND (m.value_in_use IS NULL OR m.value_in_use = 0 OR v.max_dop <= m.value_in_use)\n" +
            "                        THEN COALESCE(v.database_name, '') || '/' || COALESCE(v.query_hash, '') END) AS high_dop_queries,";

        Assert.Equal(expected, QueryHighDopSignature.HighDopQueriesCountSql);
        Assert.Contains(expected, PgFactCollector.QueryStatsSql, StringComparison.Ordinal);
    }
}
