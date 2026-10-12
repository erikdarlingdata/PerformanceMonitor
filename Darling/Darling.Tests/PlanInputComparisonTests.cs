/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5630/#5631: <see cref="PlanInputComparison"/> decides which PLAN_REGRESSION candidates are kept. A candidate is
/// dropped when the statement recompiles on every call, when the newer plan returns far more rows and the cost did not
/// grow per row, or when the plans differ only in their compiled values. It is kept, with a stated basis, when the
/// compiled values are the same, when statistics changed between the compiles, or when the inputs cannot be compared.
/// Every plan below is a small synthetic document with invented names.
/// </summary>
public sealed class PlanInputComparisonTests
{
    private const string Ns = "http://schemas.microsoft.com/sqlserver/2004/07/showplan";
    private const string Early = "2026-01-05T10:00:00";
    private const string Late = "2026-01-09T10:00:00";

    /// <summary>One optimizer statistic: its name, the ordinal LastUpdate string the plan carries, and its modification count.</summary>
    private sealed record Stat(string Name, string LastUpdate, int Modifications = 0);

    /// <summary>A small synthetic showplan. <c>stats</c> null writes no OptimizerStatsUsage; a null parameter value writes no compiled value.</summary>
    private static string Plan(
        (string Column, string? Value)[]? parameters = null,
        Stat[]? stats = null,
        string? ceVersion = "160")
    {
        var list = string.Empty;
        foreach (var (column, value) in parameters ?? [])
        {
            list += "<ColumnReference Column=\"" + column + "\" ParameterDataType=\"int\""
                + (value is null ? string.Empty : " ParameterCompiledValue=\"" + value + "\" ParameterRuntimeValue=\"" + value + "\"")
                + "/>";
        }

        var usage = string.Empty;
        if (stats is not null)
        {
            usage = "<OptimizerStatsUsage>";
            foreach (var stat in stats)
            {
                usage += "<StatisticsInfo Database=\"[sample_db]\" Schema=\"[dbo]\" Table=\"[widgets]\" Statistics=\"[" + stat.Name
                    + "]\" ModificationCount=\"" + stat.Modifications + "\" SamplingPercent=\"100\" LastUpdate=\"" + stat.LastUpdate + "\"/>";
            }

            usage += "</OptimizerStatsUsage>";
        }

        return "<ShowPlanXML xmlns=\"" + Ns + "\" Version=\"1.564\"><BatchSequence><Batch><Statements>"
            + "<StmtSimple StatementText=\"SELECT 1\" StatementId=\"1\" StatementType=\"SELECT\""
            + (ceVersion is null ? string.Empty : " CardinalityEstimationModelVersion=\"" + ceVersion + "\"") + ">"
            + "<QueryPlan CachedPlanSize=\"16\">"
            + usage
            + (list.Length == 0 ? string.Empty : "<ParameterList>" + list + "</ParameterList>")
            + "</QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>";
    }

    private static Stat[] Stats(string lastUpdate, int count = 3, int modifications = 0)
    {
        var result = new Stat[count];
        for (var i = 0; i < count; i++)
            result[i] = new Stat("stat_" + i, lastUpdate, modifications);
        return result;
    }

    private static (string, string?)[] Values(params string?[] values)
    {
        var names = new[] { "@tenant", "@page", "@status" };
        var result = new (string, string?)[values.Length];
        for (var i = 0; i < values.Length; i++)
            result[i] = (names[i], values[i]);
        return result;
    }

    private static PlanInputOutcome Judge(string? latest, string? best) => PlanInputComparison.JudgePlans(latest, best);

    [Theory]
    [InlineData("SELECT 1 OPTION (RECOMPILE)")]
    [InlineData("select 1 option(recompile)")]
    [InlineData("SELECT 1\r\nOPTION\r\n(\r\n  MAXDOP 1 ,\r\n  RecompILE\r\n)")]
    [InlineData("SELECT 1 OPTION (OPTIMIZE FOR (@a = 1), RECOMPILE)")]
    [InlineData("SELECT 1 /* x */ OPTION (/* y */ RECOMPILE /* z */)")]
    public void ARecompileHintIsFoundInAnyCaseAndSpacing(string text)
    {
        Assert.True(PlanInputComparison.HasRecompileHint(text));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("SELECT 1 OPTION (MAXDOP 1)")]
    [InlineData("SELECT 1 /* OPTION (RECOMPILE) */")]
    [InlineData("SELECT 1 -- OPTION (RECOMPILE)")]
    [InlineData("SELECT 'OPTION (RECOMPILE)'")]
    [InlineData("SELECT recompile FROM dbo.t")]
    [InlineData("SELECT 1 OPTION (OPTIMIZE FOR UNKNOWN)")]
    [InlineData("-- statement text withheld (#4348)")]
    public void AStatementWithoutTheHintIsNotFlagged(string? text)
    {
        Assert.False(PlanInputComparison.HasRecompileHint(text));
    }

    // ---- JudgeBeforePlans: the rule steps that need no plan ----

    [Fact]
    public void ARecompilingLookupIsDropped_WhateverTheRowsAre()
    {
        Assert.Equal(PlanInputOutcome.DropRecompile,
            PlanInputComparison.JudgeBeforePlans("SELECT 1 OPTION (RECOMPILE)", 252, null, null));
        Assert.Equal(PlanInputOutcome.DropRecompile,
            PlanInputComparison.JudgeBeforePlans("SELECT 1 OPTION (RECOMPILE)", 252, 1, 1));
    }

    [Fact]
    public void AListQueryWhoseOutputGrewFarMoreThanItsCostIsDropped()
    {
        /* Rows 520x, factor 187: the cost per row went down (187 / 520 = 0.36). */
        Assert.Equal(PlanInputOutcome.DropOutputGrew,
            PlanInputComparison.JudgeBeforePlans("SELECT 1", 187, 26_000, 50));
    }

    [Theory]
    [InlineData(10.0, 1.0, 19.9, PlanInputOutcome.DropOutputGrew)]   // 10x rows, per-row 1.99
    [InlineData(10.0, 1.0, 20.0, PlanInputOutcome.NeedsPlans)]       // 10x rows, per-row 2.0: still a regression per row
    [InlineData(9.9, 1.0, 10.0, PlanInputOutcome.NeedsPlans)]        // 9.9x rows: not 10x
    [InlineData(5.0, 50.0, 100.0, PlanInputOutcome.NeedsPlans)]      // the latest plan returns fewer rows
    [InlineData(50.0, 50.0, 12.0, PlanInputOutcome.NeedsPlans)]      // the same rows
    [InlineData(null, 50.0, 12.0, PlanInputOutcome.NeedsPlans)]      // no rows for the latest plan
    [InlineData(26_000.0, null, 187.0, PlanInputOutcome.NeedsPlans)] // no rows for the best plan
    [InlineData(null, null, 187.0, PlanInputOutcome.NeedsPlans)]
    [InlineData(20.0, 0.5, 30.0, PlanInputOutcome.DropOutputGrew)]   // best under 1 row uses the floor of 1: 20x, per-row 1.5
    [InlineData(12.0, 0.1, 20.0, PlanInputOutcome.DropOutputGrew)]   // 12x against the floor, per-row 1.67
    [InlineData(12.0, 0.1, 30.0, PlanInputOutcome.NeedsPlans)]       // 12x against the floor, per-row 2.5
    [InlineData(0.0031, 0.0025, 12.0, PlanInputOutcome.NeedsPlans)]  // both under 1 row: both floor to 1
    [InlineData(0.0, 0.0, 12.0, PlanInputOutcome.NeedsPlans)]
    [InlineData(9.5, 0.2, 100.0, PlanInputOutcome.NeedsPlans)]       // 9.5 against the floor is under 10x
    public void TheOutputRuleNeedsTenTimesTheRowsAndLessThanTwiceThePerRowCost(
        double? rowsLatest, double? rowsBest, double factor, PlanInputOutcome expected)
    {
        Assert.Equal(expected, PlanInputComparison.JudgeBeforePlans("SELECT 1", factor, rowsLatest, rowsBest));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void MissingStatementTextCannotRunTheRecompileRuleAndAsksForThePlans(string? text)
    {
        Assert.Equal(PlanInputOutcome.NeedsPlans, PlanInputComparison.JudgeBeforePlans(text, 12, null, null));
    }

    [Fact]
    public void AWithheldStatementIsNotARecompileHint()
    {
        Assert.Equal(PlanInputOutcome.NeedsPlans,
            PlanInputComparison.JudgeBeforePlans(WithheldStatementMarker.Text, 12, 1, 1));
    }

    [Fact]
    public void TheRecompileRuleWinsOverTheOutputRule()
    {
        Assert.Equal(PlanInputOutcome.DropRecompile,
            PlanInputComparison.JudgeBeforePlans("SELECT 1 OPTION (RECOMPILE)", 187, 26_000, 50));
    }

    [Fact]
    public void TheConstantsAreTheDocumentedOnes()
    {
        Assert.Equal(10, PlanInputComparison.OutputGrowthRatio);
        Assert.Equal(2, PlanInputComparison.PerRowFactor);
        Assert.Equal(1, PlanInputComparison.RowFloor);
    }

    // ---- JudgePlans: the production cases ----

    [Fact]
    public void ThePlanSniffedVariantWithEveryStatisticUpdatedBetweenTheCompilesIsKeptAsStatisticsChanged()
    {
        /* Three parameters all differ; 14 shared statistics all have a later LastUpdate; rows were 0 and 0, which the floor absorbs. */
        var best = Plan(Values("(1)", "(100)", "(5)"), Stats(Early, 14, 10));
        var latest = Plan(Values("(7)", "(250)", "(9)"), Stats(Late, 14, 40));
        Assert.Equal(PlanInputOutcome.NeedsPlans, PlanInputComparison.JudgeBeforePlans("SELECT 1", 12, 0, 0));
        Assert.Equal(PlanInputOutcome.KeepStatisticsChanged, Judge(latest, best));
        Assert.Equal(PlanInputOutcome.KeepStatisticsChanged, Judge(best, latest));
    }

    [Fact]
    public void TheKeysetPageCompiledForTheSameValuesIsKeptAsSameValues()
    {
        var best = Plan(Values("(1)", "(100)", "(5)"), Stats(Early));
        var latest = Plan(Values("(1)", "(100)", "(5)"), Stats(Late));
        Assert.Equal(PlanInputOutcome.NeedsPlans, PlanInputComparison.JudgeBeforePlans("SELECT 1", 12, 996, 996));
        Assert.Equal(PlanInputOutcome.KeepSameValues, Judge(latest, best));
    }

    [Fact]
    public void AListCaseWhoseOneValueDiffersOverUnchangedStatisticsIsDroppedAsValuesOnly()
    {
        var best = Plan(Values("(1)"), Stats(Early));
        var latest = Plan(Values("(500)"), Stats(Early));
        Assert.Equal(PlanInputOutcome.NeedsPlans, PlanInputComparison.JudgeBeforePlans("SELECT 1", 12, 50, 50));
        Assert.Equal(PlanInputOutcome.DropValuesOnly, Judge(latest, best));
        Assert.Equal(PlanInputOutcome.DropValuesOnly, Judge(best, latest));
    }

    // ---- JudgePlans: edges ----

    [Fact]
    public void OneOfThreeValuesDiffering_DropsOverUnchangedStatistics_AndKeepsWhenOneStatisticIsNewer()
    {
        var best = Plan(Values("(1)", "(100)", "(5)"), Stats(Early));
        var sameStats = Plan(Values("(1)", "(100)", "(6)"), Stats(Early));
        Assert.Equal(PlanInputOutcome.DropValuesOnly, Judge(sameStats, best));

        var oneNewer = new[] { new Stat("stat_0", Early), new Stat("stat_1", Late), new Stat("stat_2", Early) };
        var newerStat = Plan(Values("(1)", "(100)", "(6)"), oneNewer);
        Assert.Equal(PlanInputOutcome.KeepStatisticsChanged, Judge(newerStat, best));
    }

    [Fact]
    public void ALiteralNullDiffersFromAValueAndMatchesAnotherNull()
    {
        var withNull = Plan(Values("NULL"), Stats(Early));
        var withValue = Plan(Values("(1)"), Stats(Early));
        Assert.Equal(PlanInputOutcome.DropValuesOnly, Judge(withNull, withValue));
        Assert.Equal(PlanInputOutcome.KeepSameValues, Judge(withNull, Plan(Values("NULL"), Stats(Late))));
    }

    [Fact]
    public void NoCompiledValueOnBothSidesCountsAsTheSame()
    {
        var a = Plan(Values((string?)null), Stats(Early));
        var b = Plan(Values((string?)null), Stats(Late));
        Assert.Equal(PlanInputOutcome.KeepSameValues, Judge(a, b));
    }

    [Fact]
    public void ACompiledValueOnOneSideOnlyIsUnverified()
    {
        var bare = Plan(Values((string?)null), Stats(Early));
        var valued = Plan(Values("(7)"), Stats(Early));
        Assert.Equal(PlanInputOutcome.KeepUnverified, Judge(bare, valued));
        Assert.Equal(PlanInputOutcome.KeepUnverified, Judge(valued, bare));
    }

    [Fact]
    public void ABareParameterBesideADifferingValuedOneIsUnverified()
    {
        var a = Plan(Values("(1)", null), Stats(Early));
        var b = Plan(Values("(2)", "(3)"), Stats(Early));
        Assert.Equal(PlanInputOutcome.KeepUnverified, Judge(a, b));
    }

    [Fact]
    public void NoParametersInEitherPlanIsTheSame_AndInOnlyOneIsUnverified()
    {
        Assert.Equal(PlanInputOutcome.KeepSameValues, Judge(Plan(), Plan()));
        Assert.Equal(PlanInputOutcome.KeepSameValues, Judge(Plan(stats: Stats(Early)), Plan(stats: Stats(Late))));
        Assert.Equal(PlanInputOutcome.KeepUnverified, Judge(Plan(), Plan(Values("(1)"))));
        Assert.Equal(PlanInputOutcome.KeepUnverified, Judge(Plan(Values("(1)")), Plan()));
    }

    [Fact]
    public void DifferentParameterNamesAreUnverified()
    {
        var a = Plan([("@tenant", "(1)")], Stats(Early));
        var b = Plan([("@other", "(1)")], Stats(Early));
        Assert.Equal(PlanInputOutcome.KeepUnverified, Judge(a, b));
        Assert.Equal(PlanInputOutcome.KeepUnverified,
            Judge(Plan(Values("(1)"), Stats(Early)), Plan(Values("(1)", "(2)"), Stats(Early))));
    }

    [Fact]
    public void DifferingValuesWithoutOptimizerStatsUsageOnEitherSideAreUnverified()
    {
        var withStats = Plan(Values("(1)"), Stats(Early));
        var without = Plan(Values("(2)"), stats: null);
        Assert.Equal(PlanInputOutcome.KeepUnverified, Judge(without, withStats));
        Assert.Equal(PlanInputOutcome.KeepUnverified, Judge(withStats, without));
        Assert.Equal(PlanInputOutcome.KeepUnverified, Judge(without, Plan(Values("(3)"), stats: null)));
    }

    [Fact]
    public void ASameValuePairNeedsNoStatisticsToBeKept()
    {
        Assert.Equal(PlanInputOutcome.KeepSameValues,
            Judge(Plan(Values("(1)"), stats: null), Plan(Values("(1)"), stats: null)));
    }

    [Fact]
    public void AStatisticOnlyInOnePlanIsAStatisticsChange()
    {
        var best = Plan(Values("(1)"), Stats(Early, 3));
        var latest = Plan(Values("(2)"), Stats(Early, 4));
        Assert.Equal(PlanInputOutcome.KeepStatisticsChanged, Judge(latest, best));
        Assert.Equal(PlanInputOutcome.KeepStatisticsChanged, Judge(best, latest));
    }

    [Fact]
    public void OnlyTheModificationCountDifferingStillDrops()
    {
        var best = Plan(Values("(1)"), Stats(Early, 3, modifications: 0));
        var latest = Plan(Values("(2)"), Stats(Early, 3, modifications: 5000));
        Assert.Equal(PlanInputOutcome.DropValuesOnly, Judge(latest, best));
    }

    [Fact]
    public void ADifferentEstimatorVersionIsKept_AndOneSidedIsUnverified()
    {
        var best = Plan(Values("(1)"), Stats(Early), ceVersion: "150");
        var latest = Plan(Values("(2)"), Stats(Early), ceVersion: "160");
        Assert.Equal(PlanInputOutcome.KeepStatisticsChanged, Judge(latest, best));
        Assert.Equal(PlanInputOutcome.DropValuesOnly, Judge(latest, Plan(Values("(1)"), Stats(Early), ceVersion: "160")));
        Assert.Equal(PlanInputOutcome.KeepUnverified, Judge(latest, Plan(Values("(1)"), Stats(Early), ceVersion: null)));
    }

    [Fact]
    public void TheEstimatorVersionAloneDoesNotChangeASameValuePair()
    {
        Assert.Equal(PlanInputOutcome.KeepSameValues,
            Judge(Plan(Values("(1)"), Stats(Early), "150"), Plan(Values("(1)"), Stats(Early), "160")));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("not xml at all")]
    [InlineData("<ShowPlanXML><unclosed>")]
    [InlineData("<!DOCTYPE r [<!ENTITY e \"x\">]><r>&e;</r>")]
    public void AMissingOrUnreadablePlanIsUnverified(string? bad)
    {
        var good = Plan(Values("(1)"), Stats(Early));
        Assert.Equal(PlanInputOutcome.KeepUnverified, Judge(bad, good));
        Assert.Equal(PlanInputOutcome.KeepUnverified, Judge(good, bad));
        Assert.Equal(PlanInputOutcome.KeepUnverified, Judge(bad, bad));
    }

    [Fact]
    public void AWithheldPlanOrValueIsUnverified_AndTwoOfThemAreNeverTheSame()
    {
        var marker = WithheldStatementMarker.Text;
        var good = Plan(Values("(1)"), Stats(Early));
        Assert.Equal(PlanInputOutcome.KeepUnverified, Judge(marker, good));
        Assert.Equal(PlanInputOutcome.KeepUnverified, Judge(marker, marker));

        var withheldValue = Plan(Values(marker), Stats(Early));
        Assert.Equal(PlanInputOutcome.KeepUnverified, Judge(withheldValue, good));
        Assert.Equal(PlanInputOutcome.KeepUnverified, Judge(good, withheldValue));
        Assert.Equal(PlanInputOutcome.KeepUnverified, Judge(withheldValue, withheldValue));
    }

    [Fact]
    public void TheOutcomeHelpersMapDropsAndBases()
    {
        PlanInputOutcome[] drops = [PlanInputOutcome.DropRecompile, PlanInputOutcome.DropOutputGrew, PlanInputOutcome.DropValuesOnly];
        foreach (var outcome in drops)
        {
            Assert.True(PlanInputComparison.IsDrop(outcome));
            Assert.Equal(0, PlanInputComparison.InputBasis(outcome));
        }

        Assert.False(PlanInputComparison.IsDrop(PlanInputOutcome.NeedsPlans));
        Assert.False(PlanInputComparison.IsDrop(PlanInputOutcome.KeepSameValues));
        Assert.False(PlanInputComparison.IsDrop(PlanInputOutcome.KeepStatisticsChanged));
        Assert.False(PlanInputComparison.IsDrop(PlanInputOutcome.KeepUnverified));
        Assert.Equal(1, PlanInputComparison.InputBasis(PlanInputOutcome.KeepSameValues));
        Assert.Equal(2, PlanInputComparison.InputBasis(PlanInputOutcome.KeepStatisticsChanged));
        Assert.Equal(0, PlanInputComparison.InputBasis(PlanInputOutcome.KeepUnverified));
    }

    [Fact]
    public void ThePlaceholderTextMatchesTheFilterMarker()
    {
        Assert.Equal(PerformanceMonitor.Common.SensitiveStatements.PlaceholderText, WithheldStatementMarker.Text);
    }

    [Fact]
    public void PlansStoredAfterTheRealFilterKeepOrdinaryValuesAndWithholdTheNamedOnes()
    {
        var ordinaryA = PerformanceMonitor.Common.SensitiveStatements.Xml(
            Plan([("@c", "(1)")], Stats(Early)).Replace("SELECT 1", StatementScrubCanary.PlainStatement, StringComparison.Ordinal));
        var ordinaryB = PerformanceMonitor.Common.SensitiveStatements.Xml(
            Plan([("@c", "(9)")], Stats(Early)).Replace("SELECT 1", StatementScrubCanary.PlainStatement, StringComparison.Ordinal));
        Assert.Equal(PlanInputOutcome.DropValuesOnly, Judge(ordinaryA, ordinaryB));
        Assert.Equal(PlanInputOutcome.KeepSameValues, Judge(ordinaryA, ordinaryA));

        var withheldA = PerformanceMonitor.Common.SensitiveStatements.Xml(
            Plan([("@p", "N'one'")], Stats(Early)).Replace("SELECT 1", StatementScrubCanary.CanaryStatement, StringComparison.Ordinal));
        var withheldB = PerformanceMonitor.Common.SensitiveStatements.Xml(
            Plan([("@p", "N'two'")], Stats(Early)).Replace("SELECT 1", StatementScrubCanary.CanaryStatement, StringComparison.Ordinal));
        Assert.DoesNotContain("N'one'", withheldA, StringComparison.Ordinal);
        Assert.Equal(PlanInputOutcome.KeepUnverified, Judge(withheldA, withheldB));
        Assert.Equal(PlanInputOutcome.KeepUnverified, Judge(withheldA, withheldA));
        Assert.Equal(PlanInputOutcome.KeepUnverified, Judge(withheldA, ordinaryA));
    }
}
